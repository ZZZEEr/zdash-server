"""
Alert engine with retry mechanism and error recovery.
"""

import logging
import time
from typing import Any, Dict, List, Optional
from functools import wraps

from server.models.database import Database, db as default_db

logger = logging.getLogger("zdash.alert_engine")

ALERT_COOLDOWN_SECONDS = 300
VALID_METRICS = {
    "cpu_usage_pct", "mem_usage_pct", "disk_usage_pct",
    "net_rx_rate_kbps", "net_tx_rate_kbps", "status",
}
VALID_OPERATORS = {"gt", "gte", "lt", "lte", "eq", "neq"}


def retry_on_db_error(max_retries: int = 3, delay: float = 0.1):
    """Decorator for retrying database operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.error(f"All {max_retries} attempts failed: {e}")
            raise last_error
        return wrapper
    return decorator


def _compare(actual: float, operator: str, threshold: float) -> bool:
    """Evaluate a comparison."""
    if operator == "gt":
        return actual > threshold
    elif operator == "gte":
        return actual >= threshold
    elif operator == "lt":
        return actual < threshold
    elif operator == "lte":
        return actual <= threshold
    elif operator == "eq":
        return actual == threshold
    elif operator == "neq":
        return actual != threshold
    return False


class AlertEngine:
    """Alert engine with retry and error recovery."""

    def __init__(self, database: Optional[Database] = None):
        self._db = database or default_db
        self._last_triggered: Dict[str, float] = {}

    @retry_on_db_error(max_retries=3)
    def create_rule(
        self,
        name: str,
        metric: str,
        operator: str = "gt",
        threshold: float = 0.0,
        node_id: Optional[str] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """Create alert rule with validation."""
        if metric not in VALID_METRICS:
            raise ValueError(f"Invalid metric: {metric}. Valid: {VALID_METRICS}")
        if operator not in VALID_OPERATORS:
            raise ValueError(f"Invalid operator: {operator}. Valid: {VALID_OPERATORS}")

        now = time.time()
        cursor = self._db.execute(
            """
            INSERT INTO alert_rules (name, metric, operator, threshold, node_id, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, metric, operator, threshold, node_id, 1 if enabled else 0, now, now),
        )
        rule_id = cursor.lastrowid
        logger.info(f"Created alert rule #{rule_id}: {name} ({metric} {operator} {threshold})")

        return {
            "id": rule_id, "name": name, "metric": metric, "operator": operator,
            "threshold": threshold, "node_id": node_id, "enabled": enabled,
            "created_at": now, "updated_at": now,
        }

    @retry_on_db_error(max_retries=2)
    def delete_rule(self, rule_id: int) -> bool:
        """Delete alert rule."""
        cursor = self._db.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
        deleted = cursor.rowcount > 0
        if deleted:
            logger.info(f"Deleted alert rule #{rule_id}")
        return deleted

    @retry_on_db_error(max_retries=2)
    def get_rules(self) -> List[Dict[str, Any]]:
        """Get all alert rules."""
        rows = self._db.fetchall("SELECT * FROM alert_rules ORDER BY id")
        return [
            {
                "id": r["id"], "name": r["name"], "metric": r["metric"],
                "operator": r["operator"], "threshold": r["threshold"],
                "node_id": r["node_id"], "enabled": bool(r["enabled"]),
                "created_at": r["created_at"], "updated_at": r["updated_at"],
            }
            for r in rows
        ]

    @retry_on_db_error(max_retries=3)
    def evaluate(
        self,
        node_id: str,
        node_data: Dict[str, Any],
        node_status: str,
    ) -> List[Dict[str, Any]]:
        """Evaluate rules with error recovery."""
        try:
            rules = self._db.fetchall(
                "SELECT * FROM alert_rules WHERE enabled = 1 AND (node_id IS NULL OR node_id = ?)",
                (node_id,),
            )
        except Exception as e:
            logger.error(f"Failed to fetch rules: {e}")
            return []

        triggered = []
        now = time.time()

        for rule in rules:
            try:
                rule_id = rule["id"]
                metric = rule["metric"]
                operator = rule["operator"]
                threshold = rule["threshold"]

                if metric == "status":
                    if node_status != "offline":
                        continue
                    actual_value = 0.0
                    message = f"Node '{node_id}' is offline"
                else:
                    actual_value = node_data.get(metric, 0.0)
                    if not _compare(actual_value, operator, threshold):
                        continue
                    op_symbol = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "eq": "==", "neq": "!="}
                    message = f"{metric} = {actual_value:.1f} {op_symbol.get(operator, operator)} {threshold:.1f} on node '{node_id}'"

                cooldown_key = f"{rule_id}:{node_id}"
                last_time = self._last_triggered.get(cooldown_key, 0)
                if now - last_time < ALERT_COOLDOWN_SECONDS:
                    continue

                self._last_triggered[cooldown_key] = now

                cursor = self._db.execute(
                    """
                    INSERT INTO alert_history
                        (rule_id, rule_name, node_id, metric, threshold, actual_value, message, triggered_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (rule_id, rule["name"], node_id, metric, threshold, actual_value, message, now),
                )

                event = {
                    "id": cursor.lastrowid, "rule_id": rule_id, "rule_name": rule["name"],
                    "node_id": node_id, "metric": metric, "threshold": threshold,
                    "actual_value": actual_value, "message": message, "triggered_at": now,
                    "acknowledged": False, "acked_at": None,
                }
                triggered.append(event)
                logger.warning(f"ALERT #{event['id']}: {message}")

            except Exception as e:
                logger.error(f"Error evaluating rule {rule.get('id')}: {e}")
                continue

        return triggered

    @retry_on_db_error(max_retries=2)
    def get_history(
        self,
        node_id: Optional[str] = None,
        hours: float = 24,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get alert history."""
        cutoff = time.time() - (hours * 3600)

        if node_id:
            rows = self._db.fetchall(
                "SELECT * FROM alert_history WHERE node_id = ? AND triggered_at >= ? ORDER BY triggered_at DESC LIMIT ?",
                (node_id, cutoff, limit),
            )
        else:
            rows = self._db.fetchall(
                "SELECT * FROM alert_history WHERE triggered_at >= ? ORDER BY triggered_at DESC LIMIT ?",
                (cutoff, limit),
            )

        return [
            {
                "id": r["id"], "rule_id": r["rule_id"], "rule_name": r["rule_name"],
                "node_id": r["node_id"], "metric": r["metric"], "threshold": r["threshold"],
                "actual_value": r["actual_value"], "message": r["message"],
                "triggered_at": r["triggered_at"], "acknowledged": bool(r["acknowledged"]),
                "acked_at": r["acked_at"],
            }
            for r in rows
        ]

    @retry_on_db_error(max_retries=2)
    def acknowledge(self, alert_id: int) -> Optional[Dict[str, Any]]:
        """Acknowledge an alert."""
        now = time.time()
        cursor = self._db.execute(
            "UPDATE alert_history SET acknowledged = 1, acked_at = ? WHERE id = ?",
            (now, alert_id),
        )
        if cursor.rowcount == 0:
            return None
        return {"id": alert_id, "acknowledged": True, "acked_at": now}