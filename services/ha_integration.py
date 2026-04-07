"""
Home Assistant integration — placeholder for future implementation.

Planned features:
- Push node status to HA via REST API or MQTT
- Expose nodes as HA sensors
- Alert integration
"""


class HAIntegration:
    """Placeholder for Home Assistant integration."""

    def __init__(self):
        self.enabled = False

    async def push_status(self, node_id: str, status: dict) -> None:
        """Push node status to Home Assistant. Not yet implemented."""
        pass