"""
假 Agent — 模拟真实设备持续上报，用于测试 Server + 前端联调。
"""
import requests
import time
import random
import math

SERVER_URL = "http://127.0.0.1:8850"  # ← 改成你 Server 的地址
NODE_ID = "fake-linux-01"

rx_bytes = 1_000_000_000
tx_bytes = 500_000_000
read_bytes = 200_000_000
write_bytes = 100_000_000

print(f"Fake Agent started, reporting to {SERVER_URL}")
print(f"Node ID: {NODE_ID}")
print("Press Ctrl+C to stop\n")

seq = 0
while True:
    seq += 1
    now = time.time()
    
    # 模拟波动的指标
    cpu = 20 + 15 * math.sin(now / 60) + random.uniform(-5, 5)
    mem = 55 + 10 * math.sin(now / 120) + random.uniform(-3, 3)
    cpu = max(0, min(100, cpu))
    mem = max(0, min(100, mem))

    # 模拟累增的网络计数器
    rx_bytes += random.randint(50_000, 500_000)
    tx_bytes += random.randint(20_000, 200_000)
    read_bytes += random.randint(10_000, 100_000)
    write_bytes += random.randint(5_000, 50_000)

    report = {
        "node": {
            "node_id": NODE_ID,
            "hostname": "FakeLinux",
            "os": "Ubuntu 24.04",
            "arch": "x86_64",
        },
        "timestamp": now,
        "uptime_sec": seq * 5,
        "cpu": {
            "count": 4,
            "usage_pct": round(cpu, 1),
            "load_avg": [round(cpu / 25, 2), round(cpu / 30, 2), round(cpu / 35, 2)],
        },
        "memory": {
            "total_mb": 8192,
            "used_mb": round(8192 * mem / 100),
            "usage_pct": round(mem, 1),
        },
        "disks": [{
            "device": "/dev/sda1",
            "mount": "/",
            "fs_type": "ext4",
            "total_mb": 100000,
            "used_mb": 45000,
            "usage_pct": 45.0,
        }],
        "disk_io": [{
            "device": "/dev/sda1",
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
        }],
        "network": [{
            "iface": "eth0",
            "rx_bytes": rx_bytes,
            "tx_bytes": tx_bytes,
            "rx_packets": seq * 100,
            "tx_packets": seq * 50,
        }],
    }

    try:
        resp = requests.post(f"{SERVER_URL}/api/v1/report", json=report, timeout=5)
        print(f"[#{seq}] Reported: CPU={cpu:.1f}% MEM={mem:.1f}% → {resp.json()['status']}")
    except Exception as e:
        print(f"[#{seq}] Error: {e}")

    time.sleep(5)