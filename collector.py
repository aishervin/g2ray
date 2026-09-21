"""
G2Ray Core Collector - Fast Subscriptions Aggregator & Latency Filter
"""
import os
import re
import time
import base64
import urllib.parse
import asyncio
from typing import List, Dict, Tuple, Optional, Set
import urllib.request

SOURCES = [
    "https://raw.githubusercontent.com/barry-far/V2ray-Configs/main/Sub1.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Configs/main/Sub2.txt",
    "https://raw.githubusercontent.com/yebekhe/TelegramV2rayCollector/main/sub/base64/vless",
    "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/protocols/vless",
    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge.txt",
    "https://raw.githubusercontent.com/LalatinaHub/Mineral/master/result/nodes",
    "https://raw.githubusercontent.com/MrPooyaX/VpnCollector/main/sub/mix",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt",
]

def safe_b64decode(s: str) -> str:
    clean = re.sub(r"\s+", "", s)
    pad = len(clean) % 4
    if pad:
        clean += "=" * (4 - pad)
    try:
        return base64.b64decode(clean).decode("utf-8", errors="ignore")
    except Exception:
        try:
            return base64.urlsafe_b64decode(clean).decode("utf-8", errors="ignore")
        except Exception:
            return ""

def extract_vless(text: str) -> List[str]:
    res = re.findall(r"vless://[^\s\"\'<>]+", text)
    decoded = safe_b64decode(text)
    if decoded and "vless://" in decoded:
        res.extend(re.findall(r"vless://[^\s\"\'<>]+", decoded))
    return res

def parse_vless(uri: str) -> Optional[Dict]:
    try:
        p = urllib.parse.urlparse(uri)
        if p.scheme.lower() != "vless":
            return None
        netloc = p.netloc
        uuid, host_port = netloc.split("@", 1) if "@" in netloc else ("", netloc)
        if not uuid:
            return None
        if host_port.startswith("["):
            cb = host_port.find("]")
            host = host_port[1:cb]
            rem = host_port[cb + 1:]
            port = int(rem.split(":", 1)[1]) if ":" in rem else 443
        elif ":" in host_port:
            host, port_s = host_port.split(":", 1)
            port = int(port_s)
        else:
            host = host_port
            port = 443
        return {
            "raw": uri.strip(),
            "host": host.strip(),
            "port": port,
            "uuid": uuid.strip(),
            "key": f"{host}:{port}:{uuid}"
        }
    except Exception:
        return None

async def fetch_url(url: str) -> str:
    loop = asyncio.get_event_loop()
    def _get():
        req = urllib.request.Request(url, headers={"User-Agent": "v2rayNG/1.8.12"})
        try:
            with urllib.request.urlopen(req, timeout=6.0) as resp:
                return resp.read().decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return await loop.run_in_executor(None, _get)

async def test_tcp(host: str, port: int, timeout: float = 1.4) -> Optional[float]:
    t0 = time.perf_counter()
    try:
        r, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        ms = (time.perf_counter() - t0) * 1000.0
        w.close()
        await w.wait_closed()
        return ms
    except Exception:
        return None

async def ping_worker(node: Dict, sem: asyncio.Semaphore) -> Optional[Tuple[float, str]]:
    async with sem:
        res = await test_tcp(node["host"], node["port"])
        if res is not None:
            return (res, node["raw"])
        return None

async def main():
    print("Fetching subscription channels...")
    pages = await asyncio.gather(*[fetch_url(u) for u in SOURCES])
    uris = []
    for p in pages:
        uris.extend(extract_vless(p))
    
    seen: Set[str] = set()
    nodes: List[Dict] = []
    for u in uris:
        n = parse_vless(u)
        if n and n["key"] not in seen:
            seen.add(n["key"])
            nodes.append(n)
            
    print(f"Discovered {len(nodes)} unique candidates. Testing TCP latency...")
    sem = asyncio.Semaphore(150)
    tested = await asyncio.gather(*[ping_worker(n, sem) for n in nodes])
    alive = [t for t in tested if t is not None]
    alive.sort(key=lambda x: x[0])
    
    top = alive[:1000]
    print(f"Writing {len(top)} lowest-ping nodes to vless.txt...")
    with open("vless.txt", "w", encoding="utf-8") as f:
        for _, raw in top:
            f.write(f"{raw}\n")
    print("Completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
