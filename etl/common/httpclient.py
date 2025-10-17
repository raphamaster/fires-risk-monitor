import httpx
from dataclasses import dataclass

@dataclass
class HttpSettings:
    timeout: int = 30

def get_client(timeout: int = 30) -> httpx.Client:
    return httpx.Client(timeout=timeout, headers={"User-Agent":"fires-risk-monitor/1.0"})
