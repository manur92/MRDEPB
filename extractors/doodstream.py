import logging
import random
import re
import time
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_socks import ProxyConnector

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class DoodStreamExtractor:
    """DoodStream URL extractor."""

    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.session = None
        self.mediaflow_endpoint = "proxy_stream_endpoint"
        self.proxies = proxies or []
        self.base_url = "https://d000d.com"

    def _get_random_proxy(self):
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                connector = ProxyConnector.from_url(proxy)
            else:
                connector = TCPConnector(limit=0, limit_per_host=0, keepalive_timeout=60, enable_cleanup_closed=True, force_close=False, use_dns_cache=True)
            self.session = ClientSession(timeout=timeout, connector=connector, headers={'User-Agent': self.base_headers["user-agent"]})
        return self.session

    async def extract(self, url: str, **kwargs) -> dict:
        """Extract DoodStream URL."""
        session = await self._get_session()
        
        async with session.get(url) as response:
            text = await response.text()

        # Extract URL pattern
        pattern = r"(\/pass_md5\/.*?)'.*(\\?token=.*?expiry=)"
        match = re.search(pattern, text, re.DOTALL)
        if not match:
            raise ExtractorError("Failed to extract URL pattern")

        # Build final URL
        pass_url = f"{self.base_url}{match[1]}"
        referer = f"{self.base_url}/"
        headers = {"range": "bytes=0-", "referer": referer}

        async with session.get(pass_url, headers=headers) as response:
            response_text = await response.text()
        
        timestamp = str(int(time.time()))
        final_url = f"{response_text}123456789{match[2]}{timestamp}"

        self.base_headers["referer"] = referer
        return {
            "destination_url": final_url,
            "request_headers": self.base_headers,
            "mediaflow_endpoint": self.mediaflow_endpoint,
        }

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
