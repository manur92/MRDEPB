import logging
import random
import re
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class MaxstreamExtractor:
    """Maxstream URL extractor."""

    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.session = None
        self.mediaflow_endpoint = "hls_proxy"
        self.proxies = proxies or []

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

    async def get_uprot(self, link: str):
        """Extract MaxStream URL from uprot redirect."""
        session = await self._get_session()
        if "msf" in link:
            link = link.replace("msf", "mse")
        
        async with session.get(link) as response:
            text = await response.text()
        
        soup = BeautifulSoup(text, "lxml")
        maxstream_url = soup.find("a")
        maxstream_url = maxstream_url.get("href")
        return maxstream_url

    async def extract(self, url: str, **kwargs) -> dict:
        """Extract Maxstream URL."""
        session = await self._get_session()
        
        maxstream_url = await self.get_uprot(url)
        
        async with session.get(maxstream_url, headers={"accept-language": "en-US,en;q=0.5"}) as response:
            text = await response.text()

        # Extract and decode URL
        match = re.search(r"\}\('(.+)',.+,'(.+)'\.split", text)
        if not match:
            raise ExtractorError("Failed to extract URL components")

        s1 = match.group(2)
        # Extract Terms
        terms = s1.split("|")
        urlset_index = terms.index("urlset")
        hls_index = terms.index("hls")
        sources_index = terms.index("sources")
        result = terms[urlset_index + 1 : hls_index]
        reversed_elements = result[::-1]
        first_part = terms[hls_index + 1 : sources_index]
        reversed_first_part = first_part[::-1]
        first_url_part = ""
        for first_part in reversed_first_part:
            if "0" in first_part:
                first_url_part += first_part
            else:
                first_url_part += first_part + "-"

        base_url = f"https://{first_url_part}.host-cdn.net/hls/"
        if len(reversed_elements) == 1:
            final_url = base_url + "," + reversed_elements[0] + ".urlset/master.m3u8"
        lenght = len(reversed_elements)
        i = 1
        for element in reversed_elements:
            base_url += element + ","
            if lenght == i:
                base_url += ".urlset/master.m3u8"
            else:
                i += 1
        final_url = base_url

        self.base_headers["referer"] = url
        return {
            "destination_url": final_url,
            "request_headers": self.base_headers,
            "mediaflow_endpoint": self.mediaflow_endpoint,
        }

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
