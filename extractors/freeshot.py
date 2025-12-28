import logging
import re
import urllib.parse
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_proxy import ProxyConnector

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class FreeshotExtractor:
    """
    Extractor per Freeshot (popcdn.day).
    Risolve l'URL iframe e restituisce l'm3u8 finale.
    """
    def __init__(self, request_headers, proxies=None):
        self.request_headers = request_headers
        self.base_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Referer": "https://thisnot.business/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        self.proxies = proxies or []
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            connector = TCPConnector(ssl=False)
            # Se volessimo usare proxy per la richiesta iniziale (ma qui l'idea è usare l'IP del server MFP)
            # if self.proxies:
            #     proxy = self.proxies[0] # Simple logic
            #     connector = ProxyConnector.from_url(proxy)
            
            timeout = ClientTimeout(total=15)
            self.session = ClientSession(connector=connector, timeout=timeout)
        return self.session

    async def extract(self, url, **kwargs):
        """
        Estrae l'URL m3u8 da un link popcdn.day o da un codice canale.
        Input url può essere:
        1. https://popcdn.day/player/CODICE (nuovo formato)
        2. https://popcdn.day/go.php?stream=CODICE (vecchio formato - convertito)
        3. freeshot://CODICE (se vogliamo supportare un custom scheme)
        4. CODICE (se passato come parametro d=CODICE e host=freeshot)
        """
        
        # Determina il codice canale
        channel_code = url
        
        # Estrai il codice dal vecchio formato go.php
        if "go.php?stream=" in url:
            channel_code = url.split("go.php?stream=")[-1].split("&")[0]
        elif "popcdn.day/player/" in url:
            channel_code = url.split("/player/")[-1].split("?")[0].split("/")[0]
        elif url.startswith('http'):
            # URL sconosciuto, prova a usarlo come codice
            channel_code = urllib.parse.urlparse(url).path.split("/")[-1]
        
        # Nuovo URL formato /player/
        target_url = f"https://popcdn.day/player/{urllib.parse.quote(channel_code)}"

        logger.info(f"FreeshotExtractor: Risoluzione {target_url} (channel: {channel_code})")
        
        session = await self._get_session()
        
        try:
            async with session.get(target_url, headers=self.base_headers) as resp:
                if resp.status != 200:
                    raise ExtractorError(f"Freeshot request failed: {resp.status}")
                body = await resp.text()
            
            # Nuova estrazione token via currentToken
            match = re.search(r'currentToken:\s*["\']([^"\']+)["\']', body)
            if not match:
                # Fallback al vecchio metodo iframe
                match = re.search(r'frameborder="0"\s+src="([^"]+)"', body, re.IGNORECASE)
                if match:
                    iframe_url = match.group(1)
                    # Estrai token dall'iframe URL
                    token_match = re.search(r'token=([^&]+)', iframe_url)
                    if token_match:
                        token = token_match.group(1)
                    else:
                        raise ExtractorError("Freeshot token not found in iframe")
                else:
                    raise ExtractorError("Freeshot token/iframe not found")
            else:
                token = match.group(1)
            
            # Nuovo formato URL m3u8: tracks-v1a1/mono.m3u8
            m3u8_url = f"https://planetary.lovecdn.ru/{channel_code}/tracks-v1a1/mono.m3u8?token={token}"
            
            logger.info(f"FreeshotExtractor: Risolto -> {m3u8_url}")
            
            # Ritorniamo la struttura attesa da HLSProxy
            return {
                "destination_url": m3u8_url,
                "request_headers": {
                    "User-Agent": self.base_headers["User-Agent"],
                    "Referer": "https://popcdn.day/",
                    "Origin": "https://popcdn.day"
                },
                "mediaflow_endpoint": "hls_proxy"
            }
            
        except Exception as e:
            logger.error(f"FreeshotExtractor error: {e}")
            raise ExtractorError(f"Freeshot extraction failed: {str(e)}")

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
