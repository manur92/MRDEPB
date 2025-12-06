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
        1. https://popcdn.day/go.php?stream=CODICE
        2. freeshot://CODICE (se vogliamo supportare un custom scheme)
        3. CODICE (se passato come parametro d=CODICE e host=freeshot)
        """
        
        # Determina il codice canale o l'URL completo
        target_url = url
        if not url.startswith('http'):
            # Se è solo il codice, costruisci l'URL
            target_url = f"https://popcdn.day/go.php?stream={urllib.parse.quote(url)}"
        elif "popcdn.day" not in url:
             # Fallback se arriva un URL strano, assumiamo sia il codice se non è un URL valido
             pass

        logger.info(f"FreeshotExtractor: Risoluzione {target_url}")
        
        session = await self._get_session()
        
        try:
            async with session.get(target_url, headers=self.base_headers) as resp:
                if resp.status != 200:
                    raise ExtractorError(f"Freeshot request failed: {resp.status}")
                body = await resp.text()
                
            # Estrazione iframe
            match = re.search(r'frameborder="0"\s+src="([^"]+)"', body, re.IGNORECASE)
            if not match:
                raise ExtractorError("Freeshot iframe not found")
                
            iframe_url = match.group(1)
            
            # Conversione in m3u8
            # L'URL contiene già il token e il parametro 'remote' con l'IP del chiamante (MFP)
            m3u8_url = iframe_url.replace('embed.html', 'index.fmp4.m3u8')
            
            logger.info(f"FreeshotExtractor: Risolto -> {m3u8_url}")
            
            # Ritorniamo la struttura attesa da HLSProxy
            return {
                "destination_url": m3u8_url,
                "request_headers": {
                    "User-Agent": self.base_headers["User-Agent"],
                    "Referer": iframe_url,
                    "Origin": f"https://{urllib.parse.urlparse(iframe_url).netloc}"
                },
                "mediaflow_endpoint": "hls_proxy" # O "hls_manifest_proxy" se vogliamo manipolare il manifest
            }
            
        except Exception as e:
            logger.error(f"FreeshotExtractor error: {e}")
            raise ExtractorError(f"Freeshot extraction failed: {str(e)}")

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
