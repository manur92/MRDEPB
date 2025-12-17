import random
import logging
import ssl
import urllib.parse
from urllib.parse import urlparse
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_proxy import ProxyConnector

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    """Eccezione personalizzata per errori di estrazione"""
    pass

class GenericHLSExtractor:
    def __init__(self, request_headers, proxies=None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.session = None
        self.proxies = proxies or []

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            proxy = self._get_random_proxy()
            if proxy:
                logging.info(f"Utilizzo del proxy {proxy} per la sessione generica.")
                connector = ProxyConnector.from_url(proxy)
            else:
                # Create SSL context that doesn't verify certificates
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                connector = TCPConnector(
                    limit=20, limit_per_host=10, 
                    keepalive_timeout=60, enable_cleanup_closed=True, 
                    force_close=False, use_dns_cache=True,
                    ssl=ssl_context
                )

            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            self.session = ClientSession(
                timeout=timeout, connector=connector, 
                headers={'user-agent': self.base_headers['user-agent']}
            )
        return self.session

    async def extract(self, url, **kwargs):
        # ✅ AGGIORNATO: Rimossa validazione estensioni su richiesta utente.
        # Accetta qualsiasi URL per evitare errori con segmenti mascherati.
        # if not any(pattern in url.lower() for pattern in ['.m3u8', '.mpd', '.ts', '.js', '.css', '.html', '.txt', 'vixsrc.to/playlist', 'newkso.ru']):
        #     raise ExtractorError("URL non supportato (richiesto .m3u8, .mpd, .ts, .js, .css, .html, .txt, URL VixSrc o URL newkso.ru valido)")

        parsed_url = urlparse(url)
        origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
        headers = self.base_headers.copy()
        
        # ✅ FIX: Non sovrascrivere Referer/Origin se già presenti in request_headers (es. passati via h_ params)
        # GenericHLSExtractor viene usato come fallback per i segmenti, ma se abbiamo già headers specifici
        # (come quelli di DLHD), dobbiamo preservarli e non resettarli al dominio del segmento.
        if not any(k.lower() == 'referer' for k in self.request_headers):
            headers["referer"] = origin
        if not any(k.lower() == 'origin' for k in self.request_headers):
            headers["origin"] = origin

        # ✅ FIX: Ripristinata logica conservativa. Non inoltrare tutti gli header del client
        # per evitare conflitti (es. Host, Cookie, Accept-Encoding) con il server di destinazione.
        # Gli header necessari (Referer, User-Agent) vengono gestiti tramite i parametri h_.
        # ✅ FIX: Prevent IP Leakage. Explicitly filter out X-Forwarded-For and similar headers.
        # Only allow specific headers that are safe or necessary for authentication.
        for h, v in self.request_headers.items():
            h_lower = h.lower()
            # ✅ FIX DLHD: Ora accetta User-Agent passato via h_ params (contiene Chrome UA completo)
            # Salta solo se è lo User-Agent del player (es. "Player (Linux; Android 13)")
            # ma accetta se è un Chrome UA (contiene "Chrome" o "AppleWebKit")
            if h_lower == "user-agent":
                # Se è un vero browser UA (ha Chrome/Safari), usalo sovrascrivendo il default
                if "chrome" in v.lower() or "applewebkit" in v.lower():
                    headers["user-agent"] = v
                continue
                
            if h_lower in ["authorization", "x-api-key", "x-auth-token", "cookie", "referer", "origin", "x-channel-key"]:
                headers[h] = v
            # Explicitly block forwarding of IP-related headers
            if h_lower in ["x-forwarded-for", "x-real-ip", "forwarded", "via"]:
                continue

        return {
            "destination_url": url, 
            "request_headers": headers, 
            "mediaflow_endpoint": "hls_proxy"
        }

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
