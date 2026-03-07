import asyncio
import logging
import re
import json
import time
from urllib.parse import urlparse, urljoin
from typing import Dict, Any
import gzip
import zlib
import random
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector
import zstandard # Importa la libreria zstandard
from aiohttp_socks import ProxyConnector

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    """Eccezione personalizzata per errori di estrazione."""
    pass

def unpack(p, a, c, k, e=None, d=None):
    """
    Unpacker for P.A.C.K.E.R. packed javascript.
    This is a Python port of the common Javascript unpacker.
    """
    while c > 0:
        c -= 1
        if k[c]:
            p = re.sub('\\b' + _int2base(c, a) + '\\b', k[c], p)
    return p

def _int2base(x, base):
    if x < 0:
        sign = -1
    elif x == 0:
        return '0'
    else:
        sign = 1
    
    x *= sign
    digits = []
    
    while x:
        digits.append('0123456789abcdefghijklmnopqrstuvwxyz'[x % base])
        x = int(x / base)
        
    if sign < 0:
        digits.append('-')
        
    digits.reverse()
    return ''.join(digits)

class SportsonlineExtractor:
    """Sportsonline/Sportzonline URL extractor for M3U8 streams."""

    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        }
        self.session = None
        self.mediaflow_endpoint = "hls_manifest_proxy"
        self._session_lock = asyncio.Lock()
        self.proxies = proxies or []
        self._failed_iframe_hosts: dict[str, float] = {}
        self._preferred_iframe_url: str | None = None
        self._failed_iframe_ttl_seconds = 300

    def _get_random_proxy(self):
        return random.choice(self.proxies) if self.proxies else None

    @staticmethod
    def _dedupe_preserve_order(items: list[str]) -> list[str]:
        seen = set()
        result = []
        for item in items:
            if item and item not in seen:
                seen.add(item)
                result.append(item)
        return result

    def _normalize_candidate_url(self, raw_url: str, base_url: str) -> str:
        candidate = raw_url.strip().replace('\\/', '/')
        if candidate.startswith('//'):
            return f"https:{candidate}"
        return urljoin(base_url, candidate)

    def _cleanup_failed_iframe_hosts(self):
        if not self._failed_iframe_hosts:
            return
        now = time.time()
        expired = [host for host, ts in self._failed_iframe_hosts.items() if now - ts >= self._failed_iframe_ttl_seconds]
        for host in expired:
            self._failed_iframe_hosts.pop(host, None)

    def _is_recently_failed_iframe_host(self, candidate_url: str) -> bool:
        host = urlparse(candidate_url).netloc.lower()
        if not host:
            return False
        ts = self._failed_iframe_hosts.get(host)
        if ts is None:
            return False
        return (time.time() - ts) < self._failed_iframe_ttl_seconds

    def _mark_iframe_host_failed(self, candidate_url: str):
        host = urlparse(candidate_url).netloc.lower()
        if host:
            self._failed_iframe_hosts[host] = time.time()

    def _mark_iframe_host_success(self, candidate_url: str):
        host = urlparse(candidate_url).netloc.lower()
        if host:
            self._failed_iframe_hosts.pop(host, None)

    def _order_iframe_candidates(self, candidates: list[str]) -> list[str]:
        self._cleanup_failed_iframe_hosts()
        ordered = list(candidates)

        # Prefer the last working iframe URL first to reduce repeated retries.
        if self._preferred_iframe_url and self._preferred_iframe_url in ordered:
            ordered.remove(self._preferred_iframe_url)
            ordered.insert(0, self._preferred_iframe_url)

        viable = [url for url in ordered if not self._is_recently_failed_iframe_host(url)]

        # If all candidates are currently failed, probe the first one to recover automatically.
        if not viable and ordered:
            viable = [ordered[0]]

        return viable

    def _get_iframe_candidates(self, html: str, base_url: str) -> list[str]:
        candidates = []

        # Primary source: iframe tags in the channel page.
        for match in re.findall(r'<iframe[^>]+src=["\']([^"\']+)["\']', html, re.IGNORECASE):
            candidates.append(self._normalize_candidate_url(match, base_url))

        # Fallback source: embed URLs in scripts.
        embed_pattern = re.compile(
            r'((?:https?:)?//[a-zA-Z0-9.-]+/(?:embed|player|e)/[a-zA-Z0-9_\-/?=&%.]+)',
            re.IGNORECASE
        )
        for match in embed_pattern.findall(html):
            candidates.append(self._normalize_candidate_url(match, base_url))

        return self._dedupe_preserve_order(candidates)

    async def _get_session(self):
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                logger.info(f"Using proxy {proxy} for Sportsonline session.")
                connector = ProxyConnector.from_url(proxy)
            else:
                connector = TCPConnector(limit=0, limit_per_host=0)

            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.base_headers,
                cookie_jar=aiohttp.CookieJar()
            )
        return self.session

    async def _make_robust_request(self, url: str, headers: dict = None, retries=3, initial_delay=2, timeout=15):
        final_headers = headers or self.base_headers
        # Rimuovi l'header Accept-Encoding per tentare di ricevere una risposta non compressa
        request_headers = final_headers.copy()
        request_headers['Accept-Encoding'] = 'gzip, deflate'

        for attempt in range(retries):
            try:
                session = await self._get_session()
                logger.info(f"Attempt {attempt + 1}/{retries} for URL: {url}")
                # Disabilita la decompressione automatica di aiohttp
                async with session.get(url, headers=request_headers, timeout=timeout, auto_decompress=False) as response:
                    response.raise_for_status()
                    content = await self._handle_response_content(response)
                    return content
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Connection error attempt {attempt + 1} for {url}: {str(e)}")
                if isinstance(e, aiohttp.ClientConnectorDNSError):
                    raise ExtractorError(f"DNS resolution failed for {url}: {str(e)}")
                if attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    raise ExtractorError(f"All {retries} attempts failed for {url}: {str(e)}")
            except Exception as e: # Cattura altri potenziali errori durante la decompressione/decodifica
                logger.exception(f"Error in _make_robust_request for {url}")
                raise ExtractorError(f"Error in robust request: {str(e)}")
        raise ExtractorError(f"Unable to complete request for {url}")

    async def _handle_response_content(self, response: aiohttp.ClientResponse) -> str:
        """Gestisce la decompressione manuale del corpo della risposta."""
        content_encoding = response.headers.get('Content-Encoding')
        raw_body = await response.read()
        
        if content_encoding == 'zstd':
            logger.info(f"Detected zstd compression for {response.url}. Manual streaming decompression.")
            dctx = zstandard.ZstdDecompressor()
            try:
                decompressed_body = dctx.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            except zstandard.ZstdError as zs_e:
                logger.error(f"Error during zstd decompression: {zs_e}")
                raise ExtractorError(f"Zstd decompression error: {zs_e}")
        elif content_encoding == 'gzip':
            logger.info(f"Detected gzip compression for {response.url}. Manual decompression.")
            decompressed_body = gzip.decompress(raw_body)
            return decompressed_body.decode(response.charset or 'utf-8')
        elif content_encoding == 'deflate':
            logger.info(f"Detected deflate compression for {response.url}. Manual decompression.")
            decompressed_body = zlib.decompress(raw_body)
            return decompressed_body.decode(response.charset or 'utf-8')
        else:
            return raw_body.decode(response.charset or 'utf-8')

    def _detect_packed_blocks(self, html: str) -> list[str]:
        """Rileva e estrae i blocchi eval packed dall'HTML."""
        # Pattern robusto che cattura l'intero blocco eval
        pattern = re.compile(r"(eval\(function\(p,a,c,k,e,d\).*?)\s*<\/script>", re.DOTALL)
        raw_matches = pattern.findall(html)
        
        # Fallback se il pattern precedente non funziona
        if not raw_matches:
            pattern = re.compile(r"(eval\(function\(p,a,c,k,e,.*?\)\))", re.DOTALL)
            raw_matches = pattern.findall(html)
        
        return raw_matches

    async def extract(self, url: str, **kwargs) -> Dict[str, Any]:
        try:
            logger.info(f"Fetching main page: {url}")
            main_html = await self._make_robust_request(url)

            iframe_candidates = self._get_iframe_candidates(main_html, url)
            if not iframe_candidates:
                raise ExtractorError("No iframe found on the page")

            ordered_iframe_candidates = self._order_iframe_candidates(iframe_candidates)
            logger.info(
                f"Found {len(iframe_candidates)} iframe candidate(s), trying {len(ordered_iframe_candidates)} viable candidate(s)"
            )

            iframe_headers = {
                'Referer': 'https://sportzonline.st/',
                'User-Agent': self.base_headers['user-agent'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,it;q=0.8',
                'Cache-Control': 'no-cache'
            }

            iframe_html = None
            iframe_url = None
            last_iframe_error = None

            for candidate_url in ordered_iframe_candidates:
                try:
                    logger.info(f"Trying iframe candidate: {candidate_url}")
                    iframe_html = await self._make_robust_request(candidate_url, headers=iframe_headers)
                    iframe_url = candidate_url
                    self._preferred_iframe_url = candidate_url
                    self._mark_iframe_host_success(candidate_url)
                    break
                except ExtractorError as iframe_error:
                    last_iframe_error = iframe_error
                    self._mark_iframe_host_failed(candidate_url)
                    logger.warning(f"Iframe candidate failed: {candidate_url} -> {iframe_error}")

            if not iframe_html or not iframe_url:
                raise ExtractorError(
                    f"All iframe candidates failed ({len(ordered_iframe_candidates)}): {last_iframe_error}"
                )

            logger.debug(f"Iframe HTML length: {len(iframe_html)}")

            packed_blocks = self._detect_packed_blocks(iframe_html)
            logger.info(f"Found {len(packed_blocks)} packed blocks")

            if not packed_blocks:
                direct_match = re.search(r'(https?://[^\s"\'<>]+?\.m3u8[^\s"\'<>]*)', iframe_html)
                if direct_match:
                    m3u8_url = direct_match.group(1)
                    logger.info(f"Found direct m3u8 URL: {m3u8_url}")
                    return {
                        "destination_url": m3u8_url,
                        "request_headers": {'Referer': iframe_url, 'User-Agent': iframe_headers['User-Agent']},
                        "mediaflow_endpoint": self.mediaflow_endpoint,
                    }
                raise ExtractorError("No packed blocks or direct m3u8 URL found")

            chosen_idx = 1 if len(packed_blocks) > 1 else 0
            m3u8_url = None

            for i in range(len(packed_blocks)):
                current_idx = (chosen_idx + i) % len(packed_blocks)
                try:
                    # Usa la funzione unpack direttamente sul blocco catturato
                    unpacked_code = unpack(packed_blocks[current_idx])
                    logger.info(f"Successfully unpacked block {current_idx}")
                    
                    patterns = [
                        r'var\s+src\s*=\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
                        r'src\s*=\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
                        r'file\s*:\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
                        # Pattern più generico per 'source:"...m3u8..."'
                        r'source\s*:\s*["\'](https?://[^\'"]+?\.m3u8[^\'"]*?)["\']',
                        # Pattern ancora più generico per qualsiasi URL m3u8 tra virgolette
                        r'["\'](https?://[^"\']+\.m3u8[^"\']*)["\']',
                    ]
                    for pattern in patterns:
                        src_match = re.search(pattern, unpacked_code)
                        if src_match:
                            m3u8_url = src_match.group(1)
                            if '.m3u8' in m3u8_url:
                                logger.info(f"Found m3u8 in block {current_idx}")
                                break
                    if m3u8_url:
                        break
                except Exception as e:
                    logger.warning(f"Failed to process block {current_idx}: {e}")
                    continue

            if not m3u8_url:
                raise ExtractorError("Could not extract m3u8 URL from any packed code block")

            logger.info(f"Successfully extracted m3u8 URL: {m3u8_url}")

            return {
                "destination_url": m3u8_url,
                "request_headers": {'Referer': iframe_url, 'User-Agent': iframe_headers['User-Agent']},
                "mediaflow_endpoint": self.mediaflow_endpoint,
            }
        except Exception as e:
            logger.exception(f"Sportsonline extraction failed for {url}")
            raise ExtractorError(f"Extraction failed: {str(e)}")

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

def unpack(packed_js):
    """
    Unpacker for P.A.C.K.E.R. packed javascript.
    This is a Python port of the common Javascript unpacker.
    """
    try:
        # Estrae i parametri p,a,c,k,e,d dalla stringa packed_js
        match = re.search(r"}\((.*)\)\)", packed_js)
        if not match:
            raise ValueError("Cannot find packed data.")
        
        p, a, c, k, e, d = eval(f"({match.group(1)})", {"__builtins__": {}}, {})
        return _unpack_logic(p, a, c, k, e, d)
    except Exception as e:
        raise ValueError(f"Failed to unpack JS: {e}")

def _unpack_logic(p, a, c, k, e, d):
    while c > 0:
        c -= 1
        if k[c]:
            p = re.sub('\\b' + _int2base(c, a) + '\\b', k[c], p)
    return p
