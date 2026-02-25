import asyncio
import logging
import time
import socket
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp_socks import ProxyConnector
from typing import Optional, Dict, Any
import random
import re
import uuid
from urllib.parse import quote

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class VavooExtractor:
    """Vavoo URL extractor per risolvere link vavoo.to"""
    
    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": "electron-fetch/1.0 electron (+https://github.com/arantes555/electron-fetch)"
        }
        self.api_ua = "electron-fetch/1.0 electron (+https://github.com/arantes555/electron-fetch)"
        self.ts_ua = "VAVOO/2.6"
        self.session = None
        self.mediaflow_endpoint = "proxy_stream_endpoint"
        self.proxies = proxies or []

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None
        
    async def _get_session(self):
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                logger.info(f"Using proxy {proxy} for Vavoo session.")
                connector = ProxyConnector.from_url(proxy)
            else:
                connector = TCPConnector(
                    limit=0,
                    limit_per_host=0,
                    keepalive_timeout=60,
                    enable_cleanup_closed=True,
                    force_close=False,
                    use_dns_cache=True,
                    family=socket.AF_INET # Force IPv4
                )

            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers={'User-Agent': self.base_headers["user-agent"]}
            )
        return self.session

    async def _get_auth_signature(self) -> Optional[str]:
        session = await self._get_session()
        unique_id = uuid.uuid4().hex[:16]
        headers = {
            "user-agent": self.api_ua,
            "accept": "*/*",
            "Accept-Language": "de",
            "Accept-Encoding": "gzip, deflate",
            "content-type": "application/json; charset=utf-8",
            "Connection": "close",
        }
        body = {
            "token": "8Us2TfjeOFrzqFFTEjL3E5KfdAWGa5PV3wQe60uK4BmzlkJRMYFu0ufaM_eeDXKS2U04XUuhbDTgGRJrJARUwzDyCcRToXhW5AcDekfFMfwNUjuieeQ1uzeDB9YWyBL2cn5Al3L3gTnF8Vk1t7rPwkBob0swvxA",
            "reason": "player.enter",
            "locale": "de",
            "theme": "dark",
            "metadata": {
                "device": {"type": "Desktop", "brand": "Unknown", "model": "Unknown", "name": "Unknown", "uniqueId": unique_id},
                "os": {"name": "windows", "version": "10.0.22631", "abis": [], "host": "electron"},
                "app": {"platform": "electron", "version": "3.1.4", "buildId": "288045000", "engine": "jsc", "signatures": [], "installer": "unknown"},
                "version": {"package": "tv.vavoo.app", "binary": "3.1.4", "js": "3.1.4"},
            },
            "appFocusTime": 27229,
            "playerActive": True,
            "playDuration": 0,
            "devMode": False,
            "hasAddon": False,
            "castConnected": False,
            "package": "tv.vavoo.app",
            "version": "3.1.4",
            "process": "app",
            "firstAppStart": int(time.time() * 1000) - 86400000,
            "lastAppStart": int(time.time() * 1000),
            "ipLocation": "",
            "adblockEnabled": False,
            "proxy": {"supported": ["ss"], "engine": "ss", "enabled": False, "autoServer": True, "id": "ca-bhs"},
            "iap": {"supported": True},
        }
        try:
            async with session.post("https://www.vavoo.tv/api/app/ping", json=body, headers=headers) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.json(content_type=None)
                return data.get("addonSig")
        except Exception as exc:
            logger.debug("get_auth_signature error: %s", exc)
            return None

    async def _get_ts_signature(self) -> Optional[str]:
        vec = "9frjpxPjxSNilxJPCJ0XGYs6scej3dW/h/VWlnKUiLSG8IP7mfyDU7NirOlld+VtCKGj03XjetfliDMhIev7wcARo+YTU8KPFuVQP9E2DVXzY2BFo1NhE6qEmPfNDnm74eyl/7iFJ0EETm6XbYyz8IKBkAqPN/Spp3PZ2ulKg3QBSDxcVN4R5zRn7OsgLJ2CNTuWkd/h451lDCp+TtTuvnAEhcQckdsydFhTZCK5IiWrrTIC/d4qDXEd+GtOP4hPdoIuCaNzYfX3lLCwFENC6RZoTBYLrcKVVgbqyQZ7DnLqfLqvf3z0FVUWx9H21liGFpByzdnoxyFkue3NzrFtkRL37xkx9ITucepSYKzUVEfyBh+/3mtzKY26VIRkJFkpf8KVcCRNrTRQn47Wuq4gC7sSwT7eHCAydKSACcUMMdpPSvbvfOmIqeBNA83osX8FPFYUMZsjvYNEE3arbFiGsQlggBKgg1V3oN+5ni3Vjc5InHg/xv476LHDFnNdAJx448ph3DoAiJjr2g4ZTNynfSxdzA68qSuJY8UjyzgDjG0RIMv2h7DlQNjkAXv4k1BrPpfOiOqH67yIarNmkPIwrIV+W9TTV/yRyE1LEgOr4DK8uW2AUtHOPA2gn6P5sgFyi68w55MZBPepddfYTQ+E1N6R/hWnMYPt/i0xSUeMPekX47iucfpFBEv9Uh9zdGiEB+0P3LVMP+q+pbBU4o1NkKyY1V8wH1Wilr0a+q87kEnQ1LWYMMBhaP9yFseGSbYwdeLsX9uR1uPaN+u4woO2g8sw9Y5ze5XMgOVpFCZaut02I5k0U4WPyN5adQjG8sAzxsI3KsV04DEVymj224iqg2Lzz53Xz9yEy+7/85ILQpJ6llCyqpHLFyHq/kJxYPhDUF755WaHJEaFRPxUqbparNX+mCE9Xzy7Q/KTgAPiRS41FHXXv+7XSPp4cy9jli0BVnYf13Xsp28OGs/D8Nl3NgEn3/eUcMN80JRdsOrV62fnBVMBNf36+LbISdvsFAFr0xyuPGmlIETcFyxJkrGZnhHAxwzsvZ+Uwf8lffBfZFPRrNv+tgeeLpatVcHLHZGeTgWWml6tIHwWUqv2TVJeMkAEL5PPS4Gtbscau5HM+FEjtGS+KClfX1CNKvgYJl7mLDEf5ZYQv5kHaoQ6RcPaR6vUNn02zpq5/X3EPIgUKF0r/0ctmoT84B2J1BKfCbctdFY9br7JSJ6DvUxyde68jB+Il6qNcQwTFj4cNErk4x719Y42NoAnnQYC2/qfL/gAhJl8TKMvBt3Bno+va8ve8E0z8yEuMLUqe8OXLce6nCa+L5LYK1aBdb60BYbMeWk1qmG6Nk9OnYLhzDyrd9iHDd7X95OM6X5wiMVZRn5ebw4askTTc50xmrg4eic2U1w1JpSEjdH/u/hXrWKSMWAxaj34uQnMuWxPZEXoVxzGyuUbroXRfkhzpqmqqqOcypjsWPdq5BOUGL/Riwjm6yMI0x9kbO8+VoQ6RYfjAbxNriZ1cQ+AW1fqEgnRWXmjt4Z1M0ygUBi8w71bDML1YG6UHeC2cJ2CCCxSrfycKQhpSdI1QIuwd2eyIpd4LgwrMiY3xNWreAF+qobNxvE7ypKTISNrz0iYIhU0aKNlcGwYd0FXIRfKVBzSBe4MRK2pGLDNO6ytoHxvJweZ8h1XG8RWc4aB5gTnB7Tjiqym4b64lRdj1DPHJnzD4aqRixpXhzYzWVDN2kONCR5i2quYbnVFN4sSfLiKeOwKX4JdmzpYixNZXjLkG14seS6KR0Wl8Itp5IMIWFpnNokjRH76RYRZAcx0jP0V5/GfNNTi5QsEU98en0SiXHQGXnROiHpRUDXTl8FmJORjwXc0AjrEMuQ2FDJDmAIlKUSLhjbIiKw3iaqp5TVyXuz0ZMYBhnqhcwqULqtFSuIKpaW8FgF8QJfP2frADf4kKZG1bQ99MrRrb2A="
        session = await self._get_session()
        try:
            async with session.post("https://www.vavoo.tv/api/box/ping2", data={"vec": vec}) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.json(content_type=None)
                return (data.get("response") or {}).get("signed")
        except Exception as exc:
            logger.debug("get_ts_signature error: %s", exc)
            return None

    async def _resolve_with_auth(self, url: str, signature: str) -> Optional[str]:
        session = await self._get_session()
        headers = {
            "user-agent": self.api_ua,
            "accept": "*/*",
            "Accept-Language": "de",
            "Accept-Encoding": "gzip, deflate",
            "content-type": "application/json; charset=utf-8",
            "Connection": "close",
            "mediahubmx-signature": signature,
        }
        payload = {"language": "de", "region": "AT", "url": url, "clientVersion": "3.1.4"}
        try:
            async with session.post("https://vavoo.to/mediahubmx-resolve.json", json=payload, headers=headers) as resp:
                if resp.status >= 400:
                    return None
                data = await resp.json(content_type=None)
                if isinstance(data, list) and data and isinstance(data[0], dict) and data[0].get("url"):
                    return str(data[0]["url"])
                if isinstance(data, dict):
                    if data.get("url"):
                        return str(data["url"])
                    if isinstance(data.get("data"), dict) and data["data"].get("url"):
                        return str(data["data"]["url"])
                return None
        except Exception as exc:
            logger.debug("resolve_with_auth error: %s", exc)
            return None

    async def _follow_stream_url(self, url: str) -> str:
        session = await self._get_session()
        stream_headers = {
            "User-Agent": self.api_ua,
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "close",
        }
        try:
            async with session.head(url, headers=stream_headers, allow_redirects=True) as resp:
                final_url = str(resp.url)
                ctype = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" in ctype:
                async with session.get(url, headers=stream_headers, allow_redirects=True) as resp:
                    text = await resp.text(errors="ignore")
                    m3u8 = re.findall(r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)', text)
                    if m3u8:
                        return m3u8[0]
                    generic = re.findall(r'(https?://[^\s"\'<>]+(?:\.ts|/live/|/stream/|/playlist|/index)[^\s"\'<>]*)', text)
                    if generic:
                        return generic[0]
                return final_url
            return final_url
        except Exception:
            return url

    async def _build_ts_fallback(self, url: str) -> Optional[str]:
        if "vavoo-iptv" not in url:
            return None
        ts_sig = await self._get_ts_signature()
        if not ts_sig:
            return None
        base = re.sub(r"/index\.m3u8(?:\?.*)?$", "", url.replace("vavoo-iptv", "live2")).rstrip("/")
        ts_url = f"{base}.ts?n=1&b=5&vavoo_auth={quote(ts_sig, safe='')}"
        session = await self._get_session()
        try:
            async with session.get(ts_url, headers={"User-Agent": self.ts_ua}, allow_redirects=True) as resp:
                if resp.status < 400:
                    return ts_url
        except Exception:
            return None
        return None

    async def extract(self, url: str, **kwargs) -> Dict[str, Any]:
        if "vavoo.to" not in url:
            raise ExtractorError("Not a valid Vavoo URL")

        resolved_url = None
        stream_headers = None

        # New plugin logic: app ping signature + mediahub resolve first.
        sig = await self._get_auth_signature()
        if sig:
            candidate = await self._resolve_with_auth(url, sig)
            if candidate:
                candidate = await self._follow_stream_url(candidate)
                resolved_url = candidate
                stream_headers = {
                    "user-agent": self.api_ua,
                    "referer": "https://vavoo.to/",
                    "origin": "https://vavoo.to",
                    "mediahubmx-signature": sig,
                }
                logger.info("Using Auth Resolve Mode: %s", resolved_url)

        # Fallback plugin logic: live2 .ts + vavoo_auth from ping2.
        if not resolved_url:
            ts_url = await self._build_ts_fallback(url)
            if ts_url:
                resolved_url = ts_url
                stream_headers = {"user-agent": self.ts_ua}
                logger.info("Using TS Fallback Mode: %s", resolved_url)

        # Last fallback: direct URL with VAVOO UA.
        if not resolved_url:
            resolved_url = url
            stream_headers = {
                "user-agent": self.ts_ua,
                "referer": "https://vavoo.to/",
            }
            logger.info("Using Direct Fallback Mode: %s", resolved_url)

        return {
            "destination_url": resolved_url,
            "request_headers": stream_headers,
            "mediaflow_endpoint": self.mediaflow_endpoint,
        }

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
