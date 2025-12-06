import logging
import sys
import os
from aiohttp import web

# Aggiungi path corrente per import moduli
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.hls_proxy import HLSProxy
from config import PORT

# Configurazione logging (già configurata in config.py ma utile per il main)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)

# --- Logica di Avvio ---
def create_app():
    """Crea e configura l'applicazione aiohttp."""
    proxy = HLSProxy()
    
    app = web.Application()
    
    # Registra le route
    app.router.add_get('/', proxy.handle_root)
    app.router.add_get('/favicon.ico', proxy.handle_favicon) # ✅ Route Favicon
    
    # ✅ Route Static Files (con path assoluto e creazione automatica)
    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    if not os.path.exists(static_path):
        os.makedirs(static_path)
    app.router.add_static('/static', static_path)
    
    app.router.add_get('/builder', proxy.handle_builder)
    app.router.add_get('/info', proxy.handle_info_page)
    app.router.add_get('/api/info', proxy.handle_api_info)
    app.router.add_get('/key', proxy.handle_key_request)
    app.router.add_get('/proxy/manifest.m3u8', proxy.handle_proxy_request)
    app.router.add_get('/proxy/hls/manifest.m3u8', proxy.handle_proxy_request)
    app.router.add_get('/proxy/mpd/manifest.m3u8', proxy.handle_proxy_request)
    # ✅ NUOVO: Endpoint generico per stream (compatibilità MFP)
    app.router.add_get('/proxy/stream', proxy.handle_proxy_request)
    app.router.add_get('/extractor', proxy.handle_extractor_request)
    # ✅ NUOVO: Endpoint compatibilità MFP per estrazione
    app.router.add_get('/extractor/video', proxy.handle_extractor_request)
    
    # ✅ NUOVO: Route per segmenti con estensioni corrette per compatibilità player
    app.router.add_get('/proxy/hls/segment.ts', proxy.handle_proxy_request)
    app.router.add_get('/proxy/hls/segment.m4s', proxy.handle_proxy_request)
    app.router.add_get('/proxy/hls/segment.mp4', proxy.handle_proxy_request)
    
    app.router.add_get('/playlist', proxy.handle_playlist_request)
    app.router.add_get('/segment/{segment}', proxy.handle_ts_segment)
    app.router.add_get('/decrypt/segment.mp4', proxy.handle_decrypt_segment) # ✅ NUOVO ROUTE
    
    # Route per licenze DRM (GET e POST)
    app.router.add_get('/license', proxy.handle_license_request)
    app.router.add_post('/license', proxy.handle_license_request)
    
    # ✅ NUOVO: Endpoint per generazione URL (compatibilità MFP)
    app.router.add_post('/generate_urls', proxy.handle_generate_urls)

    # ✅ NUOVO: Endpoint per ottenere l'IP pubblico
    app.router.add_get('/proxy/ip', proxy.handle_proxy_ip)
    
    # Gestore OPTIONS generico per CORS
    app.router.add_route('OPTIONS', '/{tail:.*}', proxy.handle_options)
    
    async def cleanup_handler(app):
        await proxy.cleanup()
    app.on_cleanup.append(cleanup_handler)
    
    return app

# Crea l'istanza "privata" dell'applicazione aiohttp.
app = create_app()

def main():
    """Funzione principale per avviare il server."""
    # Workaround per il bug di asyncio su Windows con ConnectionResetError
    if sys.platform == 'win32':
        # Silenzia il logger di asyncio per evitare spam di ConnectionResetError
        logging.getLogger('asyncio').setLevel(logging.CRITICAL)

    print("🚀 Avvio HLS Proxy Server...")
    print(f"📡 Server disponibile su: http://localhost:{PORT}")
    print(f"📡 Oppure: http://server-ip:{PORT}")
    print("🔗 Endpoints:")
    print("   • / - Pagina principale")
    print("   • /builder - Interfaccia web per il builder di playlist")
    print("   • /info - Pagina con informazioni sul server")
    print("   • /proxy/manifest.m3u8?url=<URL> - Proxy principale per stream")
    print("   • /playlist?url=<definizioni> - Generatore di playlist")
    print("=" * 50)
    
    web.run_app(
        app, # Usa l'istanza aiohttp originale per il runner integrato
        host='0.0.0.0',
        port=PORT
    )

if __name__ == '__main__':
    main()