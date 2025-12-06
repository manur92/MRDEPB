import re
import urllib.parse
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)

# Conditional import for DLHD detection
try:
    from extractors.dlhd import DLHDExtractor
except ImportError:
    DLHDExtractor = None

class ManifestRewriter:
    @staticmethod
    def rewrite_mpd_manifest(manifest_content: str, base_url: str, proxy_base: str, stream_headers: dict, clearkey_param: str = None, api_password: str = None) -> str:
        """Riscrive i manifest MPD (DASH) per passare attraverso il proxy."""
        try:
            # Aggiungiamo il namespace di default se non presente, per ET
            if 'xmlns' not in manifest_content:
                manifest_content = manifest_content.replace('<MPD', '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"', 1)

            root = ET.fromstring(manifest_content)
            ns = {'mpd': 'urn:mpeg:dash:schema:mpd:2011', 'cenc': 'urn:mpeg:cenc:2013', 'dashif': 'http://dashif.org/guidelines/clearKey'}
            
            # Registra i namespace per evitare prefissi ns0
            ET.register_namespace('', ns['mpd'])
            ET.register_namespace('cenc', ns['cenc'])
            ET.register_namespace('dashif', ns['dashif'])

            # Includiamo tutti gli header rilevanti passati dall'estrattore
            header_params = "".join([f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in stream_headers.items()])
            
            if api_password:
                header_params += f"&api_password={api_password}"

            def create_proxy_url(relative_url):
                absolute_url = urljoin(base_url, relative_url)
                encoded_url = urllib.parse.quote(absolute_url, safe='')
                return f"{proxy_base}/proxy/mpd/manifest.m3u8?d={encoded_url}{header_params}"

            # --- GESTIONE CLEARKEY STATICA ---
            if clearkey_param:
                # Se è presente il parametro clearkey, iniettiamo il ContentProtection
                # clearkey_param formato: id:key (hex)
                try:
                    kid_hex, key_hex = clearkey_param.split(':')
                    
                    # Crea l'elemento ContentProtection per ClearKey
                    cp_element = ET.Element('ContentProtection')
                    cp_element.set('schemeIdUri', 'urn:uuid:e2719d58-a985-b3c9-781a-007147f192ec')
                    cp_element.set('value', 'ClearKey')
                    
                    # Aggiungi l'elemento Laurl (License Acquisition URL)
                    # Puntiamo al nostro endpoint /license con i parametri necessari
                    license_url = f"{proxy_base}/license?clearkey={clearkey_param}"
                    if api_password:
                        license_url += f"&api_password={api_password}"
                    
                    # 1. Laurl standard (namespace MPD) - alcuni player lo usano
                    laurl_element = ET.SubElement(cp_element, '{urn:mpeg:dash:schema:mpd:2011}Laurl')
                    laurl_element.text = license_url
                    
                    # 2. dashif:Laurl (namespace DashIF) - standard de facto per ClearKey
                    laurl_dashif = ET.SubElement(cp_element, '{http://dashif.org/guidelines/clearKey}Laurl')
                    laurl_dashif.text = license_url
                    
                    # 3. Aggiungi cenc:default_KID per aiutare il player a identificare la chiave
                    # Formatta il KID con i trattini: 8-4-4-4-12
                    if len(kid_hex) == 32:
                        kid_guid = f"{kid_hex[:8]}-{kid_hex[8:12]}-{kid_hex[12:16]}-{kid_hex[16:20]}-{kid_hex[20:]}"
                        cp_element.set('{urn:mpeg:cenc:2013}default_KID', kid_guid)

                    # Inietta ContentProtection nel primo AdaptationSet trovato (o dove appropriato)
                    # Per semplicità, lo aggiungiamo a tutti gli AdaptationSet se non presente
                    adaptation_sets = root.findall('.//mpd:AdaptationSet', ns)
                    logger.info(f"🔎 Trovati {len(adaptation_sets)} AdaptationSet nel manifest.")
                    
                    for adaptation_set in adaptation_sets:
                        # RIMUOVI altri ContentProtection (es. Widevine, PlayReady) per forzare ClearKey
                        # Questo è fondamentale perché i browser preferiscono Widevine se presente
                        for cp in adaptation_set.findall('mpd:ContentProtection', ns):
                            scheme = cp.get('schemeIdUri', '').lower()
                            # ClearKey UUID: e2719d58-a985-b3c9-781a-007147f192ec
                            if 'e2719d58-a985-b3c9-781a-007147f192ec' not in scheme:
                                adaptation_set.remove(cp)
                                logger.info(f"🗑️ Rimosso ContentProtection conflittuale: {scheme}")

                        # Verifica se esiste già un ContentProtection ClearKey
                        existing_cp = False
                        for cp in adaptation_set.findall('mpd:ContentProtection', ns):
                            if cp.get('schemeIdUri') == 'urn:uuid:e2719d58-a985-b3c9-781a-007147f192ec':
                                existing_cp = True
                                break
                        
                        if not existing_cp:
                            adaptation_set.insert(0, cp_element)
                            logger.info(f"💉 Iniettato ContentProtection ClearKey statico in AdaptationSet")
                        else:
                            logger.info(f"⚠️ ContentProtection ClearKey già presente in AdaptationSet, salto iniezione.")

                except Exception as e:
                    logger.error(f"❌ Errore nel parsing del parametro clearkey: {e}")

            # --- GESTIONE PROXY LICENZE ESISTENTI ---
            # Cerca ContentProtection esistenti e riscrive le URL di licenza
            for cp in root.findall('.//mpd:ContentProtection', ns):
                # Cerca elementi che contengono URL di licenza (es. dashif:Laurl, laurl, ecc.)
                # Nota: Questo è un tentativo generico, potrebbe richiedere adattamenti per specifici schemi
                for child in cp:
                    if 'Laurl' in child.tag and child.text:
                        original_license_url = child.text
                        encoded_license_url = urllib.parse.quote(original_license_url, safe='')
                        proxy_license_url = f"{proxy_base}/license?url={encoded_license_url}{header_params}"
                        child.text = proxy_license_url
                        logger.info(f"🔄 Redirected License URL: {original_license_url} -> {proxy_license_url}")

            # Riscrive gli attributi 'media' e 'initialization' in <SegmentTemplate>
            for template_tag in root.findall('.//mpd:SegmentTemplate', ns):
                for attr in ['media', 'initialization']:
                    if template_tag.get(attr):
                        template_tag.set(attr, create_proxy_url(template_tag.get(attr)))
            
            # Riscrive l'attributo 'media' in <SegmentURL>
            for seg_url_tag in root.findall('.//mpd:SegmentURL', ns):
                if seg_url_tag.get('media'):
                    seg_url_tag.set('media', create_proxy_url(seg_url_tag.get('media')))

            # Riscrive BaseURL se presente
            for base_url_tag in root.findall('.//mpd:BaseURL', ns):
                if base_url_tag.text:
                    base_url_tag.text = create_proxy_url(base_url_tag.text)

            return ET.tostring(root, encoding='unicode', method='xml')

        except Exception as e:
            logger.error(f"❌ Errore durante la riscrittura del manifest MPD: {e}")
            return manifest_content # Restituisce il contenuto originale in caso di errore

    @staticmethod
    async def rewrite_manifest_urls(manifest_content: str, base_url: str, proxy_base: str, stream_headers: dict, original_channel_url: str = '', api_password: str = None, get_extractor_func=None) -> str:
        """✅ AGGIORNATA: Riscrive gli URL nei manifest HLS per passare attraverso il proxy (incluse chiavi AES)"""
        lines = manifest_content.split('\n')
        rewritten_lines = []

        # ✅ NUOVO: Logica speciale per VixSrc e DLHD
        # Determina se l'URL base è di VixSrc o DLHD per applicare la logica personalizzata.
        is_vixsrc_stream = False
        is_dlhd_stream = False
        logger.info(f"Manifest rewriter called with base_url: {base_url}, original_channel_url: {original_channel_url}")
        try:
            # Usiamo l'URL originale della richiesta per determinare l'estrattore
            # Questo è più affidabile di `base_url` che potrebbe essere già un URL di playlist.
            if get_extractor_func:
                original_request_url = stream_headers.get('referer') or stream_headers.get('Referer') or base_url
                logger.info(f"Using original_request_url for extractor detection: {original_request_url}")
                extractor = await get_extractor_func(original_request_url, {})
                logger.info(f"Extractor obtained: {type(extractor).__name__}")
                if hasattr(extractor, 'is_vixsrc') and extractor.is_vixsrc:
                    is_vixsrc_stream = True
                    logger.info("Rilevato stream VixSrc. Applicherò la logica di filtraggio qualità e non-proxy.")
                elif DLHDExtractor and isinstance(extractor, DLHDExtractor):
                    is_dlhd_stream = True
                    logger.info(f"✅ Rilevato stream DLHD (type: {type(extractor).__name__}). Proxierò solo la chiave AES, non i segmenti.")
                else:
                    logger.info(f"Extractor type: {type(extractor).__name__}, DLHDExtractor available: {DLHDExtractor is not None}")
            else:
                logger.info("No get_extractor_func provided")
        except Exception as e:
            # Se l'estrattore non viene trovato, procedi normalmente.
            logger.error(f"Error in extractor detection: {e}")
            pass
        logger.info(f"Stream detection result: is_dlhd_stream={is_dlhd_stream}, is_vixsrc_stream={is_vixsrc_stream}")

        if is_vixsrc_stream:
            streams = []
            for i, line in enumerate(lines):
                if line.startswith('#EXT-X-STREAM-INF:'):
                    bandwidth_match = re.search(r'BANDWIDTH=(\d+)', line)
                    if bandwidth_match:
                        bandwidth = int(bandwidth_match.group(1))
                        streams.append({'bandwidth': bandwidth, 'inf': line, 'url': lines[i+1]})
            
            if streams:
                # Filtra per la qualità più alta
                highest_quality_stream = max(streams, key=lambda x: x['bandwidth'])
                logger.info(f"VixSrc: Trovata qualità massima con bandwidth {highest_quality_stream['bandwidth']}.")
                
                # Ricostruisci il manifest solo con la qualità più alta e gli URL originali
                rewritten_lines.append('#EXTM3U')
                for line in lines:
                    if line.startswith('#EXT-X-MEDIA:') or line.startswith('#EXT-X-STREAM-INF:') or (line and not line.startswith('#')):
                        continue # Salta i vecchi tag di stream e media
                
                # Aggiungi i tag media e lo stream di qualità più alta
                rewritten_lines.extend([line for line in lines if line.startswith('#EXT-X-MEDIA:')])
                rewritten_lines.append(highest_quality_stream['inf'])
                rewritten_lines.append(highest_quality_stream['url'])
                return '\n'.join(rewritten_lines)

        # Logica standard per tutti gli altri stream
        # ✅ FIX: Assicuriamoci che il Referer originale venga preservato nei parametri h_
        # Se stream_headers contiene già un Referer (es. da VOE), usiamo quello.
        # Altrimenti, se non c'è, potremmo voler usare l'original_channel_url o il base_url,
        # ma per VOE è CRUCIALE che il Referer sia quello del sito embed (walterprettytheir.com), non del CDN.
        
        # Passiamo tutti gli header presenti in stream_headers come parametri h_
        # Questo assicura che header critici come X-Channel-Key (DLHD) o Referer specifici (Vavoo) non vengano persi.
        header_params = "".join([f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" for key, value in stream_headers.items()])
        
        if api_password:
            header_params += f"&api_password={api_password}"

        # Estrai query params dal base_url per ereditarli se necessario (es. token)
        base_parsed = urllib.parse.urlparse(base_url)
        base_query = base_parsed.query

        for line in lines:
            line = line.strip()
            
            # ✅ NUOVO: Gestione chiavi AES-128
            if line.startswith('#EXT-X-KEY:') and 'URI=' in line:
                # Trova e sostituisci l'URI della chiave AES
                uri_start = line.find('URI="') + 5
                uri_end = line.find('"', uri_start)
                
                if uri_start > 4 and uri_end > uri_start:
                    original_key_url = line[uri_start:uri_end]
                    
                    # ✅ CORREZIONE: Usa urljoin per costruire l'URL assoluto della chiave in modo sicuro.
                    absolute_key_url = urljoin(base_url, original_key_url)
                    
                    # Crea URL proxy per la chiave
                    encoded_key_url = urllib.parse.quote(absolute_key_url, safe='')
                    # ✅ AGGIUNTO: Passa l'URL originale del canale per l'invalidazione della cache
                    encoded_original_channel_url = urllib.parse.quote(original_channel_url, safe='')
                    proxy_key_url = f"{proxy_base}/key?key_url={encoded_key_url}&original_channel_url={encoded_original_channel_url}"

                    # Aggiungi gli header necessari come parametri h_
                    # Questo permette al gestore della chiave di usare il contesto corretto
                    # ✅ CORREZIONE: Passa tutti gli header rilevanti alla richiesta della chiave
                    # per garantire l'autenticazione corretta.
                    key_header_params = "".join(
                        [f"&h_{urllib.parse.quote(key)}={urllib.parse.quote(value)}" 
                         for key, value in stream_headers.items()]
                    )
                    proxy_key_url += key_header_params
                    
                    if api_password:
                        proxy_key_url += f"&api_password={api_password}"
                    
                    # Sostituisci l'URI nel tag EXT-X-KEY
                    new_line = line[:uri_start] + proxy_key_url + line[uri_end:]
                    rewritten_lines.append(new_line)
                    logger.info(f"🔄 Redirected AES key: {absolute_key_url} -> {proxy_key_url}")
                else:
                    rewritten_lines.append(line)
            
            # ✅ NUOVO: Gestione per i sottotitoli e altri media nel tag #EXT-X-MEDIA
            elif line.startswith('#EXT-X-MEDIA:') and 'URI=' in line:
                uri_start = line.find('URI="') + 5
                uri_end = line.find('"', uri_start)
                
                if uri_start > 4 and uri_end > uri_start:
                    original_media_url = line[uri_start:uri_end]
                    
                    # Costruisci l'URL assoluto e poi il proxy URL
                    absolute_media_url = urljoin(base_url, original_media_url)
                    encoded_media_url = urllib.parse.quote(absolute_media_url, safe='')
                    
                    # I sottotitoli sono manifest, quindi usano l'endpoint del proxy principale
                    # Per DLHD, anche i sottotitoli non vengono proxati se sono considerati media
                    if is_dlhd_stream:
                        new_line = line[:uri_start] + absolute_media_url + line[uri_end:]
                        rewritten_lines.append(new_line)
                        logger.info(f"🔄 DLHD: Media diretto: {absolute_media_url}")
                    else:
                        proxy_media_url = f"{proxy_base}/proxy/hls/manifest.m3u8?d={encoded_media_url}{header_params}"
                        new_line = line[:uri_start] + proxy_media_url + line[uri_end:]
                        rewritten_lines.append(new_line)
                        logger.info(f"🔄 Redirected Media URL: {absolute_media_url} -> {proxy_media_url}")
                else:
                    rewritten_lines.append(line)

            # ✅ NUOVO: Gestione per EXT-X-MAP (fMP4 initialization segment)
            elif line.startswith('#EXT-X-MAP:') and 'URI=' in line:
                uri_start = line.find('URI="') + 5
                uri_end = line.find('"', uri_start)
                
                if uri_start > 4 and uri_end > uri_start:
                    original_map_url = line[uri_start:uri_end]
                    absolute_map_url = urljoin(base_url, original_map_url)
                    
                    if is_dlhd_stream:
                         new_line = line[:uri_start] + absolute_map_url + line[uri_end:]
                         rewritten_lines.append(new_line)
                         logger.info(f"🔄 DLHD: MAP diretto: {absolute_map_url}")
                    else:
                        encoded_map_url = urllib.parse.quote(absolute_map_url, safe='')
                        # Usa l'endpoint segment.mp4 che è gestito da handle_proxy_request
                        proxy_map_url = f"{proxy_base}/proxy/hls/segment.mp4?d={encoded_map_url}{header_params}"
                        
                        new_line = line[:uri_start] + proxy_map_url + line[uri_end:]
                        rewritten_lines.append(new_line)
                        logger.info(f"🔄 Redirected MAP URL: {absolute_map_url} -> {proxy_map_url}")
                else:
                    rewritten_lines.append(line)

            # Gestione segmenti video e sub-manifest, sia relativi che assoluti
            elif line and not line.startswith('#'):
                # ✅ CORREZIONE: Riscrive qualsiasi URL relativo o assoluto che non sia un tag.
                # Distingue tra manifest (.m3u8, .css) e segmenti (.ts, .html, etc.).
                absolute_url = urljoin(base_url, line) if not line.startswith('http') else line

                # ✅ NUOVO: Eredita i query params (es. token) dal base_url se non presenti nel segmento
                if base_query and '?' not in absolute_url:
                    absolute_url += f"?{base_query}"

                if is_dlhd_stream:
                    # Per DLHD, non proxare i segmenti, usa l'URL assoluto diretto
                    rewritten_lines.append(absolute_url)
                    logger.info(f"🔄 DLHD: Segmento diretto: {absolute_url}")
                else:
                    # Per altri stream, proxare normalmente
                    encoded_url = urllib.parse.quote(absolute_url, safe='')

                    # I sub-manifest o URL che potrebbero contenere altri manifest vengono inviati all'endpoint proxy.
                    # ✅ RIPRISTINO LOGICA ORIGINALE (SEMPLIFICATA)
                    # Usiamo l'endpoint standard di EasyProxy per tutto, garantendo la massima compatibilità
                    # con la logica che "già funzionava".
                    
                    # Se è un manifest (.m3u8), usa l'endpoint manifest.
                    # Altrimenti, assumiamo sia un segmento (fMP4, TS, etc.) e usiamo l'endpoint segment.
                    if '.m3u8' in absolute_url:
                         proxy_url = f"{proxy_base}/proxy/manifest.m3u8?url={encoded_url}{header_params}"
                    else:
                         # Segmento (o altro file binario)
                         proxy_url = f"{proxy_base}/proxy/hls/segment.mp4?d={encoded_url}{header_params}"
                    
                    rewritten_lines.append(proxy_url)

            else:
                # Aggiunge tutti gli altri tag (es. #EXTINF, #EXT-X-ENDLIST)
                rewritten_lines.append(line)
        
        return '\n'.join(rewritten_lines)
