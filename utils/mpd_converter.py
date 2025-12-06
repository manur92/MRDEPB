import xml.etree.ElementTree as ET
import urllib.parse
from urllib.parse import urljoin
import logging
import os

logger = logging.getLogger(__name__)

class MPDToHLSConverter:
    """Converte manifest MPD (DASH) in playlist HLS (m3u8) on-the-fly."""
    
    def __init__(self):
        self.ns = {
            'mpd': 'urn:mpeg:dash:schema:mpd:2011',
            'cenc': 'urn:mpeg:cenc:2013'
        }

    def convert_master_playlist(self, manifest_content: str, proxy_base: str, original_url: str, params: str) -> str:
        """Genera la Master Playlist HLS dagli AdaptationSet del MPD."""
        try:
            if 'xmlns' not in manifest_content:
                manifest_content = manifest_content.replace('<MPD', '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"', 1)
            
            root = ET.fromstring(manifest_content)
            lines = ['#EXTM3U', '#EXT-X-VERSION:3']
            
            # Trova AdaptationSet Video e Audio
            video_sets = []
            audio_sets = []
            
            for adaptation_set in root.findall('.//mpd:AdaptationSet', self.ns):
                mime_type = adaptation_set.get('mimeType', '')
                content_type = adaptation_set.get('contentType', '')
                
                if 'video' in mime_type or 'video' in content_type:
                    video_sets.append(adaptation_set)
                elif 'audio' in mime_type or 'audio' in content_type:
                    audio_sets.append(adaptation_set)
            
            # Fallback per detection
            if not video_sets and not audio_sets:
                for adaptation_set in root.findall('.//mpd:AdaptationSet', self.ns):
                    if adaptation_set.find('mpd:Representation[@mimeType="video/mp4"]', self.ns) is not None:
                        video_sets.append(adaptation_set)
                    elif adaptation_set.find('mpd:Representation[@mimeType="audio/mp4"]', self.ns) is not None:
                        audio_sets.append(adaptation_set)

            # --- GESTIONE AUDIO (EXT-X-MEDIA) ---
            audio_group_id = 'audio'
            has_audio = False
            
            for adaptation_set in audio_sets:
                for representation in adaptation_set.findall('mpd:Representation', self.ns):
                    rep_id = representation.get('id')
                    bandwidth = representation.get('bandwidth', '128000') # Default fallback
                    
                    # Costruisci URL Media Playlist Audio
                    encoded_url = urllib.parse.quote(original_url, safe='')
                    media_url = f"{proxy_base}/proxy/hls/manifest.m3u8?d={encoded_url}&format=hls&rep_id={rep_id}{params}"
                    
                    # Usa GROUP-ID 'audio' e NAME basato su ID o lingua
                    lang = adaptation_set.get('lang', 'und')
                    name = f"Audio {lang} ({bandwidth})"
                    
                    # EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="...",DEFAULT=YES,AUTOSELECT=YES,URI="..."
                    # Impostiamo DEFAULT=YES solo per il primo
                    default_attr = "YES" if not has_audio else "NO"
                    
                    lines.append(f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="{audio_group_id}",NAME="{name}",LANGUAGE="{lang}",DEFAULT={default_attr},AUTOSELECT=YES,URI="{media_url}"')
                    has_audio = True

            # --- GESTIONE VIDEO (EXT-X-STREAM-INF) ---
            for adaptation_set in video_sets:
                for representation in adaptation_set.findall('mpd:Representation', self.ns):
                    rep_id = representation.get('id')
                    bandwidth = representation.get('bandwidth')
                    width = representation.get('width')
                    height = representation.get('height')
                    frame_rate = representation.get('frameRate')
                    codecs = representation.get('codecs')
                    
                    encoded_url = urllib.parse.quote(original_url, safe='')
                    media_url = f"{proxy_base}/proxy/hls/manifest.m3u8?d={encoded_url}&format=hls&rep_id={rep_id}{params}"
                    
                    inf = f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth}'
                    if width and height:
                        inf += f',RESOLUTION={width}x{height}'
                    if frame_rate:
                        inf += f',FRAME-RATE={frame_rate}'
                    if codecs:
                        inf += f',CODECS="{codecs}"'
                    
                    # Collega il gruppo audio se presente
                    if has_audio:
                        inf += f',AUDIO="{audio_group_id}"'
                    
                    lines.append(inf)
                    lines.append(media_url)
            
            return '\n'.join(lines)
        except Exception as e:
            logging.error(f"Errore conversione Master Playlist: {e}")
            return "#EXTM3U\n#EXT-X-ERROR: " + str(e)

    def convert_media_playlist(self, manifest_content: str, rep_id: str, proxy_base: str, original_url: str, params: str, clearkey_param: str = None) -> str:
        """Genera la Media Playlist HLS per una specifica Representation."""
        try:
            if 'xmlns' not in manifest_content:
                manifest_content = manifest_content.replace('<MPD', '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"', 1)
                
            root = ET.fromstring(manifest_content)
            
            # --- RILEVAMENTO LIVE vs VOD ---
            mpd_type = root.get('type', 'static')
            is_live = mpd_type.lower() == 'dynamic'
            
            # Trova la Representation specifica
            representation = None
            adaptation_set = None
            
            # Cerca in tutti gli AdaptationSet
            for aset in root.findall('.//mpd:AdaptationSet', self.ns):
                rep = aset.find(f'mpd:Representation[@id="{rep_id}"]', self.ns)
                if rep is not None:
                    representation = rep
                    adaptation_set = aset
                    break
            
            if representation is None:
                logger.error(f"❌ Representation {rep_id} non trovata nel manifest.")
                return "#EXTM3U\n#EXT-X-ERROR: Representation not found"

            # fMP4 richiede HLS versione 6 o 7
            # Per LIVE: non usare VOD e forza partenza dal live edge
            if is_live:
                lines = ['#EXTM3U', '#EXT-X-VERSION:7']
                # Forza il player a partire dal live edge (fine della playlist)
                lines.append('#EXT-X-START:TIME-OFFSET=-3.0,PRECISE=YES')
            else:
                lines = ['#EXTM3U', '#EXT-X-VERSION:7', '#EXT-X-TARGETDURATION:10', '#EXT-X-PLAYLIST-TYPE:VOD']
            
            # --- GESTIONE DRM (ClearKey) ---
            # Decrittazione lato server con mp4decrypt
            server_side_decryption = False
            decryption_params = ""
            
            if clearkey_param:
                try:
                    kid_hex, key_hex = clearkey_param.split(':')
                    server_side_decryption = True
                    decryption_params = f"&key={key_hex}&key_id={kid_hex}"
                    # Server-side decryption enabled
                except Exception as e:
                    logger.error(f"Errore parsing clearkey_param: {e}")

            # --- GESTIONE SEGMENTI ---
            # SegmentTemplate è il caso più comune per lo streaming live/vod moderno
            segment_template = representation.find('mpd:SegmentTemplate', self.ns)
            if segment_template is None:
                # Fallback: cerca nell'AdaptationSet
                segment_template = adaptation_set.find('mpd:SegmentTemplate', self.ns)
            
            if segment_template is not None:
                timescale = int(segment_template.get('timescale', '1'))
                initialization = segment_template.get('initialization')
                media = segment_template.get('media')
                start_number = int(segment_template.get('startNumber', '1'))
                
                # Risolvi URL base
                base_url_tag = root.find('mpd:BaseURL', self.ns)
                base_url = base_url_tag.text if base_url_tag is not None else os.path.dirname(original_url)
                if not base_url.endswith('/'): base_url += '/'

                # --- INITIALIZATION SEGMENT (EXT-X-MAP) ---
                encoded_init_url = ""
                if initialization:
                    # Processing initialization segment
                    init_url = initialization.replace('$RepresentationID$', str(rep_id))
                    full_init_url = urljoin(base_url, init_url)
                    encoded_init_url = urllib.parse.quote(full_init_url, safe='')
                    
                    # Aggiungiamo EXT-X-MAP solo se NON usiamo decrittazione server
                    # Quando usiamo ffmpeg per decrittare, ogni segmento include già il moov
                    if not server_side_decryption:
                        proxy_init_url = f"{proxy_base}/segment/init.mp4?base_url={encoded_init_url}{params}"
                        lines.append(f'#EXT-X-MAP:URI="{proxy_init_url}"')

                # --- SEGMENT TIMELINE ---
                segment_timeline = segment_template.find('mpd:SegmentTimeline', self.ns)
                if segment_timeline is not None:
                    # Prima raccogli tutti i segmenti
                    all_segments = []
                    current_time = 0
                    segment_number = start_number
                    
                    for s in segment_timeline.findall('mpd:S', self.ns):
                        t = s.get('t')
                        if t: current_time = int(t)
                        d = int(s.get('d'))
                        r = int(s.get('r', '0'))
                        
                        duration_sec = d / timescale
                        
                        # Ripeti per r + 1 volte
                        for _ in range(r + 1):
                            all_segments.append({
                                'time': current_time,
                                'number': segment_number,
                                'duration': duration_sec,
                                'd': d
                            })
                            current_time += d
                            segment_number += 1
                    
                    # Per LIVE: FILTRA solo gli ultimi N segmenti per forzare partenza dal live edge
                    # Questo è necessario perché molti player (Stremio, ExoPlayer) ignorano EXT-X-START
                    # Per VOD: prendi tutti normalmente
                    segments_to_use = all_segments
                    
                    if is_live and len(all_segments) > 0:
                        # ✅ FIX LIVE: Includi solo gli ultimi ~30 secondi di segmenti
                        # Questo forza il player a partire dal live edge invece che dall'inizio del DVR
                        LIVE_WINDOW_SECONDS = 30
                        total_duration = 0
                        live_segments = []
                        
                        # Prendi segmenti dalla fine fino a raggiungere ~30 secondi
                        for seg in reversed(all_segments):
                            live_segments.insert(0, seg)
                            total_duration += seg['duration']
                            if total_duration >= LIVE_WINDOW_SECONDS:
                                break
                        
                        segments_to_use = live_segments
                        logger.info(f"🔴 LIVE: Filtrati {len(live_segments)}/{len(all_segments)} segmenti (ultimi ~{total_duration:.1f}s)")
                        
                        # Calcola TARGETDURATION dal segmento più lungo
                        max_duration = max(seg['duration'] for seg in segments_to_use)
                        lines.insert(2, f'#EXT-X-TARGETDURATION:{int(max_duration) + 1}')
                        # MEDIA-SEQUENCE indica il primo segmento disponibile
                        first_seg_number = segments_to_use[0]['number']
                        lines.append(f'#EXT-X-MEDIA-SEQUENCE:{first_seg_number}')
                    else:
                        lines.append('#EXT-X-MEDIA-SEQUENCE:0')
                    
                    for seg in segments_to_use:
                        # Costruisci URL segmento
                        seg_name = media.replace('$RepresentationID$', str(rep_id))
                        seg_name = seg_name.replace('$Number$', str(seg['number']))
                        seg_name = seg_name.replace('$Time$', str(seg['time']))
                        
                        full_seg_url = urljoin(base_url, seg_name)
                        encoded_seg_url = urllib.parse.quote(full_seg_url, safe='')
                        
                        lines.append(f'#EXTINF:{seg["duration"]:.3f},')
                        
                        if server_side_decryption:
                            # Usa endpoint di decrittazione
                            # Passiamo init_url perché serve per la concatenazione
                            decrypt_url = f"{proxy_base}/decrypt/segment.mp4?url={encoded_seg_url}&init_url={encoded_init_url}{decryption_params}{params}"
                            lines.append(decrypt_url)
                        else:
                            # Proxy standard
                            proxy_seg_url = f"{proxy_base}/segment/{seg_name}?base_url={encoded_seg_url}{params}"
                            lines.append(proxy_seg_url)
                
                # --- SEGMENT TEMPLATE (DURATION) ---
                else:
                    duration = int(segment_template.get('duration', '0'))
                    if duration > 0:
                        # Stima o limite segmenti (per VOD/Live senza timeline è complicato sapere quanti sono)
                        # Per ora generiamo un numero fisso o basato sulla durata periodo se disponibile
                        period = root.find('mpd:Period', self.ns)
                        period_duration_str = period.get('duration')
                        # Parsing durata ISO8601 (semplificato)
                        # TODO: Implementare parsing durata reale
                        total_segments = 100 # Placeholder
                        
                        duration_sec = duration / timescale
                        
                        for i in range(total_segments):
                            seg_num = start_number + i
                            seg_name = media.replace('$RepresentationID$', str(rep_id))
                            seg_name = seg_name.replace('$Number$', str(seg_num))
                            
                            full_seg_url = urljoin(base_url, seg_name)
                            encoded_seg_url = urllib.parse.quote(full_seg_url, safe='')
                            proxy_seg_url = f"{proxy_base}/segment/seg_{seg_num}.m4s?base_url={encoded_seg_url}{params}"
                            
                            lines.append(f'#EXTINF:{duration_sec:.6f},')
                            lines.append(proxy_seg_url)

            # Per VOD aggiungi ENDLIST, per LIVE no (indica stream in corso)
            if not is_live:
                lines.append('#EXT-X-ENDLIST')
            
            return '\n'.join(lines)

        except Exception as e:
            logging.error(f"Errore conversione Media Playlist: {e}")
            return "#EXTM3U\n#EXT-X-ERROR: " + str(e)