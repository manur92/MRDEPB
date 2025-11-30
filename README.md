# 🚀 EasyProxy - Server Proxy Universale per Streaming HLS

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://docker.com)
[![HLS](https://img.shields.io/badge/HLS-Streaming-red.svg)](https://developer.apple.com/streaming/)

> **Un server proxy universale per streaming HLS, M3U8 e IPTV** 🎬  
> Supporto nativo per Vavoo, DaddyLive HD e tutti i servizi di streaming  
> Interfaccia web integrata e configurazione zero

---

## 📚 Indice

- [✨ Caratteristiche Principali](#-caratteristiche-principali)
- [💾 Setup Rapido](#-setup-rapido)
- [☁️ Deploy Cloud](#️-deploy-cloud)
- [💻 Installazione Locale](#-installazione-locale)
- [⚙️ Configurazione Proxy](#️-configurazione-proxy)
- [🧰 Utilizzo del Proxy](#-utilizzo-del-proxy)
- [🔧 Configurazione](#-configurazione)
- [📖 Architettura](#-architettura)

---

## ✨ Caratteristiche Principali

| 🎯 **Proxy Universale** | 🔐 **Estrattori Specializzati** | ⚡ **Performance** |
|------------------------|------------------------|-------------------|
| HLS, M3U8, MPD, DLHD streams, VIXSRC | Vavoo, DLHD, Sportsonline, VixSrc | Connessioni async e keep-alive |
| **🔓 DRM Decryption** | **🎬 MPD to HLS** | **🔑 ClearKey Support** |
| CENC decryption con PyCryptodome | Conversione automatica DASH → HLS | Server-side ClearKey per VLC |

| 🌐 **Multi-formato** | 🔄 **Retry Logic** | 🚀 **Scalabilità** |
|--------------------|-------------------|------------------|
| Supporto #EXTVLCOPT e #EXTHTTP | Tentativi automatici | Server asincrono |

| 🛠️ **Builder Integrato** | 📱 **Interfaccia Web** | 🔗 **Playlist Manager** |
|--------------------------|----------------------|---------------------|
| Combinazione playlist M3U | Dashboard completa | Gestione automatica headers |

---

## 💾 Setup Rapido

### 🐳 Docker (Raccomandato)

**Assicurati di avere un file `Dockerfile` e `requirements.txt` nella root del progetto.**

```bash
git clone https://github.com/nzo66/EasyProxy.git
cd EasyProxy
docker build -t EasyProxy .
docker run -d -p 7860:7860 --name EasyProxy EasyProxy
```

### 🐍 Python Diretto

```bash
git clone https://github.com/nzo66/EasyProxy.git
cd EasyProxy
pip install -r requirements.txt
gunicorn --bind 0.0.0.0:7860 --workers 4 --worker-class aiohttp.worker.GunicornWebWorker app:app
```

**Server disponibile su:** `http://localhost:7860`

---

## ☁️ Deploy Cloud

### ▶️ Render

1. **Projects** → **New → Web Service** → *Public Git Repository*
2. **Repository**: `https://github.com/nzo66/EasyProxy`
3. **Build Command**: `pip install -r requirements.txt`
4. **Start Command**: `gunicorn --bind 0.0.0.0:7860 --workers 4 --worker-class aiohttp.worker.GunicornWebWorker app:app`
5. **Deploy**

### 🤖 HuggingFace Spaces

1. Crea nuovo **Space** (SDK: *Docker*)
2. Carica tutti i file
3. Deploy automatico
4. **Pronto!**

### 🌐 Railway / Heroku

```bash
# Railway
railway login && railway init && railway up

# Heroku
heroku create EasyProxy && git push heroku main
```

### 🎯 Configurazione Cloud Ottimale

**Il proxy funziona senza configurazione!**

Ottimizzato per:
- ✅ **Piattaforme gratuite** (HuggingFace, Render Free)
- ✅ **Server limitati** (512MB - 1GB RAM)
- ✅ **Streaming diretto** senza cache
- ✅ **Massima compatibilità** con tutti i servizi

---

## 💻 Installazione Locale

### 📋 Requisiti

- **Python 3.8+**
- **aiohttp**
- **gunicorn**

### 🔧 Installazione Completa

```bash
# Clone repository
git clone https://github.com/nzo66/EasyProxy.git
cd EasyProxy

# Installa dipendenze
pip install -r requirements.txt

# Avvio 
gunicorn --bind 0.0.0.0:7860 --workers 4 --worker-class aiohttp.worker.GunicornWebWorker app:app
```

### 🐧 Termux (Android)

```bash
pkg update && pkg upgrade
pkg install python git -y
git clone https://github.com/nzo66/EasyProxy.git
cd EasyProxy
pip install -r requirements.txt
gunicorn --bind 0.0.0.0:7860 --workers 4 --worker-class aiohttp.worker.GunicornWebWorker app:app
```

### 🐳 Docker Avanzato

```bash
# Build personalizzata
docker build -t EasyProxy .

# Run con configurazioni personalizzate
docker run -d -p 7860:7860 \
  --name EasyProxy EasyProxy

# Run con volume per logs
docker run -d -p 7860:7860 \
  -v $(pwd)/logs:/app/logs \
  --name EasyProxy EasyProxy
```

---

## ⚙️ Configurazione Proxy

Il modo più semplice per configurare i proxy è tramite un file `.env`.

1.  **Crea un file `.env`** nella cartella principale del progetto (puoi rinominare il file `.env.example`).
2.  **Aggiungi le tue variabili proxy** al file `.env`.

**Esempio di file `.env`:**

```env
# Proxy globale per tutto il traffico
GLOBAL_PROXY=http://user:pass@myproxy.com:8080

# Proxy multipli per DLHD (uno verrà scelto a caso)
DLHD_PROXY=socks5://proxy1.com:1080,socks5://proxy2.com:1080

# Proxy specifico per Vavoo
VAVOO_PROXY=socks5://vavoo-proxy.net:9050
```

Le variabili supportate sono:
- `GLOBAL_PROXY`: Proxy di fallback per tutte le richieste.
- `VAVOO_PROXY`: Proxy specifico per le richieste a Vavoo.
- `DLHD_PROXY`: Proxy specifico per le richieste a DaddyLiveHD.

---

## 📚 API Endpoints

### 🔍 Extractor API (`/extractor/video`)

Questo endpoint **non può essere aperto direttamente** senza parametri. Serve per estrarre l'URL diretto dello stream da servizi supportati (come Vavoo, DLHD, ecc.).

**Info e Aiuto:**
Se apri `/extractor` o `/extractor/video` senza parametri, riceverai una risposta JSON con le istruzioni d'uso e la lista degli host supportati.

**Come si usa:**
**Come si usa:**
Devi aggiungere `?url=` (o `?d=`) seguito dal link del video che vuoi processare.

**Esempi Pratici:**

1.  **Ottenere il JSON con i dettagli (Default):**
    ```
    http://tuo-server:7860/extractor/video?url=https://vavoo.to/channel/123
    ```
    *Restituisce un JSON con `destination_url`, `request_headers`, ecc.*

2.  **Reindirizzare direttamente allo stream (Redirect):**
    Aggiungi `&redirect_stream=true`. Utile per mettere il link direttamente in un player.
    ```
    http://tuo-server:7860/extractor/video?url=https://daddylive.mp/stream/stream-1.php&redirect_stream=true
    ```
    *Il server risponderà con un redirect 302 verso l'URL del proxy pronto per la riproduzione.*

3.  **Specificare manualmente l'host (Bypass Auto-detect):**
    Se l'auto-detection fallisce, puoi forzare l'uso di un estrattore specifico con `host=`.
    ```
    http://tuo-server:7860/extractor/video?host=vavoo&url=https://custom-link.com/123
    ```

4.  **URL in Base64:**
    Puoi passare l'URL codificato in Base64 nel parametro `url` (o `d`). Il server lo decodificherà automaticamente.
    ```
    http://tuo-server:7860/extractor/video?url=aHR0cHM6Ly9leGFtcGxlLmNvbS92aWRlbw==
    ```

**Parametri:**
- `url` (o `d`): **(Obbligatorio)** L'URL originale del video o della pagina. Supporta URL in chiaro, URL Encoded o **Base64 Encoded**.
- `host`: (Opzionale) Forza l'uso di un estrattore specifico (es. `vavoo`, `dlhd`, `mixdrop`, `voe`, `streamtape`, `orion`).
- `redirect_stream`: 
  - `true`: Esegue un redirect immediato allo stream giocabile.
  - `false` (default): Restituisce i dati in formato JSON.
- `api_password`: (Opzionale) Password API se configurata.

**Servizi Supportati:**
Vavoo, DaddyLiveHD, Mixdrop, Orion, Sportsonline, Streamtape, VixSrc, Voe.

**Esempio di Risposta (JSON):**
```json
{
  "destination_url": "https://stream.example.com/video.m3u8",
  "request_headers": {
    "User-Agent": "Mozilla/5.0...",
    "Referer": "https://example.com/"
  },
  "endpoint_type": "hls_proxy",
  "proxy_url": "http://server:7860/proxy/manifest.m3u8?d=...",
  "query_params": {}
}
```

### 📺 Proxy Endpoints

Questi endpoint gestiscono il proxying effettivo dei flussi video.

- **`/proxy/manifest.m3u8`**: Endpoint principale per HLS. Gestisce anche la conversione automatica da DASH (MPD) a HLS.
- **`/proxy/hls/manifest.m3u8`**: Alias specifico per HLS.
- **`/proxy/mpd/manifest.m3u8`**: Forza il trattamento dell'input come DASH (MPD).
- **`/proxy/stream`**: Proxy universale per file statici (MP4, MKV, AVI) o stream progressivi.

**Parametri Comuni:**
- `url` (o `d`): URL dello stream originale.
- `h_<header>`: Headers personalizzati (es. `h_User-Agent=VLC`).
- `clearkey`: Chiavi di decrittazione DRM in formato `KID:KEY` (per stream MPD protetti).

### 🛠️ Utilities

- **`/builder`**: Interfaccia Web per il Playlist Builder.
- **`/playlist`**: Endpoint per processare intere playlist M3U remote.
- **`/info`**: Pagina HTML con lo stato del server e le versioni dei componenti.
- **`/api/info`**: API JSON che restituisce lo stato del server.
- **`/proxy/ip`**: Restituisce l'indirizzo IP pubblico del server (utile per debug VPN/Proxy).
- **`/generate_urls`** (POST): Genera URL proxy in batch (usato dal Builder).
- **`/license`**: Endpoint per gestire richieste di licenza DRM (se necessario).

---

## 🧰 Utilizzo del Proxy

Sostituisci `<server-ip>` con l'IP del tuo server.

### 🎯 Interfaccia Web Principale

```
http://<server-ip>:7860/
```

### 📺 Proxy HLS Universale

```
http://<server-ip>:7860/proxy/manifest.m3u8?url=<URL_STREAM>
```

**Supporta:**
- **HLS (.m3u8)** - Streaming live e VOD
- **M3U playlist** - Liste canali IPTV  
- **MPD (DASH)** - Streaming adattivo con conversione automatica HLS
- **MPD + ClearKey DRM** - Decrittazione server-side CENC (VLC compatible)
- **DLHD streams** - Flussi dinamici
- **VIXSRC** - Streaming VOD
- **Sportsonline** - Streaming sportivo

**Esempi:**
```bash
# Stream HLS generico
http://server:7860/proxy/manifest.m3u8?url=https://example.com/stream.m3u8

# MPD con ClearKey DRM (decrittazione server-side)
http://server:7860/proxy/manifest.m3u8?url=https://cdn.com/stream.mpd&clearkey=KID:KEY

# Playlist IPTV
http://server:7860/playlist?url=https://iptv-provider.com/playlist.m3u

# Stream con headers personalizzati
http://server:7860/proxy/manifest.m3u8?url=https://stream.com/video.m3u8&h_user-agent=VLC&h_referer=https://site.com
```

### 🔍 Estrazione Vavoo Automatico

**Risolve automaticamente:**
- Link vavoo.to in stream diretti
- Autenticazione API automatica
- Headers ottimizzati per streaming

### 📡 Risoluzione DaddyLive HD Automatico

**Funzionalità:**
- Risoluzione link DaddyLive HD
- Bypass automatico restrizioni
- Ottimizzazione qualità stream

### ⚽ Risoluzione Sportsonline/Sportzonline Automatico

**Funzionalità:**
- Risoluzione link da `sportsonline.*` e `sportzonline.*`
- Estrazione automatica da iframe
- Supporto per decodifica Javascript (P.A.C.K.E.R.)

### 🔗 Playlist Builder

```
http://<server-ip>:7860/builder
```

**Interfaccia completa per:**
- ✅ Combinare playlist multiple
- ✅ Gestione automatica Vavoo e DLHD
- ✅ Supporto #EXTVLCOPT e #EXTHTTP  
- ✅ Estrazione automatica #KODIPROP ClearKey
- ✅ Proxy automatico per tutti gli stream
- ✅ Compatibilità VLC, Kodi, IPTV players

### 🔑 Headers Personalizzati

Aggiungi headers con prefisso `h_`:

```
http://server:7860/proxy/manifest.m3u8?url=STREAM_URL&h_user-agent=CustomUA&h_referer=https://site.com&h_authorization=Bearer token123
```

**Headers supportati:**
- `h_user-agent` - User Agent personalizzato
- `h_referer` - Sito di riferimento  
- `h_authorization` - Token di autorizzazione
- `h_origin` - Dominio origine
- `h_*` - Qualsiasi header personalizzato

---

## 📖 Architettura

### 🔄 Flusso di Elaborazione

1. **Richiesta Stream** → Endpoint proxy universale
2. **Rilevamento Servizio** → Auto-detect Vavoo/DLHD/Generic
3. **Estrazione URL** → Risoluzione link reali
4. **Proxy Stream** → Forward con headers ottimizzati
5. **Risposta Client** → Stream diretto compatibile

### ⚡ Sistema Asincrono

- **aiohttp** - HTTP client non-bloccante
- **Connection pooling** - Riutilizzo connessioni
- **Retry automatico** - Gestione errori intelligente

### 🔐 Gestione Autenticazione

- **Vavoo** - Sistema signature automatico
- **DaddyLive** - Headers specializzati  
- **Generic** - Supporto Authorization standard

---

## 🎯 Esempi Pratici

### 📱 Player IPTV

Configura il tuo player con:
```
http://tuo-server:7860/proxy/manifest.m3u8?url=STREAM_URL
```

### 🎬 VLC Media Player

```bash
vlc "http://tuo-server:7860/proxy/manifest.m3u8?url=https://example.com/stream.m3u8"
```

### 📺 Kodi

Aggiungi come sorgente:
```
http://tuo-server:7860/proxy/manifest.m3u8?url=PLAYLIST_URL
```

### 🌐 Browser Web

Apri direttamente nel browser:
```
http://tuo-server:7860/proxy/manifest.m3u8?url=https://stream.example.com/live.m3u8
```

---

### 🔧 Gestione Docker

```bash
# Logs in tempo reale
docker logs -f EasyProxy

# Riavvio container
docker restart EasyProxy

# Stop/Start
docker stop EasyProxy
docker start EasyProxy

# Rimozione completa
docker rm -f EasyProxy
```

---

## 🚀 Prestazioni

### 📊 Benchmark Tipici

| **Metric** | **Valore** | **Descrizione** |
|------------|------------|-----------------|
| **Latenza** | <50ms | Overhead proxy minimo |
| **Throughput** | Unlimited | Limitato dalla banda disponibile |
| **Connessioni** | 1000+ | Simultanee supportate |
| **Memoria** | 50-200MB | Utilizzo tipico |

### ⚡ Ottimizzazioni

- **Connection Pooling** - Riutilizzo connessioni HTTP
- **Async I/O** - Gestione non-bloccante delle richieste
- **Keep-Alive** - Connessioni persistenti
- **DNS Caching** - Cache risoluzione domini

---

## 🤝 Contributi

I contributi sono benvenuti! Per contribuire:

1. **Fork** del repository
2. **Crea** un branch per le modifiche (`git checkout -b feature/AmazingFeature`)
3. **Commit** le modifiche (`git commit -m 'Add some AmazingFeature'`)
4. **Push** al branch (`git push origin feature/AmazingFeature`)
5. **Apri** una Pull Request

### 🐛 Segnalazione Bug

Per segnalare bug, apri una issue includendo:
- Versione del proxy
- Sistema operativo
- URL di test che causa il problema
- Log di errore completo

### 💡 Richieste Feature

Per nuove funzionalità, apri una issue descrivendo:
- Funzionalità desiderata
- Caso d'uso specifico
- Priorità (bassa/media/alta)

---

## 📄 Licenza

Questo progetto è distribuito sotto licenza MIT. Vedi il file `LICENSE` per maggiori dettagli.

---

<div align="center">

**⭐ Se questo progetto ti è utile, lascia una stella! ⭐**

> 🎉 **Enjoy Your Streaming!**  
> Accedi ai tuoi contenuti preferiti ovunque, senza restrizioni, con controllo completo e performance ottimizzate.

</div>
