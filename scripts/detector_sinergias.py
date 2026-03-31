"""
detector_sinergias.py — Talento País
Pipeline ETAPA 1.5: Detecta oportunidades de colaboración institucional
usando Google News RSS + scraping de ministerios + Gemini Flash (gratuito).

Fuentes:
  - Google News RSS (sin API key, totalmente gratis)
  - Páginas públicas de CORFO, AGCI, Min. Energía, ANID, CENIA, Min. Ciencia

Salida:
  datos/procesados/sinergias_ia.json
"""

import os
import json
import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("detector_sinergias.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash:generateContent?key={key}"
)

BASE_DIR = Path(__file__).parent.parent
PROC_DIR = BASE_DIR / "datos" / "procesados"
PROC_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ─── Sectores estratégicos ────────────────────────────────────────────────────
SECTORES = {
    "litio": {
        "label": "Litio y Minería",
        "queries": [
            "litio Chile colaboración ministerio universidad",
            "minería Chile acuerdo institución tecnología",
        ],
    },
    "energias_renovables": {
        "label": "Energías Renovables",
        "queries": [
            "energía solar eólica Chile colaboración institucional",
            "hidrógeno verde Chile ministerio empresa acuerdo",
        ],
    },
    "ia_tecnologia": {
        "label": "IA y Tecnología",
        "queries": [
            "inteligencia artificial Chile colaboración ministerio universidad",
            "tecnología digital Chile programa conjunto institución",
        ],
    },
    "astronomia": {
        "label": "Astronomía",
        "queries": [
            "astronomía Chile colaboración internacional observatorio",
            "ciencia Chile convenio cooperación astrofísica",
        ],
    },
    "oceanografia": {
        "label": "Oceanografía",
        "queries": [
            "oceanografía Chile colaboración investigación marina",
            "recursos marinos Chile ministerio universidad acuerdo",
        ],
    },
    "asia_pacifico": {
        "label": "Asia-Pacífico",
        "queries": [
            "Chile Asia Pacífico cooperación institucional 2025",
            "Chile Corea Japón China acuerdo colaboración ciencia",
        ],
    },
}

# ─── Páginas de ministerios y organizaciones ──────────────────────────────────
FUENTES_MINISTERIOS = [
    {
        "nombre": "CORFO Inicio",
        "url": "https://www.corfo.cl/sites/cpp/homecorfo",
        "sectores": ["litio", "energias_renovables", "ia_tecnologia"],
        "verify_ssl": True,
    },
    {
        "nombre": "AGCI Chile",
        "url": "https://www.agci.cl/",
        "sectores": ["asia_pacifico"],
        "verify_ssl": True,
    },
    {
        "nombre": "Ministerio de Energía Noticias",
        "url": "https://energia.gob.cl/noticias",
        "sectores": ["energias_renovables"],
        "verify_ssl": True,
    },
    {
        "nombre": "ANID Inicio",
        "url": "https://www.anid.cl/",
        "sectores": ["astronomia", "oceanografia", "ia_tecnologia", "litio"],
        "verify_ssl": True,
    },
    {
        "nombre": "CENIA Investigación IA",
        "url": "https://cenia.cl/investigacion/",
        "sectores": ["ia_tecnologia"],
        "verify_ssl": True,
    },
    {
        "nombre": "Ministerio de Ciencia Noticias",
        "url": "https://www.minciencia.gob.cl/noticias/",
        "sectores": ["astronomia", "oceanografia", "ia_tecnologia"],
        "verify_ssl": True,
    },
    {
        "nombre": "Ministerio de Minería",
        "url": "https://www.minmineria.gob.cl/",
        "sectores": ["litio"],
        "verify_ssl": False,   # SSL problemático, se omite verificación
    },
]

# ─── Prompt de extracción para Gemini ─────────────────────────────────────────
PROMPT = """\
Eres un analista de políticas públicas chilenas. Analiza el siguiente texto \
sobre el sector "{sector_label}" en Chile y extrae ÚNICAMENTE oportunidades \
CONCRETAS de colaboración institucional entre organizaciones (ministerios, \
universidades, empresas, centros de investigación, organismos internacionales).

Una oportunidad concreta debe:
- Mencionar al menos dos actores institucionales específicos con nombre.
- Describir una acción real o potencial de colaboración.

Devuelve EXCLUSIVAMENTE un JSON array válido, sin markdown ni texto adicional:
[
  {{
    "actor_a": "nombre exacto de institución 1",
    "actor_b": "nombre exacto de institución 2",
    "actor_c": "nombre de tercera institución o null",
    "tipo_sinergia": "uno de: investigación conjunta | financiamiento | \
capacitación | regulación | transferencia tecnológica | \
cooperación internacional | desarrollo de políticas",
    "descripcion": "descripción breve y concisa en máximo 2 oraciones",
    "fuente": "URL o nombre de la fuente donde se encontró"
  }}
]

Si no hay oportunidades concretas con actores identificables, devuelve solo: []

Texto a analizar:
{texto}
"""


# ════════════════════════════════════════════════════════════════════════════
# 1. GOOGLE NEWS RSS
# ════════════════════════════════════════════════════════════════════════════

def fetch_rss(query: str, max_items: int = 6) -> list[str]:
    """Descarga Google News RSS y retorna textos de los artículos."""
    url = (
        f"https://news.google.com/rss/search"
        f"?q={quote(query)}&hl=es-419&gl=CL&ceid=CL:es-419"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = root.findall(".//item")[:max_items]
        textos = []
        for item in items:
            titulo = item.findtext("title", "")
            desc   = item.findtext("description", "")
            link   = item.findtext("link", "")
            textos.append(f"Título: {titulo}\nResumen: {desc}\nURL: {link}")
        log.info(f"    RSS '{query[:40]}…': {len(textos)} noticias")
        return textos
    except Exception as e:
        log.warning(f"    RSS error ({query[:30]}): {e}")
        return []


# ════════════════════════════════════════════════════════════════════════════
# 2. SCRAPING DE MINISTERIOS
# ════════════════════════════════════════════════════════════════════════════

def scrape_pagina(info: dict) -> str:
    """Scrapea texto relevante de la página de un ministerio u organismo."""
    try:
        verify = info.get("verify_ssl", True)
        r = requests.get(info["url"], headers=HEADERS, timeout=20, verify=verify)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Eliminar nav/footer/scripts para no contaminar el texto
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        partes = []
        for tag in soup.find_all(["h1", "h2", "h3", "p", "li"], limit=60):
            t = tag.get_text(strip=True)
            if len(t) > 40:
                partes.append(t)

        texto = " | ".join(partes[:25])
        log.info(f"    Scrape '{info['nombre']}': {len(partes)} párrafos")
        return f"Fuente: {info['nombre']} ({info['url']})\n{texto[:2500]}"
    except Exception as e:
        log.warning(f"    Scrape error ({info['nombre']}): {e}")
        return ""


# ════════════════════════════════════════════════════════════════════════════
# 3. GEMINI FLASH — EXTRACCIÓN ESTRUCTURADA
# ════════════════════════════════════════════════════════════════════════════

def llamar_gemini(sector_key: str, sector_label: str, textos: list[str]) -> list[dict]:
    """
    Envía los textos a Gemini 1.5 Flash y parsea la respuesta JSON.
    Usa el plan gratuito de Google AI Studio (1M tokens/día, 15 RPM).
    """
    if not GEMINI_API_KEY:
        log.warning("  GEMINI_API_KEY no configurada — saltando análisis IA")
        return []

    # Combinar textos (límite ~6000 chars para no gastar tokens innecesarios)
    texto_combinado = "\n\n---\n\n".join(textos)[:6000]
    prompt = PROMPT.format(sector_label=sector_label, texto=texto_combinado)

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,     # Baja = más determinista
            "maxOutputTokens": 1200,
        },
    }

    try:
        url = GEMINI_URL.format(key=GEMINI_API_KEY)
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        resp = r.json()

        texto_resp = (
            resp.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "[]")
            .strip()
        )

        # Limpiar markdown si Gemini devuelve ```json ... ```
        if "```" in texto_resp:
            partes = texto_resp.split("```")
            for p in partes:
                p = p.strip()
                if p.startswith("json"):
                    p = p[4:].strip()
                if p.startswith("["):
                    texto_resp = p
                    break

        texto_resp = texto_resp.strip()
        if texto_resp == "[]" or not texto_resp:
            return []

        sinergias = json.loads(texto_resp)

        # Agregar metadata a cada sinergia
        fecha_hoy = datetime.now().strftime("%Y-%m-%d")
        for s in sinergias:
            s["sector"]       = sector_key
            s["sector_label"] = sector_label
            s["fecha"]        = fecha_hoy
            s["estado"]       = "detectada"
            if not s.get("actor_c"):
                s["actor_c"] = ""

        log.info(f"  Gemini → {len(sinergias)} sinergias para '{sector_label}'")
        return sinergias

    except json.JSONDecodeError as e:
        log.warning(f"  Gemini respuesta no parseable ({sector_key}): {e}")
        return []
    except Exception as e:
        log.error(f"  Gemini error ({sector_key}): {e}")
        return []


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("TALENTO PAÍS — Detector de Sinergias IA (Etapa 1.5)")
    log.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    todas_sinergias: list[dict] = []

    # ── 1. Scrapear páginas de ministerios (una sola vez) ──────────────────
    log.info("\n[1/2] Scrapeando ministerios y organismos...")
    textos_por_sector: dict[str, list[str]] = {k: [] for k in SECTORES}

    for fuente in FUENTES_MINISTERIOS:
        texto = scrape_pagina(fuente)
        if texto:
            for sec in fuente["sectores"]:
                textos_por_sector.setdefault(sec, []).append(texto)
        time.sleep(1)   # cortesía con los servidores

    # ── 2. Por sector: RSS + textos de ministerios → Gemini ───────────────
    log.info("\n[2/2] Analizando sectores con Gemini Flash...")

    for sector_key, sector_info in SECTORES.items():
        label = sector_info["label"]
        log.info(f"\n  === {label} ===")

        textos_sector: list[str] = []

        # Google News RSS
        for query in sector_info["queries"]:
            noticias = fetch_rss(query, max_items=5)
            textos_sector.extend(noticias)
            time.sleep(0.5)

        # Páginas de ministerios relevantes para este sector
        textos_sector.extend(textos_por_sector.get(sector_key, []))

        log.info(f"  Textos recopilados: {len(textos_sector)}")

        if not textos_sector:
            log.warning(f"  Sin textos para {label}, saltando")
            continue

        sinergias = llamar_gemini(sector_key, label, textos_sector)
        todas_sinergias.extend(sinergias)

        # Rate limiting: Gemini free = 15 RPM → esperar ≥4 segundos
        time.sleep(5)

    # ── 3. Deduplicar ─────────────────────────────────────────────────────
    vistas: set[str] = set()
    unicas: list[dict] = []
    for s in todas_sinergias:
        key = (
            f"{s.get('actor_a', '').lower()}|"
            f"{s.get('actor_b', '').lower()}|"
            f"{s.get('tipo_sinergia', '').lower()}"
        )
        if key not in vistas:
            vistas.add(key)
            unicas.append(s)

    log.info(f"\nTotal sinergias únicas: {len(unicas)}")

    # ── 4. Guardar ────────────────────────────────────────────────────────
    out = PROC_DIR / "sinergias_ia.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False, indent=2)
    log.info(f"Guardado: {out} ({len(unicas)} sinergias)")
    log.info("\nDetector de Sinergias completado.")


if __name__ == "__main__":
    main()
