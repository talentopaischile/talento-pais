"""
detector_sinergias.py — Talento País
Pipeline ETAPA 2.5: Detecta dos tipos de sinergias y las combina.

TIPO A — Institucional (Groq):
  Google News RSS + scraping ministerios → Llama 3.3 extrae colaboraciones
  reales entre instituciones públicas (CORFO-ANID, Min.Energía-AGCID, etc.)

TIPO B — Match educación-industria (algorítmico, sin Groq):
  Cruza programas_relevantes_por_sector (carreras_estrategicas.json)
  con oportunidades.json para detectar:
    "INACAP Antofagasta forma técnicos en minería → SQM demanda ese perfil
     en Antofagasta → CORFO debería intermediar la alianza"

Corre después de processor.py (necesita oportunidades.json).

Salida: datos/procesados/sinergias_ia.json
"""

import os
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(), logging.FileHandler("detector_sinergias.log", encoding="utf-8")])
log = logging.getLogger(__name__)

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"
BASE_DIR     = Path(__file__).parent.parent
RAW_DIR      = BASE_DIR / "datos" / "raw"
PROC_DIR     = BASE_DIR / "datos" / "procesados"
PROC_DIR.mkdir(parents=True, exist_ok=True)
PLANES_JSON  = RAW_DIR / "planes_estrategicos.json"
HEADERS      = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
SECTORES = {
    "litio": {
        "label": "Litio y Minería",
        "queries": [
            "litio Chile acuerdo colaboración ministerio when:30d",
            "minería Chile convenio institución universidad when:30d",
        ],
    },
    "energias_renovables": {
        "label": "Energías Renovables",
        "queries": [
            "energía renovable Chile acuerdo colaboración institucional when:30d",
            "hidrógeno verde Chile ministerio empresa convenio when:30d",
        ],
    },
    "ia_tecnologia": {
        "label": "IA y Tecnología",
        "queries": [
            "inteligencia artificial Chile LATAM colaboración ministerio when:30d",
            "tecnología digital Chile universidad empresa programa when:30d",
        ],
    },
    "astronomia": {
        "label": "Astronomía",
        "queries": [
            "astronomía Chile convenio colaboración internacional when:30d",
            "observatorio Chile ciencia acuerdo institución when:30d",
        ],
    },
    "oceanografia": {
        "label": "Oceanografía",
        "queries": [
            "oceanografía Chile LATAM colaboración investigación marina when:30d",
            "recursos marinos Chile ministerio universidad acuerdo when:30d",
        ],
    },
    "asia_pacifico": {
        "label": "Asia-Pacífico",
        "queries": [
            "Chile Asia Pacífico cooperación institucional acuerdo when:30d",
            "Chile Corea Japón China convenio colaboración ciencia when:30d",
        ],
    },
}

# ─── Páginas de ministerios y organizaciones ──────────────────────────────────
FUENTES_MINISTERIOS = [
    {
        "nombre": "CORFO Sala de Prensa",
        "url": "https://www.corfo.gob.cl/sites/cpp/sala-de-prensa/",
        "sectores": ["litio", "energias_renovables", "ia_tecnologia"],
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
    {
        "nombre": "AGCID Cooperación Internacional",
        "url": "https://www.agcid.gob.cl/noticias/",
        "sectores": ["asia_pacifico", "energias_renovables", "ia_tecnologia"],
        "verify_ssl": True,
    },
    {
        "nombre": "Ministerio del Trabajo",
        "url": "https://www.mintrab.gob.cl/noticias/",
        "sectores": ["litio", "energias_renovables", "ia_tecnologia"],
        "verify_ssl": True,
    },
    {
        "nombre": "DECYTI Cancillería",
        "url": "https://www.minrel.gob.cl/noticias/",
        "sectores": ["asia_pacifico", "astronomia", "oceanografia"],
        "verify_ssl": True,
    },
    {
        "nombre": "BCN Asia-Pacífico",
        "url": "https://www.bcn.cl/observatorio/asiapacifico/",
        "sectores": ["asia_pacifico"],
        "verify_ssl": True,
    },
    {
        "nombre": "Servicio Civil DPS",
        "url": "https://www.serviciocivil.cl/noticias/",
        "sectores": ["litio", "energias_renovables", "ia_tecnologia", "astronomia", "oceanografia", "asia_pacifico"],
        "verify_ssl": True,
    },
]

# ─── Prompt anti-alucinación para Groq ───────────────────────────────────────
PROMPT = """\
Eres un analista de políticas públicas chilenas. Tu única tarea es EXTRAER \
(no inventar, no inferir, no completar) colaboraciones institucionales que \
estén EXPLÍCITAMENTE descritas en el texto que se te entrega.

REGLAS ESTRICTAS:
1. Solo incluye colaboraciones que aparezcan textualmente en el texto.
2. NUNCA inventes actores, acuerdos o descripciones que no estén en el texto.
3. Si el texto no menciona una colaboración concreta entre dos instituciones \
nombradas, NO la incluyas.
4. El campo "evidencia" debe contener la frase EXACTA del texto que respalda \
la colaboración — cópiala sin modificar.
5. Si no hay evidencia suficiente, devuelve: []

Sector analizado: {sector_label}

Devuelve EXCLUSIVAMENTE un JSON array válido, sin markdown ni texto adicional:
[
  {{
    "actor_a": "nombre exacto de institución 1 tal como aparece en el texto",
    "actor_b": "nombre exacto de institución 2 tal como aparece en el texto",
    "actor_c": "tercera institución si aparece en el texto, si no: null",
    "tipo_sinergia": "uno de: investigación conjunta | financiamiento | \
capacitación | regulación | transferencia tecnológica | \
cooperación internacional | desarrollo de políticas",
    "descripcion": "resumen fiel en máximo 2 oraciones, sin agregar información",
    "evidencia": "frase textual del texto que respalda esta colaboración",
    "fuente": "URL o nombre de la fuente donde se encontró"
  }}
]

Si no hay colaboraciones con respaldo textual claro, devuelve solo: []

Texto a analizar:
{texto}
"""


# ── 1. Google News RSS ────────────────────────────────────────────────────────

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


# ── 2. Scraping de ministerios ────────────────────────────────────────────────

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


# ── 3. Groq (Llama 3.3) — extracción estructurada ────────────────────────────

def llamar_groq(sector_key: str, sector_label: str, textos: list[str]) -> list[dict]:
    """
    Envía los textos a Groq (Llama 3.3 70B) y parsea la respuesta JSON.
    Plan gratuito: 1000 req/día, sin tarjeta de crédito requerida.
    """
    if not GROQ_API_KEY:
        log.warning("  GROQ_API_KEY no configurada — saltando análisis IA")
        return []

    # Combinar textos (límite ~6000 chars)
    texto_combinado = "\n\n---\n\n".join(textos)[:6000]
    prompt = PROMPT.format(sector_label=sector_label, texto=texto_combinado)

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 1200,
    }

    for intento in range(2):   # hasta 2 intentos (1 reintento en caso de 429)
        try:
            r = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)

            # 429 Too Many Requests → esperar y reintentar
            if r.status_code == 429:
                espera = 20 if intento == 0 else 40
                log.warning(f"  Groq 429 ({sector_key}): rate limit, esperando {espera}s...")
                time.sleep(espera)
                continue

            r.raise_for_status()
            resp = r.json()

            texto_resp = resp.get("choices", [{}])[0].get("message", {}).get("content", "[]").strip()

            # Limpiar markdown si el modelo devuelve ```json ... ```
            for p in texto_resp.split("```"):
                p = p.strip().lstrip("json").strip()
                if p.startswith("["):
                    texto_resp = p
                    break
            texto_resp = texto_resp.strip()
            if texto_resp == "[]" or not texto_resp:
                return []

            # Intentar parsear con raw_decode para ignorar texto extra tras el array
            decoder = json.JSONDecoder()
            sinergias = None
            # Buscar el primer '[' para iniciar la decodificación
            idx = texto_resp.find("[")
            if idx == -1:
                log.warning(f"  Groq sin array JSON válido ({sector_key})")
                return []
            try:
                sinergias, _ = decoder.raw_decode(texto_resp, idx)
            except json.JSONDecodeError as e:
                log.warning(f"  Groq respuesta no parseable ({sector_key}): {e}")
                return []
            if not isinstance(sinergias, list):
                log.warning(f"  Groq no devolvió lista ({sector_key})")
                return []

            # Agregar metadata a cada sinergia
            fecha_hoy = datetime.now().strftime("%Y-%m-%d")
            for s in sinergias:
                s["sector"]       = sector_key
                s["sector_label"] = sector_label
                s["fecha"]        = fecha_hoy
                s["estado"]       = "detectada"
                if not s.get("actor_c"):
                    s["actor_c"] = ""
                if not s.get("evidencia"):
                    s["evidencia"] = ""

            log.info(f"  Groq → {len(sinergias)} sinergias para '{sector_label}'")
            return sinergias

        except Exception as e:
            log.error(f"  Groq error ({sector_key}): {e}")
            return []

    log.error(f"  Groq: máximo de reintentos alcanzado ({sector_key})")
    return []


# ── MAIN ─────────────────────────────────────────────────────────────────────

def cargar_planes() -> dict[str, str]:
    """
    Carga datos/raw/planes_estrategicos.json si existe.
    Retorna dict sector → extracto de texto del plan.
    """
    if not PLANES_JSON.exists():
        log.info("  planes_estrategicos.json no encontrado — se omite contexto de planes")
        return {}
    try:
        with open(PLANES_JSON, encoding="utf-8") as f:
            planes = json.load(f)
        resultado: dict[str, str] = {}
        for plan in planes:
            sector = plan.get("sector", "")
            extracto = plan.get("extracto", "")
            actores = ", ".join(plan.get("actores_clave", [])[:8])
            if extracto and sector:
                texto = (
                    f"[{plan['nombre']} — {plan['organismo']}, {plan['año']}]\n"
                    f"Actores clave: {actores}\n"
                    f"Extracto: {extracto[:1500]}"
                )
                resultado[sector] = texto
        log.info(f"  planes_estrategicos.json: {len(resultado)} planes cargados")
        return resultado
    except Exception as e:
        log.warning(f"  Error cargando planes_estrategicos.json: {e}")
        return {}


# ── TIPO B: Match educación-industria (algorítmico, sin Groq) ────────────────

def detectar_matches_educacion_industria() -> list[dict]:
    """
    Cruza programas educativos por región con demanda laboral real para
    detectar matches institución-empresa accionables.

    Sin llamadas Groq — matching 100% algorítmico.
    Requiere: oportunidades.json (processor.py) + carreras_estrategicas.json
    """
    # ── Cargar programas relevantes ──────────────────────────────────────────
    carreras_path = RAW_DIR / "carreras_estrategicas.json"
    if not carreras_path.exists():
        log.warning("  carreras_estrategicas.json no encontrado — skip matches")
        return []

    with open(carreras_path, encoding="utf-8") as f:
        carreras = json.load(f)

    programas_por_sector = carreras.get("programas_relevantes_por_sector", {})
    if not programas_por_sector:
        log.warning("  programas_relevantes_por_sector vacío — corre preparar_mallas.py primero")
        return []

    # ── Cargar oportunidades laborales procesadas ────────────────────────────
    op_path = PROC_DIR / "oportunidades.json"
    if not op_path.exists():
        log.warning("  oportunidades.json no encontrado — skip matches")
        return []

    with open(op_path, encoding="utf-8") as f:
        oportunidades = json.load(f)

    # ── Indexar demanda laboral por (sector, region) → {organización: count} ─
    demanda: dict[tuple, dict] = {}
    for op in oportunidades:
        if op.get("tipo") != "oferta_laboral":
            continue
        sector = op.get("sector_principal", "")
        region = op.get("region", "No especificada")
        org    = (op.get("organizacion") or "").strip()
        if not (sector and region and org and org != "Desconocida"):
            continue
        key = (sector, region)
        demanda.setdefault(key, {})
        demanda[key][org] = demanda[key].get(org, 0) + 1

    if not demanda:
        log.warning("  Sin demanda laboral indexada — verifica oportunidades.json")
        return []

    # ── Generar matches ──────────────────────────────────────────────────────
    matches: list[dict] = []
    vistos:  set[str]   = set()
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    for sector, programas in programas_por_sector.items():
        sector_label = SECTORES.get(sector, {}).get("label", sector)

        for prog in programas:
            if prog.get("relevancia") != "alta":
                continue

            region = prog.get("region_sede", "No especificada")
            key    = (sector, region)
            if key not in demanda:
                continue

            inst    = prog["nomb_inst"]
            carrera = prog["nomb_carrera"].title()
            mat     = prog["matriculados"]

            # Top 3 empresas con más ofertas en esa región/sector
            top_empresas = sorted(demanda[key].items(), key=lambda x: -x[1])[:3]

            for empresa, n_ofertas in top_empresas:
                # Evitar duplicados y auto-matches
                clave = f"{inst.lower()}|{empresa.lower()}|{sector}|{region.lower()}"
                if clave in vistos or empresa.lower() == inst.lower():
                    continue
                vistos.add(clave)

                desc = (
                    f"{inst} ({region}) forma {mat:,} estudiantes en {carrera}. "
                    f"{empresa} publicó {n_ofertas} oferta{'s' if n_ofertas > 1 else ''} "
                    f"en {region} para el sector {sector_label}. "
                    f"Perfil de egresado compatible con la demanda detectada."
                )
                evidencia = (
                    f"{n_ofertas} oferta{'s' if n_ofertas > 1 else ''} laboral"
                    f"{'es' if n_ofertas > 1 else ''} de {empresa} en {region} "
                    f"detectada{'s' if n_ofertas > 1 else ''} por el scraper semanal."
                )

                matches.append({
                    "actor_a":       inst,
                    "actor_b":       empresa,
                    "actor_c":       "CORFO",
                    "tipo_sinergia": "match educación-industria",
                    "descripcion":   desc,
                    "evidencia":     evidencia,
                    "fuente":        "Pipeline Talento País — matching algorítmico",
                    "sector":        sector,
                    "sector_label":  sector_label,
                    "fecha":         fecha_hoy,
                    "estado":        "detectada",
                    "region":        region,
                    "carrera":       carrera,
                    "matriculados":  mat,
                    "n_ofertas":     n_ofertas,
                })

    log.info(f"  Matches educación-industria: {len(matches)} "
             f"({len(programas_por_sector)} sectores × regiones)")
    return matches


def main():
    log.info("=" * 60)
    log.info("TALENTO PAÍS — Detector de Sinergias IA (Etapa 2.5)")
    log.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info(f"Modelo: {GROQ_MODEL}")
    log.info("=" * 60)

    todas_sinergias: list[dict] = []

    log.info("\n[1/5] Detectando matches educación-industria (algorítmico)...")
    matches_edu = detectar_matches_educacion_industria()
    todas_sinergias.extend(matches_edu)

    log.info("\n[2/5] Cargando planes estratégicos nacionales...")
    textos_planes = cargar_planes()

    log.info("\n[3/5] Scrapeando ministerios y organismos...")
    textos_por_sector: dict[str, list[str]] = {k: [] for k in SECTORES}
    for fuente in FUENTES_MINISTERIOS:
        texto = scrape_pagina(fuente)
        if texto:
            for sec in fuente["sectores"]:
                textos_por_sector.setdefault(sec, []).append(texto)
        time.sleep(1)

    log.info("\n[4/5] Analizando sectores con Groq (Llama 3.3 70B)...")

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

        # Extracto del plan estratégico nacional del sector (si existe)
        plan_texto = textos_planes.get(sector_key) or textos_planes.get("transversal")
        if plan_texto:
            textos_sector.append(plan_texto)
            log.info(f"  + Contexto de plan estratégico nacional")

        log.info(f"  Textos recopilados: {len(textos_sector)}")

        if not textos_sector:
            log.warning(f"  Sin textos para {label}, saltando")
            continue

        sinergias = llamar_groq(sector_key, label, textos_sector)
        todas_sinergias.extend(sinergias)

        time.sleep(6)

    log.info("\n[5/5] Deduplicando y guardando...")
    vistas: set[str] = set()
    unicas = []
    for s in todas_sinergias:
        # Matches educación-industria incluyen región en la clave para no colapsar
        if s.get("tipo_sinergia") == "match educación-industria":
            k = (f"{s.get('actor_a','').lower()}|{s.get('actor_b','').lower()}"
                 f"|{s.get('sector','')}|{s.get('region','').lower()}")
        else:
            k = (f"{s.get('actor_a','').lower()}|{s.get('actor_b','').lower()}"
                 f"|{s.get('tipo_sinergia','').lower()}")
        if k not in vistas:
            vistas.add(k)
            unicas.append(s)

    tipo_a = sum(1 for s in unicas if s.get("tipo_sinergia") != "match educación-industria")
    tipo_b = sum(1 for s in unicas if s.get("tipo_sinergia") == "match educación-industria")
    log.info(f"\nSinergias institucionales (Groq): {tipo_a}")
    log.info(f"Matches educación-industria:      {tipo_b}")
    log.info(f"Total único:                      {len(unicas)}")
    out = PROC_DIR / "sinergias_ia.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False, indent=2)
    log.info(f"Guardado: {out} ({len(unicas)} sinergias)\nDetector de Sinergias completado.")


if __name__ == "__main__":
    main()
