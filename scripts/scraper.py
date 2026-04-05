"""
scraper.py — Talento País
Pipeline ETAPA 1: Recolección de datos desde fuentes estratégicas.

Fuentes:
  1.  CORFO              — sala-de-prensa / nacional
  2.  Ministerio de Ciencia (MinCiencia)
  3.  CENIA              — Centro Nacional de IA
  4.  Trabajando.com     — ofertas laborales
  5.  Bumeran Chile      — ofertas laborales
  6.  Mercado Público    — API licitaciones activas
  7.  Programa DPS           — Desarrollo Productivo Sostenible
  8.  AGCID                 — Agencia de Cooperación Internacional
  9.  Mintrab               — Ministerio del Trabajo
  10. DECYTI                — Cancillería, Diplomacia Científica
  11. BCN Asia-Pacífico     — Biblioteca del Congreso Nacional
  12. Getonboard.cl         — Demanda laboral tech/IA
  13. Indeed Chile          — Demanda laboral amplia
  14. Observatorio Laboral  — DESACTIVADO (dominio caído)
  15. INE                   — Estadísticas empleo por sector
  16. Mineduc               — carreras_estrategicas.json (brechas)
  17. ProChile              — Oportunidades exportación y mercados Asia-Pacífico
  18. COCHILCO              — Estadísticas y proyecciones minería litio/cobre
  19. ANID                  — Becas, Fondecyt, Fondef y concursos I+D
  20. SENCE                 — Capacitación laboral y becas sectoriales
  21. RemoteOK              — Empleos remotos tech/IA (API pública, sin clave)
  22. Adzuna Chile          — Ofertas laborales Chile (API gratuita, requiere clave)
  23. Jooble                — Agregador empleos Chile (API gratuita, requiere clave)

Salida:
  - datos/raw/        → JSON crudo por fuente
  - datos/procesados/ → JSON consolidado + análisis de brechas

Uso:
  python scripts/scraper.py
  python scripts/scraper.py --fuente mercadopublico
  python scripts/scraper.py --fuente dps
  python scripts/scraper.py --fuente educacion
"""

import os
import re
import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log", encoding="utf-8")])
log = logging.getLogger(__name__)

MERCADOPUBLICO_API_KEY = os.environ.get("Mercado_Publico_API_KEY", "PEGA_AQUI_TU_API_KEY_DE_MERCADOPUBLICO")
BASE_DIR   = Path(__file__).parent.parent / "datos"
RAW_DIR    = BASE_DIR / "raw"
PROC_DIR   = BASE_DIR / "procesados"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)
MINEDUC_CSV_PATH = RAW_DIR / "20250729_Matrícula_Ed_Superior_2025_PUBL_MRUN.csv"

# ─── Sectores estratégicos ────────────────────────────────────────────────────
SECTORES = {
    "litio": [
        "litio", "baterías litio", "salmuera", "hidrogeología", "minería litio",
        "extracción litio", "catódico", "celdas electroquímicas",
    ],
    "energias_renovables": [
        "energía solar", "energía eólica", "energía renovable", "hidrógeno verde",
        "electromovilidad", "almacenamiento energético", "transición energética",
        "geotermia", "biomasa", "eficiencia energética",
    ],
    "ia_tecnologia": [
        "inteligencia artificial", "machine learning", "deep learning",
        "ciencia de datos", "data science", "MLOps", "visión computacional",
        "procesamiento lenguaje natural", "NLP", "robótica",
    ],
    "astronomia": [
        "astronomía", "astrofísica", "telescopio", "cosmología", "ESO",
        "ALMA", "Rubin Observatory", "radioastronomía",
    ],
    "oceanografia": [
        "oceanografía", "oceanología", "corriente Humboldt", "ecosistema marino",
        "biología marina", "acuicultura", "glaciología", "fjords",
    ],
    "asia_pacifico": [
        "China", "Japón", "Corea", "Asia-Pacífico", "APEC",
        "exportación Asia", "cooperación Asia",
    ],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def get(url: str, timeout: int = 15) -> requests.Response | None:
    """GET con reintentos y manejo de errores."""
    for intento in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning(f"Intento {intento+1}/3 falló para {url}: {e}")
            time.sleep(2 ** intento)
    log.error(f"No se pudo obtener {url}")
    return None


def guardar_raw(nombre: str, datos: list[dict]) -> Path:
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = RAW_DIR / f"{nombre}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
    log.info(f"  Guardado: {path} ({len(datos)} registros)")
    return path


def detectar_sectores(texto: str) -> list[str]:
    """Devuelve qué sectores estratégicos menciona un texto."""
    texto_lower = texto.lower()
    return [
        sector
        for sector, keywords in SECTORES.items()
        if any(kw.lower() in texto_lower for kw in keywords)
    ]


# ── SCRAPER 1 — CORFO ────────────────────────────────────────────────────────

def scrapear_corfo() -> list[dict]:
    """
    Extrae noticias y convocatorias de CORFO desde su Sala de Prensa.
    Nota: la página de programas carga con JavaScript (no scrapeable con requests).
    La Sala de Prensa sí es scrapeable y contiene convocatorias relevantes.
    URL base correcta: corfo.gob.cl (corfo.cl redirige aquí)
    """
    log.info("=== CORFO ===")
    BASE_CORFO = "https://www.corfo.gob.cl"
    urls = [
        f"{BASE_CORFO}/sites/cpp/sala-de-prensa/",
        f"{BASE_CORFO}/sites/cpp/nacional/",
    ]
    resultados = _extraer_por_enlaces(
        nombre_fuente="CORFO",
        base_url=BASE_CORFO,
        urls=urls,
        tipo="programa_convocatoria",
    )
    log.info(f"  CORFO: {len(resultados)} registros con sectores estratégicos")
    guardar_raw("corfo", resultados)
    return resultados


# ── SCRAPER 2 — MINISTERIO DE CIENCIA ────────────────────────────────────────

def scrapear_minciencia() -> list[dict]:
    """
    Extrae noticias y convocatorias del Ministerio de Ciencia.
    Nota: los artículos no tienen clases CSS explícitas — se extraen por enlaces
    que apuntan a /noticias/ o /areas/ dentro del dominio.
    """
    log.info("=== Ministerio de Ciencia ===")
    BASE = "https://www.minciencia.gob.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="Ministerio de Ciencia",
        base_url=BASE,
        urls=[
            f"{BASE}/noticias/",
            f"{BASE}/areas/innovacion-y-emprendimiento/concurso-publico-premio-nacional-de-innovacion/",
            f"{BASE}/ines/",
        ],
        tipo="noticia_convocatoria",
    )
    log.info(f"  MinCiencia: {len(resultados)} registros")
    guardar_raw("minciencia", resultados)
    return resultados


# ── SCRAPER 3 — CENIA ────────────────────────────────────────────────────────

def scrapear_cenia() -> list[dict]:
    """Centro Nacional de IA — proyectos, noticias y oportunidades."""
    log.info("=== CENIA ===")
    BASE = "https://cenia.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="CENIA",
        base_url=BASE,
        urls=[f"{BASE}/", f"{BASE}/noticias/", f"{BASE}/investigacion/"],
        tipo="proyecto_noticia",
        sectores_fijos=["ia_tecnologia"],
    )
    log.info(f"  CENIA: {len(resultados)} registros")
    guardar_raw("cenia", resultados)
    return resultados


# ── SCRAPER 4 — TRABAJANDO.COM (desactivado — JS rendering) ──────────────────

def scrapear_trabajando() -> list[dict]:
    """Trabajando.com — desactivado: el sitio no responde a requests sin JS."""
    log.info("=== Trabajando.com (desactivado — JS rendering) ===")
    log.warning("  Trabajando.com: fuente desactivada temporalmente.")
    guardar_raw("trabajando", [])
    return []


# ── SCRAPER 5 — BUMERAN CHILE (desactivado — JS rendering) ───────────────────

def scrapear_bumeran() -> list[dict]:
    """Bumeran.cl — desactivado: usa API interna vía fetch, no scrapeable sin JS."""
    log.info("=== Bumeran Chile (desactivado — JS rendering) ===")
    log.warning("  Bumeran: fuente desactivada temporalmente.")
    guardar_raw("bumeran", [])
    return []


# ── FUENTE 6 — MERCADO PÚBLICO API ───────────────────────────────────────────

def scrapear_mercadopublico() -> list[dict]:
    """
    Obtiene TODAS las licitaciones activas de Mercado Público
    y filtra localmente las relevantes para sectores estratégicos.
    Una sola llamada a la API — más estable y sin problemas de encoding.
    """
    log.info("=== Mercado Público ===")

    if MERCADOPUBLICO_API_KEY == "PEGA_AQUI_TU_API_KEY_DE_MERCADOPUBLICO":
        log.error("  API key no configurada (Mercado_Publico_API_KEY).")
        return []

    url = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
    params = {
        "ticket": MERCADOPUBLICO_API_KEY,
        "estado": "activas",
    }

    try:
        log.info("  Descargando licitaciones activas (puede tardar ~60s)...")
        r = requests.get(url, params=params, timeout=90)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.error(f"  MP error: {e}")
        return []

    licitaciones = data.get("Listado", [])
    log.info(f"  Total licitaciones activas: {len(licitaciones)}")

    todos  = []
    vistos = set()

    for lit in licitaciones:
        nombre = lit.get("Nombre", "")
        desc   = lit.get("Descripcion", "")
        codigo = lit.get("CodigoExterno", "")

        # Filtrar por sectores estratégicos
        sectores = detectar_sectores(f"{nombre} {desc}")
        if not sectores:
            continue
        if codigo in vistos:
            continue
        vistos.add(codigo)

        monto = lit.get("MontoEstimado")
        todos.append({
            "fuente":         "MercadoPublico",
            "tipo":           "licitacion",
            "titulo":         nombre,
            "organizacion":   lit.get("NombreOrganismo", ""),
            "region":         lit.get("Region", ""),
            "fecha_cierre":   lit.get("FechaCierre", ""),
            "monto_estimado": float(monto) if monto else None,
            "codigo":         codigo,
            "descripcion":    desc,
            "sectores":       sectores,
            "fecha_scraping": datetime.now().isoformat(),
        })

    log.info(f"  Mercado Público: {len(todos)} licitaciones únicas")
    guardar_raw("mercadopublico", todos)
    return todos


# ── Helper genérico — extracción de enlaces por sector ───────────────────────

def _extraer_por_enlaces(
    nombre_fuente: str,
    base_url: str,
    urls: list[str],
    tipo: str,
    sectores_fijos: list[str] | None = None,
    verify_ssl: bool = True,
) -> list[dict]:
    """
    Patrón común: descarga una página, extrae todos los <a href> del
    mismo dominio, filtra por sectores estratégicos en el título.
    Si sectores_fijos se especifica, todos los registros usan esos sectores.
    """
    resultados = []
    vistos: set[str] = set()
    dominio = base_url.rstrip("/").split("//")[-1].split("/")[0]

    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20, verify=verify_ssl)
            r.raise_for_status()
        except Exception as e:
            log.warning(f"  {nombre_fuente}: error en {url}: {e}")
            continue

        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("/"):
                href = base_url.rstrip("/") + href
            if not href.startswith("http") or dominio not in href:
                continue
            if href in vistos:
                continue
            vistos.add(href)

            titulo = a.get_text(strip=True)
            if not titulo or len(titulo) < 15:
                continue

            sectores = sectores_fijos if sectores_fijos else detectar_sectores(titulo)
            if not sectores:
                continue

            parent = a.find_parent(["article", "div", "li", "section"])
            desc = ""
            if parent:
                p = parent.find("p")
                if p:
                    desc = p.get_text(strip=True)[:300]

            resultados.append({
                "fuente":        nombre_fuente,
                "tipo":          tipo,
                "titulo":        titulo,
                "descripcion":   desc,
                "url":           href,
                "sectores":      sectores,
                "fecha_scraping": datetime.now().isoformat(),
            })

    return resultados


# ── FUENTE 7 — PROGRAMA DPS ───────────────────────────────────────────────────

def scrapear_dps() -> list[dict]:
    """
    Programa interministerial (Economía, Hacienda, Minería, Energía, Ciencia).
    Financia formación de talento en litio, H2 verde, IA y manufactura avanzada.
    URL: programadps.gob.cl
    """
    log.info("=== Programa DPS ===")
    BASE = "https://programadps.gob.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="Programa DPS",
        base_url=BASE,
        urls=[f"{BASE}/", f"{BASE}/gobernanza/"],
        sectores_fijos=["litio", "energias_renovables", "ia_tecnologia"],
        tipo="programa_estado",
    )
    log.info(f"  DPS: {len(resultados)} registros")
    guardar_raw("dps", resultados)
    return resultados


# ── FUENTE 8 — AGCID ─────────────────────────────────────────────────────────

def scrapear_agcid() -> list[dict]:
    """
    Convocatorias de becas y cooperación bilateral con Asia-Pacífico.
    URL: agcid.gob.cl
    """
    log.info("=== AGCID ===")
    BASE = "https://www.agcid.gob.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="AGCID",
        base_url=BASE,
        urls=[f"{BASE}/", f"{BASE}/noticias/", f"{BASE}/becas/"],
        tipo="beca_cooperacion",
    )
    # Agregar sector asia_pacifico a todos los registros sin sector detectado
    for r in resultados:
        if "asia_pacifico" not in r["sectores"]:
            r["sectores"].append("asia_pacifico")
    log.info(f"  AGCID: {len(resultados)} registros")
    guardar_raw("agcid", resultados)
    return resultados


# ── FUENTE 9 — MINISTERIO DEL TRABAJO ────────────────────────────────────────

def scrapear_mintrab() -> list[dict]:
    """Ministerio del Trabajo — noticias y programas en sectores estratégicos."""
    log.info("=== Ministerio del Trabajo ===")
    BASE = "https://www.mintrab.gob.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="Ministerio del Trabajo",
        base_url=BASE,
        urls=[f"{BASE}/noticias/", f"{BASE}/"],
        tipo="noticia_programa",
    )
    log.info(f"  Mintrab: {len(resultados)} registros")
    guardar_raw("mintrab", resultados)
    return resultados


# ── FUENTE 10 — CANCILLERÍA DECYTI ───────────────────────────────────────────

def scrapear_decyti() -> list[dict]:
    """Cancillería DECYTI — diplomacia científica y cooperación internacional."""
    log.info("=== Cancillería DECYTI ===")
    BASE = "https://minrel.gob.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="Cancillería DECYTI",
        base_url=BASE,
        urls=[f"{BASE}/decyti", f"{BASE}/sala-de-prensa/"],
        tipo="cooperacion_internacional",
    )
    # DECYTI es relevante para asia_pacifico y todos los sectores de cooperación
    for r in resultados:
        if "asia_pacifico" not in r["sectores"]:
            r["sectores"].append("asia_pacifico")
    log.info(f"  DECYTI: {len(resultados)} registros")
    guardar_raw("decyti", resultados)
    return resultados


# ── FUENTE 11 — BCN OBSERVATORIO ASIA-PACÍFICO ───────────────────────────────

def scrapear_bcn_asia() -> list[dict]:
    """BCN Observatorio Asia-Pacífico — análisis Chile-Asia."""
    log.info("=== BCN Observatorio Asia-Pacífico ===")
    BASE = "https://www.bcn.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="BCN Observatorio Asia-Pacífico",
        base_url=BASE,
        urls=[
            f"{BASE}/observatorio/asiapacifico/noticias",
        ],
        tipo="analisis_cooperacion",
        sectores_fijos=["asia_pacifico"],   # todo el contenido es Asia-Pacífico
    )
    log.info(f"  BCN Asia-Pacífico: {len(resultados)} registros")
    guardar_raw("bcn_asia", resultados)
    return resultados


# ── FUENTE 12 — GETONBOARD.CL ────────────────────────────────────────────────

def scrapear_getonboard() -> list[dict]:
    """
    Getonboard — desactivado: URL /empleos?q= retorna 404 (estructura de URL cambiada).
    Se mantiene la función para no romper FUENTES_DISPONIBLES.
    """
    log.info("=== Getonboard (desactivado — URL estructura cambiada) ===")
    log.warning("  Getonboard: fuente desactivada temporalmente.")
    guardar_raw("getonboard", [])
    return []


# ── FUENTE 13 — INDEED CHILE ─────────────────────────────────────────────────

def scrapear_indeed() -> list[dict]:
    """
    Indeed — desactivado: bloquea con 403 Forbidden (anti-bot permanente).
    Se mantiene la función para no romper FUENTES_DISPONIBLES.
    """
    log.info("=== Indeed Chile (desactivado — 403 anti-bot) ===")
    log.warning("  Indeed: fuente desactivada (bloqueo anti-bot permanente).")
    guardar_raw("indeed", [])
    return []


# ── FUENTE 14 — OBSERVATORIO LABORAL ─────────────────────────────────────────

def scrapear_observatorio_laboral() -> list[dict]:
    """
    Observatorio Laboral — dominio original caído (DNS no resuelve).
    Fuente desactivada hasta que el MINTRAB publique una URL estable.
    Se mantiene la función para no romper FUENTES_DISPONIBLES.
    """
    log.info("=== Observatorio Laboral (desactivado — dominio caído) ===")
    log.warning("  Observatorio Laboral: fuente desactivada temporalmente.")
    guardar_raw("observatorio_laboral", [])
    return []


# ── FUENTE 15 — INE ───────────────────────────────────────────────────────────

def scrapear_ine() -> list[dict]:
    """INE — estadísticas de empleo por sector económico."""
    log.info("=== INE ===")
    BASE = "https://www.ine.gob.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="INE",
        base_url=BASE,
        urls=[
            f"{BASE}/estadisticas/economia/mineria/",
            f"{BASE}/estadisticas/economia/energia/",
            f"{BASE}/estadisticas/sociales/mercado-laboral/empleo/",
        ],
        tipo="estadistica_empleo",
    )
    log.info(f"  INE: {len(resultados)} registros")
    guardar_raw("ine", resultados)
    return resultados


# ── FUENTE 16 — CSV MINISTERIO DE EDUCACIÓN → detección de brechas ───────────

# Mapeo: palabras clave en nomb_carrera / area_carrera_generica → sector
CARRERA_SECTOR = {
    "litio":                    "litio",
    "minería":                  "litio",
    "geología":                 "litio",
    "hidrogeología":            "litio",
    "metalurgia":               "litio",
    "energía":                  "energias_renovables",
    "eléctric":                 "energias_renovables",
    "electrónic":               "energias_renovables",
    "renovable":                "energias_renovables",
    "hidrógeno":                "energias_renovables",
    "geotermia":                "energias_renovables",
    "electromovilidad":         "energias_renovables",
    "inteligencia artificial":  "ia_tecnologia",
    "machine learning":         "ia_tecnologia",
    "ciencia de datos":         "ia_tecnologia",
    "data science":             "ia_tecnologia",
    "ingeniería informática":   "ia_tecnologia",
    "computación":              "ia_tecnologia",
    "ingeniería en informática":"ia_tecnologia",
    "astronomía":               "astronomia",
    "astrofísica":              "astronomia",
    "astrofísica":              "astronomia",
    "física":                   "astronomia",
    "oceanografía":             "oceanografia",
    "biología marina":          "oceanografia",
    "acuicultura":              "oceanografia",
    "recursos del mar":         "oceanografia",
    "relaciones internacionales": "asia_pacifico",
    "comercio exterior":          "asia_pacifico",
    "negocios internacionales":   "asia_pacifico",
}


def analizar_csv_mineduc() -> dict:
    """
    Lee el CSV de matrícula del Mineduc (una fila = un alumno matriculado).

    Columnas clave usadas:
      nomb_carrera, area_carrera_generica, area_conocimiento,
      region_sede, nivel_global, nomb_inst, tipo_inst_1

    Retorna:
      matriculados_por_sector  — conteo total por sector estratégico
      carreras_por_sector      — top carreras por sector con su matrícula
      resumen_regional         — matrícula por región en sectores estratégicos
      fecha_analisis
    """
    log.info("=== Ministerio de Educación (CSV) ===")

    try:
        import pandas as pd
    except ImportError:
        log.error("Instala pandas: pip install pandas")
        return {}

    # Prioridad: usar carreras_estrategicas.json (archivo liviano ya en el repo)
    resumen_path = RAW_DIR / "carreras_estrategicas.json"
    if resumen_path.exists():
        import json as _json
        with open(resumen_path, encoding="utf-8") as f:
            datos = _json.load(f)
        log.info(f"  Usando carreras_estrategicas.json ({sum(datos.get('matriculados_por_sector', {}).values()):,} matriculados)")
        return datos

    if not MINEDUC_CSV_PATH.exists():
        log.error(f"CSV no encontrado: {MINEDUC_CSV_PATH}")
        return {}

    log.info(f"  Leyendo {MINEDUC_CSV_PATH.name} ...")
    df = pd.read_csv(
        MINEDUC_CSV_PATH,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        low_memory=False,
    )
    log.info(f"  {len(df):,} filas cargadas | columnas: {len(df.columns)}")

    df.columns = [c.strip().lower() for c in df.columns]
    df["_texto"] = (
        df.get("nomb_carrera", pd.Series(dtype=str)).fillna("").str.lower()
        + " "
        + df.get("area_carrera_generica", pd.Series(dtype=str)).fillna("").str.lower()
    )

    def asignar_sector(texto: str) -> str | None:
        for kw, sector in CARRERA_SECTOR.items():
            if kw.lower() in texto:
                return sector
        return None

    df["sector"] = df["_texto"].apply(asignar_sector)
    df_sector = df[df["sector"].notna()].copy()

    log.info(f"  Filas en sectores estratégicos: {len(df_sector):,}")

    matriculados_por_sector = df_sector.groupby("sector").size().to_dict()
    for s in SECTORES:
        matriculados_por_sector.setdefault(s, 0)

    carreras_por_sector: dict[str, list] = {}
    for sector, grupo in df_sector.groupby("sector"):
        top = (
            grupo.groupby("nomb_carrera")
            .size()
            .reset_index(name="matriculados")
            .sort_values("matriculados", ascending=False)
            .head(10)
        )
        carreras_por_sector[sector] = top.to_dict(orient="records")

    if "region_sede" in df_sector.columns:
        resumen_regional = (
            df_sector.groupby(["region_sede", "sector"])
            .size()
            .reset_index(name="matriculados")
            .sort_values("matriculados", ascending=False)
            .to_dict(orient="records")
        )
    else:
        resumen_regional = []

    for sector, total in sorted(matriculados_por_sector.items(), key=lambda x: -x[1]):
        log.info(f"  {sector:<28} {total:>7,} alumnos")

    resultado = {
        "matriculados_por_sector": matriculados_por_sector,
        "carreras_por_sector":     carreras_por_sector,
        "resumen_regional":        resumen_regional,
        "total_en_sectores":       int(len(df_sector)),
        "total_matricula_pais":    int(len(df)),
        "fecha_analisis":          datetime.now().isoformat(),
    }

    path = RAW_DIR / f"mineduc_matriculas_{datetime.now().strftime('%Y%m%d')}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    log.info(f"  Guardado: {path}")
    return resultado


# ── FUENTE 17 — ProChile ─────────────────────────────────────────────────────

def scrapear_prochile() -> list[dict]:
    """ProChile — exportaciones y oportunidades internacionales. sectores_fijos porque el contenido no usa keywords de sector."""
    log.info("=== ProChile ===")
    BASE = "https://www.prochile.gob.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="ProChile",
        base_url=BASE,
        urls=[
            f"{BASE}/",
            f"{BASE}/noticias/",
            f"{BASE}/informacion-para-exportar/",
            f"{BASE}/herramientas/",
        ],
        tipo="oportunidad_exportacion",
        sectores_fijos=["asia_pacifico", "litio", "energias_renovables"],
    )
    log.info(f"  ProChile: {len(resultados)} registros")
    guardar_raw("prochile", resultados)
    return resultados


# ── FUENTE 18 — COCHILCO ─────────────────────────────────────────────────────

def scrapear_cochilco() -> list[dict]:
    """COCHILCO — estadísticas y proyecciones de capital humano en litio/cobre."""
    log.info("=== COCHILCO ===")
    BASE = "https://www.cochilco.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="COCHILCO",
        base_url=BASE,
        urls=[
            f"{BASE}/",
            f"{BASE}/estadisticas-y-estudios/",
            f"{BASE}/sala-de-prensa/",
        ],
        tipo="estadistica_mineria",
        sectores_fijos=["litio"],
    )
    log.info(f"  COCHILCO: {len(resultados)} registros")
    guardar_raw("cochilco", resultados)
    return resultados


# ── FUENTE 19 — ANID ─────────────────────────────────────────────────────────

def scrapear_anid() -> list[dict]:
    """ANID — becas, Fondecyt, Fondef y concursos de I+D."""
    log.info("=== ANID ===")
    BASE = "https://www.anid.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="ANID",
        base_url=BASE,
        urls=[
            f"{BASE}/",
            f"{BASE}/noticias/",
        ],
        tipo="beca_concurso_id",
        sectores_fijos=["ia_tecnologia", "energias_renovables", "litio", "astronomia", "oceanografia"],
    )
    log.info(f"  ANID: {len(resultados)} registros")
    guardar_raw("anid", resultados)
    return resultados


# ── FUENTE 20 — SENCE ────────────────────────────────────────────────────────

def scrapear_sence() -> list[dict]:
    """SENCE — capacitación laboral y becas sectoriales."""
    log.info("=== SENCE ===")
    BASE = "https://www.sence.cl"
    resultados = _extraer_por_enlaces(
        nombre_fuente="SENCE",
        base_url=BASE,
        urls=[
            f"{BASE}/",
            f"{BASE}/noticias/",
            f"{BASE}/programas/",
            f"{BASE}/capacitacion/",
        ],
        tipo="capacitacion_laboral",
        sectores_fijos=["ia_tecnologia", "energias_renovables", "litio"],
    )
    log.info(f"  SENCE: {len(resultados)} registros")
    guardar_raw("sence", resultados)
    return resultados


# ── FUENTE 21 — REMOTEOK (API pública, sin API key) ──────────────────────────

def scrapear_remoteok() -> list[dict]:
    """
    RemoteOK — API pública gratuita, sin clave.
    Filtra empleos remotos tech/IA relevantes para sectores estratégicos.
    """
    log.info("=== RemoteOK ===")
    # Tags relevantes para nuestros sectores
    tags_busqueda = [
        "machine-learning", "data-science", "artificial-intelligence",
        "python", "renewable-energy", "mining", "geology",
    ]
    resultados = []
    vistos: set[str] = set()

    for tag in tags_busqueda:
        url = f"https://remoteok.com/api?tag={tag}"
        try:
            r = requests.get(url, headers={**HEADERS, "Accept": "application/json"}, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning(f"  RemoteOK ({tag}): {e}")
            time.sleep(2)
            continue

        # La API devuelve un array; el primer elemento es metadata, ignorarlo
        jobs = [j for j in data if isinstance(j, dict) and j.get("id")]
        for job in jobs:
            jid = str(job.get("id", ""))
            if jid in vistos:
                continue
            vistos.add(jid)
            titulo = job.get("position", "")
            empresa = job.get("company", "")
            descripcion = job.get("description", "")[:300]
            sectores = detectar_sectores(f"{titulo} {descripcion}")
            if not sectores:
                sectores = ["ia_tecnologia"]   # RemoteOK es mayormente tech
            resultados.append({
                "fuente":         "RemoteOK",
                "tipo":           "oferta_laboral",
                "titulo":         titulo,
                "organizacion":   empresa,
                "region":         "Remoto",
                "descripcion":    descripcion,
                "url":            job.get("url", f"https://remoteok.com/jobs/{jid}"),
                "sectores":       sectores,
                "fecha_cierre":   "",
                "fecha_scraping": datetime.now().isoformat(),
            })
        time.sleep(2)   # cortesía — API pública compartida

    log.info(f"  RemoteOK: {len(resultados)} ofertas remotas")
    guardar_raw("remoteok", resultados)
    return resultados


# ── FUENTE 22 — ADZUNA CHILE (API gratuita, requiere app_id + app_key) ────────

def scrapear_adzuna() -> list[dict]:
    """
    Adzuna — API de empleos gratuita (250 req/día).
    Nota: Chile (cl) no está soportado → usamos 'gb' (UK) con términos en inglés.
    Los resultados miden demanda global de estos perfiles, relevante para brechas.
    Requiere: ADZUNA_APP_ID y ADZUNA_APP_KEY como secrets de GitHub.
    """
    app_id  = os.environ.get("ADZUNA_APP_ID", "")
    app_key = os.environ.get("ADZUNA_APP_KEY", "")
    if not app_id or not app_key:
        log.warning("  Adzuna: ADZUNA_APP_ID / ADZUNA_APP_KEY no configurados — saltando.")
        guardar_raw("adzuna", [])
        return []

    log.info("=== Adzuna (demanda global) ===")
    # Términos en inglés — Adzuna no soporta Chile (cl), usamos gb+us para medir demanda global
    terminos = [
        "lithium mining", "renewable energy", "solar energy",
        "artificial intelligence", "machine learning", "data science",
        "green hydrogen", "oceanography", "astronomy", "energy storage",
    ]
    resultados: list[dict] = []
    vistos: set[str]       = set()

    for pais in ("gb", "us"):
        BASE = f"https://api.adzuna.com/v1/api/jobs/{pais}/search/1"
        for termino in terminos:
            params = {
                "app_id": app_id, "app_key": app_key,
                "what": termino, "results_per_page": 10,
            }
            try:
                r = requests.get(BASE, params=params, timeout=15)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                log.warning(f"  Adzuna [{pais}] ({termino}): {e}")
                time.sleep(2)
                continue

            for job in data.get("results", []):
                jid = str(job.get("id", ""))
                if jid in vistos:
                    continue
                vistos.add(jid)
                titulo      = job.get("title", "")
                descripcion = job.get("description", "")[:300]
                resultados.append({
                    "fuente":         "Adzuna",
                    "tipo":           "oferta_laboral",
                    "titulo":         titulo,
                    "organizacion":   job.get("company", {}).get("display_name", ""),
                    "region":         job.get("location", {}).get("display_name", "Global"),
                    "descripcion":    descripcion,
                    "url":            job.get("redirect_url", ""),
                    "sectores":       detectar_sectores(f"{titulo} {descripcion}") or
                                      detectar_sectores(termino),
                    "fecha_cierre":   "",
                    "fecha_scraping": datetime.now().isoformat(),
                })
            time.sleep(1)

    log.info(f"  Adzuna: {len(resultados)} ofertas")
    guardar_raw("adzuna", resultados)
    return resultados


# ── FUENTE 23 — JOOBLE (API gratuita, requiere api_key) ──────────────────────

def scrapear_jooble() -> list[dict]:
    """
    Jooble — agregador de empleos, API gratuita (uso no comercial).
    Cubre Chile. Requiere: JOOBLE_API_KEY como secret de GitHub.
    """
    api_key = os.environ.get("JOOBLE_API_KEY", "")
    if not api_key:
        log.warning("  Jooble: JOOBLE_API_KEY no configurado — saltando.")
        guardar_raw("jooble", [])
        return []

    log.info("=== Jooble ===")
    # Términos simples sin "Chile" en el keyword — la ubicación va en el campo location
    terminos = [
        "litio", "energía renovable", "inteligencia artificial",
        "data science", "hidrógeno", "oceanografía", "astrónomo",
    ]
    url        = f"https://jooble.org/api/{api_key}"
    resultados: list[dict] = []
    vistos: set[str]       = set()

    for termino in terminos:
        try:
            r = requests.post(
                url,
                json={"keywords": termino, "location": "Chile"},
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            log.info(f"  Jooble ({termino}): {data.get('totalCount', 0)} resultados totales")
        except Exception as e:
            log.warning(f"  Jooble ({termino}): {e}")
            time.sleep(2)
            continue

        for job in data.get("jobs", []):
            jid = str(job.get("id", ""))
            if jid in vistos:
                continue
            vistos.add(jid)
            titulo      = job.get("title", "")
            descripcion = job.get("snippet", "")[:300]
            resultados.append({
                "fuente":         "Jooble",
                "tipo":           "oferta_laboral",
                "titulo":         titulo,
                "organizacion":   job.get("company", ""),
                "region":         job.get("location", "Chile"),
                "descripcion":    descripcion,
                "url":            job.get("link", ""),
                "sectores":       detectar_sectores(f"{titulo} {descripcion}") or
                                  detectar_sectores(termino),
                "fecha_cierre":   job.get("updated", ""),
                "fecha_scraping": datetime.now().isoformat(),
            })
        time.sleep(1.5)

    log.info(f"  Jooble: {len(resultados)} ofertas")
    guardar_raw("jooble", resultados)
    return resultados


# ── Análisis de brechas — cruza oferta educativa vs demanda laboral ──────────

def calcular_brechas(
    datos_educacion: dict,
    ofertas_laborales: list[dict],
) -> list[dict]:
    """
    Cruza matrícula universitaria (oferta de talento) con
    ofertas de trabajo scrapeadas (demanda) para identificar brechas.
    """
    if not datos_educacion or not ofertas_laborales:
        return []

    demanda: dict[str, int] = {s: 0 for s in SECTORES}
    for oferta in ofertas_laborales:
        for sector in oferta.get("sectores", []):
            if sector in demanda:
                demanda[sector] += 1

    matricula = datos_educacion.get("matriculados_por_sector", {})
    brechas   = []

    for sector in SECTORES:
        dem  = demanda.get(sector, 0)
        mat  = matricula.get(sector, 0)
        ratio = round(dem / mat, 4) if mat > 0 else float("inf")
        brechas.append({
            "sector":              sector,
            "demanda_ofertas":     dem,
            "matricula_estimada":  mat,
            "ratio_demanda_oferta": ratio,
            "nivel_brecha": (
                "CRITICA" if ratio > 0.1 or (dem > 5 and mat == 0) else
                "ALTA"    if ratio > 0.05 else
                "MEDIA"   if ratio > 0.01 else
                "BAJA"
            ),
        })

    brechas.sort(key=lambda x: x["demanda_ofertas"], reverse=True)

    path = PROC_DIR / f"brechas_{datetime.now().strftime('%Y%m%d')}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(brechas, f, ensure_ascii=False, indent=2)
    log.info(f"Brechas guardadas: {path}")

    for b in brechas:
        log.info(
            f"  [{b['nivel_brecha']:7s}] {b['sector']:25s} "
            f"demanda={b['demanda_ofertas']:4d}  matricula≈{b['matricula_estimada']:6d}"
        )

    return brechas


# ── Consolidador — une todos los resultados en un solo JSON ──────────────────

def consolidar(todas_las_fuentes: list[list[dict]]) -> list[dict]:
    todos = [item for fuente in todas_las_fuentes for item in fuente]
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    path  = PROC_DIR / f"consolidado_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    por_fuente: dict[str, int] = {}
    for item in todos:
        f = item.get("fuente", "desconocido")
        por_fuente[f] = por_fuente.get(f, 0) + 1

    log.info(f"\nCONSOLIDADO: {len(todos)} registros totales")
    for f, n in sorted(por_fuente.items(), key=lambda x: -x[1]):
        log.info(f"  {f:<30} {n:>5} registros")
    log.info(f"Archivo: {path}")
    return todos


# ── MAIN ─────────────────────────────────────────────────────────────────────

FUENTES_DISPONIBLES = {
    "corfo":                scrapear_corfo,
    "minciencia":           scrapear_minciencia,
    "cenia":                scrapear_cenia,
    "trabajando":           scrapear_trabajando,
    "bumeran":              scrapear_bumeran,
    "mercadopublico":       scrapear_mercadopublico,
    "dps":                  scrapear_dps,
    "agcid":                scrapear_agcid,
    "mintrab":              scrapear_mintrab,
    "decyti":               scrapear_decyti,
    "bcn_asia":             scrapear_bcn_asia,
    "getonboard":           scrapear_getonboard,
    "indeed":               scrapear_indeed,
    "observatorio_laboral": scrapear_observatorio_laboral,
    "ine":                  scrapear_ine,
    "prochile":             scrapear_prochile,
    "cochilco":             scrapear_cochilco,
    "anid":                 scrapear_anid,
    "sence":                scrapear_sence,
    "remoteok":             scrapear_remoteok,
    "adzuna":               scrapear_adzuna,
    "jooble":               scrapear_jooble,
    "educacion":            analizar_csv_mineduc,
}


def main():
    parser = argparse.ArgumentParser(description="Talento País — Pipeline ETAPA 1")
    parser.add_argument(
        "--fuente",
        choices=list(FUENTES_DISPONIBLES.keys()) + ["todas"],
        default="todas",
        help="Fuente específica a scrapear (default: todas)",
    )
    parser.add_argument(
        "--sin-brechas",
        action="store_true",
        help="Omitir cálculo de brechas educación/demanda",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("TALENTO PAÍS — Scraper ETAPA 1")
    log.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    if args.fuente == "todas":
        fuentes_a_correr = [f for f in FUENTES_DISPONIBLES if f != "educacion"]
    else:
        fuentes_a_correr = [args.fuente]

    resultados_fuentes = []
    datos_educacion    = {}

    for nombre in fuentes_a_correr:
        fn = FUENTES_DISPONIBLES[nombre]
        resultado = fn()
        if isinstance(resultado, list):
            resultados_fuentes.append(resultado)
        elif isinstance(resultado, dict):
            datos_educacion = resultado

    # Siempre analizar educación cuando se corre todo
    if args.fuente == "todas" and not datos_educacion:
        datos_educacion = analizar_csv_mineduc()

    # Consolidar
    if resultados_fuentes:
        todos = consolidar(resultados_fuentes)
    else:
        todos = []

    # Brechas
    if not args.sin_brechas and datos_educacion:
        brechas = calcular_brechas(datos_educacion, todos)

    log.info("\nPipeline ETAPA 1 completado.")


if __name__ == "__main__":
    main()
