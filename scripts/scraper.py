"""
scraper.py — Talento País
Pipeline ETAPA 1: Recolección de datos desde fuentes estratégicas.

Fuentes:
  - CORFO
  - Ministerio de Ciencia
  - CENIA
  - Trabajando.com
  - Bumeran Chile
  - Mercado Público API
  - PDF Matrícula Educación Superior (Mineduc) → detección de brechas

Salida:
  - datos/raw/      → JSON crudo por fuente
  - datos/procesados/ → JSON consolidado + análisis de brechas

Uso:
  python scraper.py
  python scraper.py --fuente mercadopublico
  python scraper.py --fuente educacion
"""

import os
import re
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Configuración de logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── CONFIGURACIÓN — pon aquí tu API key de Mercado Público ─────────────────
# También puedes crear un archivo .env con: MERCADOPUBLICO_API_KEY=TU_KEY
MERCADOPUBLICO_API_KEY = os.environ.get(
    "Mercado_Publico_API_KEY",
    "PEGA_AQUI_TU_API_KEY_DE_MERCADOPUBLICO"
)

# Ruta al CSV del Ministerio de Educación (matrícula 2025)
# Directorios de salida — relativos al repo (funciona en GitHub Actions y local)
BASE_DIR   = Path(__file__).parent.parent / "datos"
RAW_DIR    = BASE_DIR / "raw"
PROC_DIR   = BASE_DIR / "procesados"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

# CSV del Ministerio de Educación
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


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════════════════════════════
# SCRAPER 1 — CORFO
# ════════════════════════════════════════════════════════════════════════════

def scrapear_corfo() -> list[dict]:
    """
    Extrae noticias y programas de CORFO relacionados con sectores estratégicos.
    """
    log.info("=== CORFO ===")
    resultados = []

    urls = [
        "https://www.corfo.cl/sites/cpp/homecorfo",
        "https://www.corfo.cl/sites/cpp/convocatoriasempresa",
        "https://www.corfo.cl/sites/cpp/programas",
    ]

    for url in urls:
        r = get(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")

        # Buscar artículos, noticias y tarjetas de programas
        for card in soup.find_all(
            ["article", "div", "li"],
            class_=re.compile(r"(noticia|convocatoria|card|item|programa|resultado)", re.I),
        ):
            titulo_el = card.find(["h1", "h2", "h3", "h4", "a"])
            titulo    = titulo_el.get_text(strip=True) if titulo_el else ""
            if not titulo or len(titulo) < 10:
                continue

            desc_el = card.find("p")
            desc    = desc_el.get_text(strip=True) if desc_el else ""

            link_el = card.find("a", href=True)
            link    = link_el["href"] if link_el else ""
            if link and not link.startswith("http"):
                link = "https://www.corfo.cl" + link

            texto_completo = f"{titulo} {desc}"
            sectores       = detectar_sectores(texto_completo)
            if not sectores:
                continue

            resultados.append({
                "fuente":   "CORFO",
                "tipo":     "programa_convocatoria",
                "titulo":   titulo,
                "descripcion": desc,
                "url":      link,
                "sectores": sectores,
                "fecha_scraping": datetime.now().isoformat(),
            })

    log.info(f"  CORFO: {len(resultados)} registros con sectores estratégicos")
    guardar_raw("corfo", resultados)
    return resultados


# ════════════════════════════════════════════════════════════════════════════
# SCRAPER 2 — MINISTERIO DE CIENCIA
# ════════════════════════════════════════════════════════════════════════════

def scrapear_minciencia() -> list[dict]:
    """
    Extrae noticias y convocatorias del Ministerio de Ciencia.
    """
    log.info("=== Ministerio de Ciencia ===")
    resultados = []

    urls = [
        "https://www.minciencia.gob.cl/noticias/",
        "https://www.minciencia.gob.cl/categoria/convocatorias/",
        "https://www.minciencia.gob.cl/",
    ]

    for url in urls:
        r = get(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")

        for card in soup.find_all(
            ["article", "div"],
            class_=re.compile(r"(noticia|post|card|convocatoria|item)", re.I),
        ):
            titulo_el = card.find(["h2", "h3", "h4", "a"])
            titulo    = titulo_el.get_text(strip=True) if titulo_el else ""
            if not titulo or len(titulo) < 10:
                continue

            desc_el = card.find("p")
            desc    = desc_el.get_text(strip=True) if desc_el else ""

            link_el = card.find("a", href=True)
            link    = link_el["href"] if link_el else ""
            if link and not link.startswith("http"):
                link = "https://www.minciencia.gob.cl" + link

            fecha_el = card.find(class_=re.compile(r"(fecha|date|time)", re.I))
            fecha    = fecha_el.get_text(strip=True) if fecha_el else ""

            texto_completo = f"{titulo} {desc}"
            sectores       = detectar_sectores(texto_completo)
            if not sectores:
                continue

            resultados.append({
                "fuente":        "Ministerio de Ciencia",
                "tipo":          "noticia_convocatoria",
                "titulo":        titulo,
                "descripcion":   desc,
                "url":           link,
                "fecha":         fecha,
                "sectores":      sectores,
                "fecha_scraping": datetime.now().isoformat(),
            })

    log.info(f"  MinCiencia: {len(resultados)} registros")
    guardar_raw("minciencia", resultados)
    return resultados


# ════════════════════════════════════════════════════════════════════════════
# SCRAPER 3 — CENIA (Centro Nacional de IA)
# ════════════════════════════════════════════════════════════════════════════

def scrapear_cenia() -> list[dict]:
    """
    Extrae proyectos, noticias y oportunidades de CENIA.
    """
    log.info("=== CENIA ===")
    resultados = []

    urls = [
        "https://cenia.cl/",
        "https://cenia.cl/noticias/",
        "https://cenia.cl/investigacion/",
    ]

    for url in urls:
        r = get(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")

        for card in soup.find_all(
            ["article", "div", "section"],
            class_=re.compile(r"(post|news|card|proyecto|research|item)", re.I),
        ):
            titulo_el = card.find(["h2", "h3", "h4"])
            titulo    = titulo_el.get_text(strip=True) if titulo_el else ""
            if not titulo or len(titulo) < 8:
                continue

            desc_el = card.find("p")
            desc    = desc_el.get_text(strip=True) if desc_el else ""

            link_el = card.find("a", href=True)
            link    = link_el["href"] if link_el else ""
            if link and not link.startswith("http"):
                link = "https://cenia.cl" + link

            resultados.append({
                "fuente":      "CENIA",
                "tipo":        "proyecto_noticia",
                "titulo":      titulo,
                "descripcion": desc,
                "url":         link,
                "sectores":    ["ia_tecnologia"],
                "fecha_scraping": datetime.now().isoformat(),
            })

    log.info(f"  CENIA: {len(resultados)} registros")
    guardar_raw("cenia", resultados)
    return resultados


# ════════════════════════════════════════════════════════════════════════════
# SCRAPER 4 — TRABAJANDO.COM
# ════════════════════════════════════════════════════════════════════════════

def scrapear_trabajando() -> list[dict]:
    """
    Busca ofertas laborales en Trabajando.com para sectores estratégicos.
    """
    log.info("=== Trabajando.com ===")
    resultados = []

    terminos_busqueda = [
        "litio", "energía renovable", "inteligencia artificial",
        "data science", "oceanografía", "astronomía", "hidrógeno verde",
    ]

    base_url = "https://www.trabajando.com/trabajo-de-{termino}.html"

    for termino in terminos_busqueda:
        slug = termino.lower().replace(" ", "-").replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
        url  = base_url.format(termino=slug)
        r    = get(url)
        if not r:
            time.sleep(1)
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        for oferta in soup.find_all(
            ["div", "article", "li"],
            class_=re.compile(r"(oferta|job|aviso|vacante|resultado|listing)", re.I),
        ):
            titulo_el  = oferta.find(["h2", "h3", "a"])
            titulo     = titulo_el.get_text(strip=True) if titulo_el else ""
            if not titulo or len(titulo) < 5:
                continue

            empresa_el = oferta.find(class_=re.compile(r"(empresa|company|employer)", re.I))
            empresa    = empresa_el.get_text(strip=True) if empresa_el else "No especificada"

            region_el  = oferta.find(class_=re.compile(r"(region|lugar|ciudad|location)", re.I))
            region     = region_el.get_text(strip=True) if region_el else "Chile"

            link_el    = oferta.find("a", href=True)
            link       = link_el["href"] if link_el else ""
            if link and not link.startswith("http"):
                link = "https://www.trabajando.com" + link

            resultados.append({
                "fuente":         "Trabajando.com",
                "tipo":           "oferta_laboral",
                "titulo":         titulo,
                "empresa":        empresa,
                "region":         region,
                "url":            link,
                "termino_busq":   termino,
                "sectores":       detectar_sectores(f"{titulo} {termino}"),
                "fecha_scraping": datetime.now().isoformat(),
            })

        time.sleep(1.5)  # cortesía al servidor

    log.info(f"  Trabajando.com: {len(resultados)} registros")
    guardar_raw("trabajando", resultados)
    return resultados


# ════════════════════════════════════════════════════════════════════════════
# SCRAPER 5 — BUMERAN CHILE
# ════════════════════════════════════════════════════════════════════════════

def scrapear_bumeran() -> list[dict]:
    """
    Busca ofertas laborales en Bumeran.cl para sectores estratégicos.
    """
    log.info("=== Bumeran Chile ===")
    resultados = []

    terminos_busqueda = [
        "litio", "energía solar", "inteligencia artificial",
        "machine learning", "oceanografía", "astrónomo",
        "hidrógeno", "data scientist",
    ]

    # Bumeran usa una API interna vía fetch — intentamos la URL de búsqueda HTML
    base_url = "https://www.bumeran.cl/empleos-{termino}.html"

    for termino in terminos_busqueda:
        slug = (
            termino.lower()
            .replace(" ", "-")
            .replace("á","a").replace("é","e").replace("í","i")
            .replace("ó","o").replace("ú","u").replace("ó","o")
        )
        url = base_url.format(termino=slug)
        r   = get(url)
        if not r:
            time.sleep(1)
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        for oferta in soup.find_all(
            ["div", "article"],
            class_=re.compile(r"(aviso|job|card|listing|result)", re.I),
        ):
            titulo_el  = oferta.find(["h2", "h3", "h4", "a"])
            titulo     = titulo_el.get_text(strip=True) if titulo_el else ""
            if not titulo or len(titulo) < 5:
                continue

            empresa_el = oferta.find(class_=re.compile(r"(empresa|company)", re.I))
            empresa    = empresa_el.get_text(strip=True) if empresa_el else "No especificada"

            region_el  = oferta.find(class_=re.compile(r"(ubicacion|region|location)", re.I))
            region     = region_el.get_text(strip=True) if region_el else "Chile"

            link_el    = oferta.find("a", href=True)
            link       = link_el["href"] if link_el else ""
            if link and not link.startswith("http"):
                link = "https://www.bumeran.cl" + link

            resultados.append({
                "fuente":         "Bumeran",
                "tipo":           "oferta_laboral",
                "titulo":         titulo,
                "empresa":        empresa,
                "region":         region,
                "url":            link,
                "termino_busq":   termino,
                "sectores":       detectar_sectores(f"{titulo} {termino}"),
                "fecha_scraping": datetime.now().isoformat(),
            })

        time.sleep(1.5)

    log.info(f"  Bumeran: {len(resultados)} registros")
    guardar_raw("bumeran", resultados)
    return resultados


# ════════════════════════════════════════════════════════════════════════════
# FUENTE 6 — MERCADO PÚBLICO API
# ════════════════════════════════════════════════════════════════════════════

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
        log.info("  Descargando licitaciones activas...")
        r = requests.get(url, params=params, timeout=30)
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


# ════════════════════════════════════════════════════════════════════════════
# FUENTE 7 — CSV MINISTERIO DE EDUCACIÓN → detección de brechas
# ════════════════════════════════════════════════════════════════════════════

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

    # Normalizar nombres de columna a minúsculas sin espacios
    df.columns = [c.strip().lower() for c in df.columns]

    # Campo de búsqueda: combina nombre de carrera + área genérica
    df["_texto"] = (
        df.get("nomb_carrera", pd.Series(dtype=str)).fillna("").str.lower()
        + " "
        + df.get("area_carrera_generica", pd.Series(dtype=str)).fillna("").str.lower()
    )

    # Asignar sector a cada fila (primera coincidencia)
    def asignar_sector(texto: str) -> str | None:
        for kw, sector in CARRERA_SECTOR.items():
            if kw.lower() in texto:
                return sector
        return None

    df["sector"] = df["_texto"].apply(asignar_sector)
    df_sector = df[df["sector"].notna()].copy()

    log.info(f"  Filas en sectores estratégicos: {len(df_sector):,}")

    # ── 1. Total matriculados por sector ──────────────────────────────────
    matriculados_por_sector = (
        df_sector.groupby("sector").size().to_dict()
    )
    # Asegurar que todos los sectores aparecen (aunque sea con 0)
    for s in SECTORES:
        matriculados_por_sector.setdefault(s, 0)

    # ── 2. Top carreras por sector ────────────────────────────────────────
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

    # ── 3. Matrícula por región en sectores estratégicos ──────────────────
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

    # ── Log resumen ───────────────────────────────────────────────────────
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


# ════════════════════════════════════════════════════════════════════════════
# ANÁLISIS DE BRECHAS — cruza oferta educativa vs demanda laboral
# ════════════════════════════════════════════════════════════════════════════

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

    # Contar ofertas por sector
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


# ════════════════════════════════════════════════════════════════════════════
# CONSOLIDADOR — une todos los resultados en un solo JSON
# ════════════════════════════════════════════════════════════════════════════

def consolidar(todas_las_fuentes: list[list[dict]]) -> list[dict]:
    todos = [item for fuente in todas_las_fuentes for item in fuente]
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    path  = PROC_DIR / f"consolidado_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    # Estadísticas
    por_fuente: dict[str, int] = {}
    for item in todos:
        f = item.get("fuente", "desconocido")
        por_fuente[f] = por_fuente.get(f, 0) + 1

    log.info(f"\nCONSOLIDADO: {len(todos)} registros totales")
    for f, n in sorted(por_fuente.items(), key=lambda x: -x[1]):
        log.info(f"  {f:<30} {n:>5} registros")
    log.info(f"Archivo: {path}")
    return todos


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

FUENTES_DISPONIBLES = {
    "corfo":           scrapear_corfo,
    "minciencia":      scrapear_minciencia,
    "cenia":           scrapear_cenia,
    "trabajando":      scrapear_trabajando,
    "bumeran":         scrapear_bumeran,
    "mercadopublico":  scrapear_mercadopublico,
    "educacion":       analizar_csv_mineduc,
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
