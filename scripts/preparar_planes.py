"""
preparar_planes.py — Talento País
Extrae texto de los Planes Estratégicos Nacionales (PDF) y genera un resumen
JSON liviano que el pipeline puede usar sin necesidad de los PDFs en el repo.

Planes incluidos:
  1. Estrategia Nacional del Litio (Ministerio de Minería / Economía, 2023)
  2. Estrategia Nacional de Hidrógeno Verde (Ministerio de Energía, 2020)
  3. Política Nacional de Inteligencia Artificial (MinCiencia, 2021)
  4. Plan de Desarrollo Productivo Sostenible — DPS (Ministerio de Energía, 2023)

Modos de operación:
  A) Archivos locales en datos/raw/planes/   → se procesan primero
  B) Descarga automática desde URLs oficiales si el archivo no está presente

Salida:
  datos/raw/planes_estrategicos.json   — resumen estructurado por plan

Uso:
  python scripts/preparar_planes.py
  python scripts/preparar_planes.py --solo litio
  python scripts/preparar_planes.py --descargar   (forzar re-descarga)
"""

import os
import re
import sys
import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

import requests

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ─── Rutas ───────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent / "datos"
RAW_DIR    = BASE_DIR / "raw"
PLANES_DIR = RAW_DIR / "planes"
SALIDA     = RAW_DIR / "planes_estrategicos.json"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PLANES_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
}

# ─── Catálogo de planes ───────────────────────────────────────────────────────
PLANES = [
    {
        "id":          "litio",
        "nombre":      "Estrategia Nacional del Litio",
        "sector":      "litio",
        "sector_label": "Litio y Minería Estratégica",
        "año":         2023,
        "organismo":   "Ministerio de Minería / CORFO",
        "url":         (
            "https://s3.amazonaws.com/gobcl-prod/public_files/Campa%C3%B1as"
            "/Litio-por-Chile/Estrategia-Nacional-del-litio-ES_14062023_2003.pdf"
        ),
        "archivo_local": "estrategia_litio.pdf",
        "paginas_clave": list(range(1, 30)),   # primeras 30 págs tienen los ejes
    },
    {
        "id":          "hidrogeno",
        "nombre":      "Estrategia Nacional de Hidrógeno Verde",
        "sector":      "energias_renovables",
        "sector_label": "Energías Renovables e H₂ Verde",
        "año":         2020,
        "organismo":   "Ministerio de Energía",
        "url":         (
            "https://energia.gob.cl/sites/default/files/"
            "estrategia_nacional_de_hidrogeno_verde_-_chile.pdf"
        ),
        "archivo_local": "estrategia_hidrogeno_verde.pdf",
        "paginas_clave": list(range(1, 25)),
    },
    {
        "id":          "ia",
        "nombre":      "Política Nacional de Inteligencia Artificial",
        "sector":      "ia_tecnologia",
        "sector_label": "IA y Tecnología",
        "año":         2021,
        "organismo":   "Ministerio de Ciencia",
        "url":         (
            "https://minciencia.gob.cl/uploads/filer_public/bc/38/"
            "bc389daf-4514-4306-867c-760ae7686e2c/documento_politica_ia_digital_.pdf"
        ),
        "archivo_local": "politica_ia.pdf",
        "paginas_clave": list(range(1, 25)),
    },
    {
        "id":          "dps",
        "nombre":      "Plan de Desarrollo Productivo Sostenible (DPS)",
        "sector":      "transversal",
        "sector_label": "Transversal — Litio · H₂ · IA · Manufactura",
        "año":         2023,
        "organismo":   "Ministerio de Energía / Comité DPS",
        "url":         (
            "https://energia.gob.cl/sites/default/files/"
            "documentos/1._plan_dps_2023.pdf"
        ),
        "archivo_local": "plan_dps_2023.pdf",
        "paginas_clave": list(range(1, 30)),
    },
]


# ════════════════════════════════════════════════════════════════════════════
# 1. DESCARGA DE PDFs
# ════════════════════════════════════════════════════════════════════════════

def descargar_pdf(plan: dict, forzar: bool = False) -> Path | None:
    """Descarga el PDF si no existe localmente. Retorna la ruta al archivo."""
    destino = PLANES_DIR / plan["archivo_local"]

    if destino.exists() and not forzar:
        log.info(f"  [{plan['id']}] Ya existe: {destino.name} ({destino.stat().st_size / 1024:.0f} KB)")
        return destino

    log.info(f"  [{plan['id']}] Descargando: {plan['url'][:70]}...")
    try:
        r = requests.get(plan["url"], headers=HEADERS, timeout=60, stream=True)
        r.raise_for_status()
        with open(destino, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        size_kb = destino.stat().st_size / 1024
        log.info(f"  [{plan['id']}] Descargado: {destino.name} ({size_kb:.0f} KB)")
        return destino
    except Exception as e:
        log.error(f"  [{plan['id']}] Error al descargar: {e}")
        return None


# ════════════════════════════════════════════════════════════════════════════
# 2. EXTRACCIÓN DE TEXTO
# ════════════════════════════════════════════════════════════════════════════

def extraer_texto_pdf(ruta: Path, paginas_clave: list[int]) -> str:
    """
    Extrae texto de las páginas clave del PDF.
    Intenta pdfplumber primero (mejor calidad), luego pypdf como fallback.
    """
    texto_total = []

    # Intentar con pdfplumber
    try:
        import pdfplumber
        with pdfplumber.open(str(ruta)) as pdf:
            total_pags = len(pdf.pages)
            paginas = [p - 1 for p in paginas_clave if 0 < p <= total_pags]
            for idx in paginas:
                page = pdf.pages[idx]
                texto = page.extract_text()
                if texto:
                    texto_total.append(texto)
        log.info(f"    pdfplumber: {len(paginas)} páginas extraídas")
        return "\n\n".join(texto_total)
    except ImportError:
        pass
    except Exception as e:
        log.warning(f"    pdfplumber error: {e}")

    # Fallback: pypdf
    try:
        import pypdf
        reader = pypdf.PdfReader(str(ruta))
        total_pags = len(reader.pages)
        paginas = [p - 1 for p in paginas_clave if 0 < p <= total_pags]
        for idx in paginas:
            texto = reader.pages[idx].extract_text()
            if texto:
                texto_total.append(texto)
        log.info(f"    pypdf: {len(paginas)} páginas extraídas")
        return "\n\n".join(texto_total)
    except ImportError:
        pass
    except Exception as e:
        log.warning(f"    pypdf error: {e}")

    # Segundo fallback: PyPDF2 (legado)
    try:
        import PyPDF2
        with open(str(ruta), "rb") as f:
            reader = PyPDF2.PdfReader(f)
            total_pags = len(reader.pages)
            paginas = [p - 1 for p in paginas_clave if 0 < p <= total_pags]
            for idx in paginas:
                texto = reader.pages[idx].extract_text()
                if texto:
                    texto_total.append(texto)
        log.info(f"    PyPDF2: {len(paginas)} páginas extraídas")
        return "\n\n".join(texto_total)
    except ImportError:
        log.error("    Sin biblioteca PDF disponible. Instala: pip install pdfplumber")
        return ""
    except Exception as e:
        log.error(f"    Error extrayendo texto: {e}")
        return ""


# ════════════════════════════════════════════════════════════════════════════
# 3. EXTRACCIÓN DE DATOS ESTRUCTURADOS
# ════════════════════════════════════════════════════════════════════════════

# Palabras clave por sector para identificar actores y medidas
ACTORES_INSTITUCIONALES = [
    "CORFO", "ANID", "Ministerio", "Banco Central", "SQM", "Codelco",
    "ENAMI", "Cochilco", "CCHEN", "Universidad", "USACH", "PUC", "UCH",
    "PUCV", "CENIA", "MinCiencia", "MMA", "SERNAGEOMIN", "SERNAPESCA",
    "DOH", "DGA", "DIRECON", "PROCHILE", "InvestChile", "Innova",
    "Fundación Chile", "CEPAL", "PNUD", "BID", "Banco Mundial",
    "Japan", "China", "Corea", "Alemania", "ESO", "ALMA",
    "Comité DPS", "Hacienda", "Economía", "Energía", "Minería",
    "Transporte", "Ciencia", "Cancillería", "DECYTI",
]

MEDIDAS_KW = [
    "meta", "objetivo", "eje", "medida", "acción", "programa",
    "fondo", "concurso", "beca", "formación", "capacitación",
    "inversión", "millones", "MW", "GW", "toneladas", "profesionales",
    "investigadores", "técnicos", "empleos", "puestos",
]


def _limpiar_texto(texto: str) -> str:
    """Normaliza espacios y caracteres en el texto extraído de PDF."""
    texto = re.sub(r'\s+', ' ', texto)
    texto = re.sub(r'[^\x20-\x7EáéíóúñüÁÉÍÓÚÑÜ¡¿°%.,;:()\[\]{}\-/\'"]+', ' ', texto)
    return texto.strip()


def extraer_datos_plan(plan: dict, texto_raw: str) -> dict:
    """
    Extrae datos estructurados del texto del plan:
    - actores institucionales mencionados
    - metas cuantitativas (números + contexto)
    - ejes o pilares del plan
    - extracto para uso en detector_sinergias
    """
    texto = _limpiar_texto(texto_raw)

    # Actores mencionados
    actores_encontrados = sorted({
        actor for actor in ACTORES_INSTITUCIONALES
        if actor.lower() in texto.lower()
    })

    # Metas cuantitativas: oraciones que contienen números + unidades de medida
    oraciones = re.split(r'(?<=[.!?])\s+', texto)
    metas = []
    for oracion in oraciones:
        if any(kw in oracion.lower() for kw in MEDIDAS_KW):
            if re.search(r'\d+', oracion):
                oracion_limpia = oracion.strip()
                if 30 < len(oracion_limpia) < 400:
                    metas.append(oracion_limpia)
    metas = metas[:15]  # máximo 15 metas

    # Extracto limpio para el detector de sinergias (primeros 2500 chars)
    extracto = texto[:2500]

    return {
        "id":           plan["id"],
        "nombre":       plan["nombre"],
        "sector":       plan["sector"],
        "sector_label": plan["sector_label"],
        "año":          plan["año"],
        "organismo":    plan["organismo"],
        "url_oficial":  plan["url"],
        "actores_clave": actores_encontrados,
        "metas_cuantitativas": metas,
        "extracto":     extracto,
        "chars_procesados": len(texto),
        "fecha_proceso": datetime.now().isoformat(),
    }


# ════════════════════════════════════════════════════════════════════════════
# 4. MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Talento País — Preparar resumen de Planes Estratégicos Nacionales"
    )
    parser.add_argument(
        "--solo",
        choices=[p["id"] for p in PLANES],
        help="Procesar solo un plan específico",
    )
    parser.add_argument(
        "--descargar",
        action="store_true",
        help="Forzar re-descarga aunque el archivo ya exista",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("TALENTO PAÍS — Preparar Planes Estratégicos Nacionales")
    log.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    # Verificar si hay biblioteca PDF disponible
    tiene_pdf_lib = False
    for lib in ("pdfplumber", "pypdf", "PyPDF2"):
        try:
            __import__(lib)
            tiene_pdf_lib = True
            log.info(f"  Biblioteca PDF: {lib}")
            break
        except ImportError:
            continue
    if not tiene_pdf_lib:
        log.error(
            "Sin biblioteca PDF. Instala una de:\n"
            "  pip install pdfplumber\n"
            "  pip install pypdf"
        )
        sys.exit(1)

    planes_a_procesar = [p for p in PLANES if not args.solo or p["id"] == args.solo]
    resultados = []

    for plan in planes_a_procesar:
        log.info(f"\n{'─'*50}")
        log.info(f"Plan: {plan['nombre']} ({plan['año']})")

        # 1. Obtener archivo
        ruta = PLANES_DIR / plan["archivo_local"]
        if not ruta.exists() or args.descargar:
            ruta = descargar_pdf(plan, forzar=args.descargar)
            if ruta:
                time.sleep(2)  # cortesía entre descargas

        if not ruta or not ruta.exists():
            log.warning(f"  [{plan['id']}] No disponible — saltando")
            continue

        # 2. Extraer texto
        log.info(f"  Extrayendo texto de: {ruta.name}")
        texto = extraer_texto_pdf(ruta, plan["paginas_clave"])

        if not texto:
            log.warning(f"  [{plan['id']}] Sin texto extraído")
            continue

        log.info(f"  Texto extraído: {len(texto):,} caracteres")

        # 3. Estructurar datos
        datos = extraer_datos_plan(plan, texto)
        resultados.append(datos)

        log.info(f"  Actores identificados ({len(datos['actores_clave'])}): "
                 f"{', '.join(datos['actores_clave'][:6])}...")
        log.info(f"  Metas cuantitativas: {len(datos['metas_cuantitativas'])}")

    # 4. Guardar
    if resultados:
        with open(SALIDA, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        log.info(f"\n{'='*60}")
        log.info(f"Guardado: {SALIDA}")
        log.info(f"Planes procesados: {len(resultados)}")
        for r in resultados:
            log.info(f"  {r['nombre']:<50} {r['chars_procesados']:>8,} chars")
        log.info("\nPróximo paso: git add datos/raw/planes_estrategicos.json && git commit")
    else:
        log.error("Sin resultados — verifica que los PDFs estén disponibles")
        sys.exit(1)


if __name__ == "__main__":
    main()
