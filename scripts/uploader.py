"""
uploader.py — Talento País
Pipeline ETAPA 3: Sube los datos procesados a Google Sheets.

Hojas destino en el Spreadsheet 1anICtR-8hRgGKEla7BSTsOtxTgAHtUju6gJpwzC2Tyg:
  - "Brechas"   (gid=0)            ← brechas.json
  - "Sinergias" (gid=1759317245)   ← oportunidades.json

Credenciales:
  Opción A — Variable de entorno GOOGLE_CREDENTIALS con el JSON de la cuenta de servicio
  Opción B — Archivo local google_credentials.json en la carpeta scripts/

Uso:
  python uploader.py
  python uploader.py --solo brechas
  python uploader.py --solo sinergias
"""

import os
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("uploader.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
SPREADSHEET_ID = "1anICtR-8hRgGKEla7BSTsOtxTgAHtUju6gJpwzC2Tyg"

HOJAS = {
    "brechas":   "Brechas",
    "sinergias": "Sinergias",
}

PROC_DIR   = Path(__file__).parent.parent / "datos" / "procesados"
CREDS_FILE = Path(__file__).parent / "google_credentials.json"

# ─── Columnas por hoja ────────────────────────────────────────────────────────

COLUMNAS_BRECHAS = [
    "sector_label",
    "nivel_brecha",
    "demanda_oportunidades",
    "matricula_estimada",
    "ratio",
    "recomendacion",
]

COLUMNAS_SINERGIAS = [
    "titulo",
    "fuente",
    "tipo",
    "sector_label",
    "organizacion",
    "region",
    "fecha_cierre",
    "monto_fmt",
    "es_asia_pacifico",
    "relevancia_score",
    "url",
    "descripcion",
    "fecha_scraping",
]


# ════════════════════════════════════════════════════════════════════════════
# CREDENCIALES
# ════════════════════════════════════════════════════════════════════════════

def obtener_credenciales():
    """
    Carga las credenciales de la cuenta de servicio.
    Prioridad:
      1. Variable de entorno GOOGLE_CREDENTIALS (JSON como string) → para Vercel
      2. Archivo local google_credentials.json → para uso local
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log.error("Instala dependencias: pip install gspread google-auth")
        return None, None

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Opción A: variable de entorno (Vercel)
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        try:
            info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
            log.info("  Credenciales: variable de entorno GOOGLE_CREDENTIALS")
            client = gspread.authorize(creds)
            return client, creds
        except Exception as e:
            log.error(f"  Error cargando GOOGLE_CREDENTIALS desde env: {e}")

    # Opción B: archivo local
    if CREDS_FILE.exists():
        try:
            creds = Credentials.from_service_account_file(str(CREDS_FILE), scopes=scopes)
            log.info(f"  Credenciales: {CREDS_FILE.name}")
            client = gspread.authorize(creds)
            return client, creds
        except Exception as e:
            log.error(f"  Error cargando {CREDS_FILE}: {e}")

    log.error(
        "No se encontraron credenciales.\n"
        "  → Sigue las instrucciones en SETUP_GOOGLE.md para configurarlas."
    )
    return None, None


# ════════════════════════════════════════════════════════════════════════════
# CARGAR PROCESADOS
# ════════════════════════════════════════════════════════════════════════════

def cargar_procesado(nombre: str) -> list | dict | None:
    path = PROC_DIR / f"{nombre}.json"
    if not path.exists():
        log.error(f"No encontrado: {path}. Corre primero processor.py")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ════════════════════════════════════════════════════════════════════════════
# HELPERS DE FORMATO
# ════════════════════════════════════════════════════════════════════════════

def safe(v) -> str:
    """Convierte cualquier valor a string seguro para Sheets."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "Sí" if v else "No"
    if isinstance(v, float):
        if v == 9999:
            return "∞"
        return f"{v:.4f}"
    if isinstance(v, list):
        return ", ".join(str(i) for i in v)
    return str(v)[:500]


def a_filas(registros: list[dict], columnas: list[str]) -> list[list]:
    """Convierte lista de dicts al formato de filas que espera gspread."""
    filas = [columnas]  # encabezado
    for r in registros:
        fila = [safe(r.get(col, "")) for col in columnas]
        filas.append(fila)
    return filas


# ════════════════════════════════════════════════════════════════════════════
# SUBIR HOJA
# ════════════════════════════════════════════════════════════════════════════

def subir_hoja(client, nombre_hoja: str, filas: list[list]) -> bool:
    """
    Sobreescribe la hoja indicada con las filas dadas.
    Agrega una fila de metadatos al final con la fecha de actualización.
    """
    try:
        sh    = client.open_by_key(SPREADSHEET_ID)
        hoja  = sh.worksheet(nombre_hoja)

        # Limpiar contenido actual
        hoja.clear()

        # Escribir datos
        hoja.update(filas, value_input_option="USER_ENTERED")

        # Formato encabezado: negrita
        hoja.format("1:1", {
            "textFormat":      {"bold": True},
            "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.6},
        })

        # Fila de metadatos al final
        meta_fila = [f"Actualizado: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Fuente: Talento País Pipeline"]
        hoja.append_row(meta_fila)

        log.info(f"  ✓ {nombre_hoja}: {len(filas)-1} registros subidos")
        return True

    except Exception as e:
        log.error(f"  Error subiendo {nombre_hoja}: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Talento País — Pipeline ETAPA 3")
    parser.add_argument(
        "--solo",
        choices=["brechas", "sinergias"],
        default=None,
        help="Subir solo una hoja específica",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("TALENTO PAÍS — Uploader ETAPA 3")
    log.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    # Credenciales
    log.info("\n[1/3] Conectando con Google Sheets...")
    client, _ = obtener_credenciales()
    if not client:
        return

    hojas_a_subir = [args.solo] if args.solo else ["brechas", "sinergias"]

    # ── Hoja Brechas ─────────────────────────────────────────────────────────
    if "brechas" in hojas_a_subir:
        log.info("\n[2/3] Subiendo Brechas...")
        datos = cargar_procesado("brechas")
        if datos:
            filas = a_filas(datos, COLUMNAS_BRECHAS)
            subir_hoja(client, HOJAS["brechas"], filas)

    # ── Hoja Sinergias ────────────────────────────────────────────────────────
    if "sinergias" in hojas_a_subir:
        log.info("\n[3/3] Subiendo Sinergias...")
        datos = cargar_procesado("oportunidades")
        if datos:
            # Filtrar solo las más relevantes (score > 0.3) para no sobrecargar Sheets
            relevantes = [o for o in datos if o.get("relevancia_score", 0) >= 0.3]
            log.info(f"  Filtrando: {len(datos)} → {len(relevantes)} oportunidades (score ≥ 0.3)")
            filas = a_filas(relevantes, COLUMNAS_SINERGIAS)
            subir_hoja(client, HOJAS["sinergias"], filas)

    log.info("\nPipeline ETAPA 3 completado.")
    log.info(f"Ver resultado: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")


if __name__ == "__main__":
    main()
