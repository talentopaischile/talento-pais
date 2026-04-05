"""
processor.py — Talento País
Pipeline ETAPA 2: Normalización, enriquecimiento y análisis de brechas.

Lee los JSON crudos de datos/raw/ y produce en datos/procesados/:
  - oportunidades.json     → registros limpios listos para el frontend
  - brechas.json           → análisis brecha oferta/demanda de talento
  - resumen.json           → KPIs y estadísticas generales

Uso:
  python processor.py
  python processor.py --run-id 20240101_120000   # procesa un run específico
"""

import re
import json
import hashlib
import logging
import argparse
from datetime import datetime
from pathlib import Path

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("processor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Rutas ──────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent.parent / "datos"
RAW_DIR   = BASE_DIR / "raw"
PROC_DIR  = BASE_DIR / "procesados"
PROC_DIR.mkdir(parents=True, exist_ok=True)

# ─── Sectores estratégicos ───────────────────────────────────────────────────
SECTORES = [
    "litio",
    "energias_renovables",
    "ia_tecnologia",
    "astronomia",
    "oceanografia",
    "asia_pacifico",
]

SECTOR_LABELS = {
    "litio":              "Litio & Minería",
    "energias_renovables":"Energías Renovables",
    "ia_tecnologia":      "IA & Tecnología",
    "astronomia":         "Astronomía",
    "oceanografia":       "Oceanografía",
    "asia_pacifico":      "Asia-Pacífico",
}

# Palabras clave Asia-Pacífico para re-etiquetar
ASIA_KW = [
    "china", "japón", "japan", "corea", "korea", "asia", "apec",
    "pacífico", "pacific", "csc", "mext", "gks", "jica",
    "export", "exportación", "acuerdo comercial",
]

# Regiones de Chile con alta actividad en sectores estratégicos
REGIONES_ESTRATEGICAS = {
    "Antofagasta":    ["litio", "energias_renovables", "astronomia"],
    "Atacama":        ["litio", "energias_renovables", "astronomia"],
    "Coquimbo":       ["astronomia", "energias_renovables"],
    "Metropolitana":  ["ia_tecnologia", "asia_pacifico"],
    "Biobío":         ["energias_renovables", "oceanografia"],
    "Los Lagos":      ["oceanografia"],
    "Magallanes":     ["oceanografia", "energias_renovables"],
    "Tarapacá":       ["litio", "energias_renovables"],
}


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def uid(texto: str) -> str:
    """Hash corto y reproducible para deduplicar registros."""
    return hashlib.md5(texto.encode("utf-8")).hexdigest()[:12]


def limpiar_texto(t: str | None) -> str:
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t).strip()
    t = t.replace("\n", " ").replace("\r", "")
    return t[:500]  # truncar a 500 chars para el frontend


def normalizar_region(region: str | None) -> str:
    if not region:
        return "No especificada"
    region = region.strip().title()
    # Normalizar variaciones comunes
    mapeo = {
        "Region Metropolitana": "Metropolitana",
        "Rm": "Metropolitana",
        "Santiago": "Metropolitana",
        "Xiii": "Metropolitana",
        "Ii": "Antofagasta",
        "Iii": "Atacama",
        "Iv": "Coquimbo",
        "V": "Valparaíso",
        "Vi": "Lib. Gral. B. O'Higgins",
        "Vii": "Maule",
        "Viii": "Biobío",
        "Ix": "La Araucanía",
        "X": "Los Lagos",
        "Xi": "Aysén",
        "Xii": "Magallanes",
        "Xiv": "Los Ríos",
        "Xv": "Arica Y Parinacota",
        "Xvi": "Ñuble",
    }
    return mapeo.get(region, region)


def score_relevancia(record: dict) -> float:
    """
    Puntaje 0.0–1.0 que indica qué tan relevante es el registro
    para los objetivos de Talento País.

    Factores:
      + sectores estratégicos detectados
      + es licitación activa con monto
      + región estratégica
      + mención Asia-Pacífico
    """
    score = 0.0
    sectores = record.get("sectores", [])
    tipo     = record.get("tipo", "")
    region   = record.get("region", "")
    texto    = f"{record.get('titulo','')} {record.get('descripcion','')}".lower()

    # Sectores (hasta 0.5)
    score += min(len(sectores) * 0.15, 0.5)

    # Tipo de registro
    if tipo == "licitacion":
        score += 0.2
        if record.get("monto_estimado") and record["monto_estimado"] > 0:
            score += 0.1
    elif tipo == "oferta_laboral":
        score += 0.15
    elif tipo == "programa_convocatoria":
        score += 0.1

    # Región estratégica
    for region_est, sects_est in REGIONES_ESTRATEGICAS.items():
        if region_est.lower() in region.lower():
            if any(s in sects_est for s in sectores):
                score += 0.1
            break

    # Mención Asia-Pacífico
    if any(kw in texto for kw in ASIA_KW):
        score += 0.1

    return round(min(score, 1.0), 3)


def es_asia_pacifico(record: dict) -> bool:
    texto = f"{record.get('titulo','')} {record.get('descripcion','')}".lower()
    return (
        "asia_pacifico" in record.get("sectores", [])
        or any(kw in texto for kw in ASIA_KW)
    )


# ════════════════════════════════════════════════════════════════════════════
# CARGADOR DE JSON CRUDOS
# ════════════════════════════════════════════════════════════════════════════

def cargar_raw(run_id: str | None = None) -> list[dict]:
    """
    Carga todos los JSON de datos/raw/.
    Si se especifica run_id, solo carga archivos que contengan ese timestamp.
    """
    patron = f"*{run_id}*.json" if run_id else "*.json"
    archivos = sorted(
        [f for f in RAW_DIR.glob(patron) if "mineduc" not in f.name],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    if not archivos:
        log.warning(f"No se encontraron archivos JSON en {RAW_DIR}")
        return []

    todos = []
    for archivo in archivos:
        try:
            with open(archivo, encoding="utf-8") as f:
                datos = json.load(f)
            if isinstance(datos, list):
                todos.extend(datos)
                log.info(f"  Cargado: {archivo.name} ({len(datos)} registros)")
        except Exception as e:
            log.warning(f"  No se pudo leer {archivo.name}: {e}")

    log.info(f"  Total registros crudos: {len(todos)}")
    return todos


def cargar_mineduc() -> dict:
    """
    Carga el resumen de carreras estratégicas del Mineduc.
    Prioridad:
      1. carreras_estrategicas.json  (generado por preparar_mineduc.py, va en el repo)
      2. mineduc_matriculas_*.json   (legacy, generado por scraper.py)
    """
    # Opción 1: resumen pre-procesado (compacto, en el repo)
    resumen = RAW_DIR / "carreras_estrategicas.json"
    log.info(f"  Buscando Mineduc en: {resumen} — existe: {resumen.exists()}")
    if resumen.exists():
        with open(resumen, encoding="utf-8") as f:
            datos = json.load(f)
        mat = datos.get("matriculados_por_sector", {})
        log.info(f"  Mineduc: carreras_estrategicas.json — {sum(mat.values()):,} matriculados en sectores")
        return datos

    # Opción 2: archivo legacy del scraper
    archivos = sorted(
        RAW_DIR.glob("mineduc_matriculas_*.json"),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    if archivos:
        with open(archivos[0], encoding="utf-8") as f:
            datos = json.load(f)
        log.info(f"  Mineduc (legacy): {archivos[0].name}")
        return datos

    log.warning("  Sin datos Mineduc — brechas se calcularán solo con demanda laboral.")
    return {}


# ════════════════════════════════════════════════════════════════════════════
# NORMALIZACIÓN
# ════════════════════════════════════════════════════════════════════════════

def normalizar(record: dict) -> dict:
    """
    Transforma un registro crudo (cualquier fuente) al esquema canónico
    que usa el frontend y Google Sheets.
    """
    titulo      = limpiar_texto(record.get("titulo", ""))
    descripcion = limpiar_texto(record.get("descripcion", ""))
    region      = normalizar_region(record.get("region"))
    sectores    = record.get("sectores", [])
    fuente      = record.get("fuente", "Desconocida")
    tipo        = record.get("tipo", "otro")

    # Sector principal: el primero en la lista (prioridad)
    sector_principal = sectores[0] if sectores else "sin_clasificar"

    # Fecha cierre / vigencia
    fecha_cierre = limpiar_texto(record.get("fecha_cierre", ""))

    # Monto (solo licitaciones MP)
    monto = record.get("monto_estimado")
    monto_fmt = None
    if monto:
        try:
            monto_fmt = f"${float(monto):,.0f} CLP"
        except (ValueError, TypeError):
            monto_fmt = str(monto)

    norm = {
        "id":                uid(f"{fuente}{titulo}{region}"),
        "fuente":            fuente,
        "tipo":              tipo,
        "titulo":            titulo,
        "descripcion":       descripcion,
        "organizacion":      limpiar_texto(record.get("organizacion", record.get("empresa", ""))),
        "region":            region,
        "sectores":          sectores,
        "sector_principal":  sector_principal,
        "sector_label":      SECTOR_LABELS.get(sector_principal, sector_principal),
        "url":               record.get("url", ""),
        "fecha_cierre":      fecha_cierre,
        "monto_estimado":    monto,
        "monto_fmt":         monto_fmt,
        "codigo_mp":         record.get("codigo", ""),
        "es_asia_pacifico":  es_asia_pacifico(record),
        "relevancia_score":  0.0,   # se calcula después
        "fecha_scraping":    record.get("fecha_scraping", ""),
        "fecha_procesado":   datetime.now().isoformat(),
    }

    norm["relevancia_score"] = score_relevancia(norm)
    return norm


# ════════════════════════════════════════════════════════════════════════════
# DEDUPLICACIÓN
# ════════════════════════════════════════════════════════════════════════════

def deduplicar(records: list[dict]) -> list[dict]:
    """
    Elimina duplicados por id (hash de fuente+titulo+region).
    En caso de duplicado, conserva el de mayor relevancia_score.
    """
    vistos: dict[str, dict] = {}
    for r in records:
        rid = r["id"]
        if rid not in vistos or r["relevancia_score"] > vistos[rid]["relevancia_score"]:
            vistos[rid] = r

    result = list(vistos.values())
    log.info(f"  Antes: {len(records)} | Después dedup: {len(result)} ({len(records)-len(result)} eliminados)")
    return result


# ════════════════════════════════════════════════════════════════════════════
# ANÁLISIS DE BRECHAS
# ════════════════════════════════════════════════════════════════════════════

def calcular_brechas(
    oportunidades: list[dict],
    mineduc: dict,
) -> list[dict]:
    """
    Cruza:
      - demanda: ofertas laborales + licitaciones por sector
      - oferta:  alumnos matriculados en carreras del sector (Mineduc)

    Clasifica cada sector en: CRÍTICA / ALTA / MEDIA / BAJA
    """
    matricula = mineduc.get("matriculados_por_sector", {})

    # Contar demanda por sector
    demanda: dict[str, int] = {s: 0 for s in SECTORES}
    for op in oportunidades:
        for s in op.get("sectores", []):
            if s in demanda:
                demanda[s] += 1

    brechas = []
    for sector in SECTORES:
        dem = demanda.get(sector, 0)
        mat = matricula.get(sector, 0)

        if mat > 0:
            ratio = round(dem / mat, 4)
        else:
            ratio = float("inf") if dem > 0 else 0.0

        if ratio == float("inf") or (dem > 3 and mat == 0):
            nivel = "CRÍTICA"
        elif ratio > 0.05:
            nivel = "ALTA"
        elif ratio > 0.01:
            nivel = "MEDIA"
        else:
            nivel = "BAJA"

        # Top carreras del sector (desde Mineduc)
        # Soporta estructura nueva (resumen_por_sector[sector]["top_carreras"]) y legacy
        resumen_sector = mineduc.get("resumen_por_sector", {}).get(sector, {})
        top_carreras_raw = resumen_sector.get("top_carreras", mineduc.get("carreras_por_sector", {}).get(sector, []))
        top_carreras = [
            c["nomb_carrera"] if isinstance(c, dict) else str(c)
            for c in top_carreras_raw[:5]
        ]

        brechas.append({
            "sector":               sector,
            "sector_label":         SECTOR_LABELS.get(sector, sector),
            "demanda_oportunidades":dem,
            "matricula_estimada":   mat,
            "ratio":                ratio if ratio != float("inf") else 9999,
            "nivel_brecha":         nivel,
            "top_carreras":         top_carreras,
            "recomendacion":        _recomendacion(nivel, sector),
        })

    brechas.sort(key=lambda x: x["demanda_oportunidades"], reverse=True)
    return brechas


def _recomendacion(nivel: str, sector: str) -> str:
    msgs = {
        "CRÍTICA": f"Urgente: no hay carreras formales en {SECTOR_LABELS.get(sector, sector)}. "
                   f"Se requieren programas de formación acelerada y atracción de talento internacional.",
        "ALTA":    f"Brecha significativa en {SECTOR_LABELS.get(sector, sector)}. "
                   f"Ampliar cupos en carreras existentes y crear diplomados especializados.",
        "MEDIA":   f"Brecha moderada en {SECTOR_LABELS.get(sector, sector)}. "
                   f"Orientar vocacionalmente a estudiantes hacia este sector.",
        "BAJA":    f"Oferta educativa adecuada para {SECTOR_LABELS.get(sector, sector)} en el corto plazo.",
    }
    return msgs.get(nivel, "")


# ════════════════════════════════════════════════════════════════════════════
# RESUMEN / KPIs
# ════════════════════════════════════════════════════════════════════════════

def generar_resumen(
    oportunidades: list[dict],
    brechas: list[dict],
    mineduc: dict,
) -> dict:
    total_mp   = sum(1 for o in oportunidades if o["fuente"] == "MercadoPublico")
    total_jobs = sum(1 for o in oportunidades if o["tipo"] == "oferta_laboral")
    monto_total_mp = sum(
        o["monto_estimado"] for o in oportunidades
        if o.get("monto_estimado") and o["fuente"] == "MercadoPublico"
    )

    por_sector = {s: 0 for s in SECTORES}
    for o in oportunidades:
        for s in o.get("sectores", []):
            if s in por_sector:
                por_sector[s] += 1

    return {
        "fecha_actualizacion":   datetime.now().isoformat(),
        "total_oportunidades":   len(oportunidades),
        "total_licitaciones_mp": total_mp,
        "total_ofertas_laborales": total_jobs,
        "monto_total_mp_clp":    monto_total_mp,
        "monto_total_mp_fmt":    f"${monto_total_mp:,.0f} CLP",
        "oportunidades_asia":    sum(1 for o in oportunidades if o["es_asia_pacifico"]),
        "por_sector":            por_sector,
        "sectores_criticos":     [b["sector_label"] for b in brechas if b["nivel_brecha"] == "CRÍTICA"],
        "sectores_alta_brecha":  [b["sector_label"] for b in brechas if b["nivel_brecha"] == "ALTA"],
        "total_matriculados_sectores": mineduc.get("total_en_sectores", mineduc.get("total_en_sectores_estrategicos", 0)),
        "total_matricula_pais":        mineduc.get("total_matricula_pais", 0),
        "top_regiones": _top_regiones(oportunidades),
        "fuentes": _conteo_fuentes(oportunidades),
    }


def _top_regiones(oportunidades: list[dict]) -> list[dict]:
    conteo: dict[str, int] = {}
    for o in oportunidades:
        r = o.get("region", "No especificada")
        conteo[r] = conteo.get(r, 0) + 1
    return sorted(
        [{"region": k, "total": v} for k, v in conteo.items()],
        key=lambda x: -x["total"],
    )[:10]


def _conteo_fuentes(oportunidades: list[dict]) -> list[dict]:
    conteo: dict[str, int] = {}
    for o in oportunidades:
        f = o.get("fuente", "Desconocida")
        conteo[f] = conteo.get(f, 0) + 1
    return sorted(
        [{"fuente": k, "total": v} for k, v in conteo.items()],
        key=lambda x: -x["total"],
    )


# ════════════════════════════════════════════════════════════════════════════
# GUARDAR PROCESADOS
# ════════════════════════════════════════════════════════════════════════════

def guardar(nombre: str, datos) -> Path:
    path = PROC_DIR / f"{nombre}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
    size = path.stat().st_size / 1024
    log.info(f"  Guardado: {path.name} ({size:.1f} KB)")
    return path


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Talento País — Pipeline ETAPA 2")
    parser.add_argument(
        "--run-id",
        default=None,
        help="Timestamp del run a procesar (ej: 20240101_120000). Default: todos.",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("TALENTO PAÍS — Processor ETAPA 2")
    log.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    # 1. Cargar datos crudos
    log.info("\n[1/5] Cargando datos crudos...")
    raw = cargar_raw(args.run_id)
    if not raw:
        log.error("Sin datos para procesar. Corre primero scraper.py")
        return

    # 2. Normalizar
    log.info("\n[2/5] Normalizando registros...")
    normalizados = [normalizar(r) for r in raw if r.get("titulo")]
    log.info(f"  Normalizados: {len(normalizados)}")

    # 3. Deduplicar
    log.info("\n[3/5] Deduplicando...")
    oportunidades = deduplicar(normalizados)

    # Ordenar por relevancia
    oportunidades.sort(key=lambda x: x["relevancia_score"], reverse=True)

    # 4. Brechas
    log.info("\n[4/5] Calculando brechas educación ↔ demanda laboral...")
    mineduc = cargar_mineduc()
    brechas = calcular_brechas(oportunidades, mineduc)
    for b in brechas:
        log.info(
            f"  [{b['nivel_brecha']:8s}] {b['sector_label']:<25} "
            f"demanda={b['demanda_oportunidades']:4d}  "
            f"matricula≈{b['matricula_estimada']:6,}"
        )

    # 5. Resumen
    log.info("\n[5/5] Generando resumen y KPIs...")
    resumen = generar_resumen(oportunidades, brechas, mineduc)

    # Guardar outputs
    log.info("\nGuardando archivos procesados...")
    guardar("oportunidades", oportunidades)
    guardar("brechas", brechas)
    guardar("resumen", resumen)

    # Imprimir KPIs finales
    log.info("\n" + "=" * 60)
    log.info("RESUMEN EJECUTIVO")
    log.info("=" * 60)
    log.info(f"  Oportunidades totales:    {resumen['total_oportunidades']:>6,}")
    log.info(f"  Licitaciones MP:          {resumen['total_licitaciones_mp']:>6,}")
    log.info(f"  Ofertas laborales:        {resumen['total_ofertas_laborales']:>6,}")
    log.info(f"  Oportunidades Asia:       {resumen['oportunidades_asia']:>6,}")
    log.info(f"  Monto total MP:           {resumen['monto_total_mp_fmt']}")
    if resumen["sectores_criticos"]:
        log.info(f"  Sectores CRÍTICOS:        {', '.join(resumen['sectores_criticos'])}")
    log.info("\nPipeline ETAPA 2 completado.")


if __name__ == "__main__":
    main()
