"""
preparar_mallas.py — Talento País
Evalúa la relevancia de cada (carrera, tipo_institución) para los sectores
estratégicos y construye un índice de programas con granularidad institucional.

Eficiencia: 1 llamada Groq por sector = 6 llamadas totales / año (~4 000 tokens).
El top-30 por matrícula cubre ~75 % de los estudiantes del sector.

Input:  datos/raw/carreras_estrategicas.json  (campo "detalle", 1 558 registros)
Output: datos/raw/carreras_estrategicas.json  (actualizado in-place)
        Agrega:
          "programas_relevantes_por_sector"  — lista (inst, sede, carrera, relevancia)
          "matriculados_ponderados_por_sector" — int ponderado por relevancia
          "fecha_evaluacion_mallas"

Pesos: alta = 100 %  |  media = 50 %  |  baja = 0 %

Uso:
  python scripts/preparar_mallas.py
  python scripts/preparar_mallas.py --sector litio
  python scripts/preparar_mallas.py --force      # reevalúa aunque ya exista
"""

import os
import json
import time
import logging
import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("preparar_mallas.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"
ENTRADA      = Path(__file__).parent.parent / "datos" / "raw" / "carreras_estrategicas.json"

TOP_N_PARES  = 30    # pares únicos (carrera, tipo_inst) por sector → 1 llamada Groq
PESOS        = {"alta": 1.0, "media": 0.5, "baja": 0.0}

TIPO_ABREV   = {
    "Universidades":                  "Univ",
    "Institutos Profesionales":       "IP",
    "Centros de Formación Técnica":   "CFT",
}

# ─── Descripción de cada sector para el prompt ───────────────────────────────
SECTOR_DESC = {
    "litio": (
        "Litio y Minería: extracción/procesamiento de litio (hidrometalurgia, "
        "electroquímica, salmueras), exploración geológica, ingeniería de minas, "
        "metalurgia extractiva, química industrial minera."
    ),
    "energias_renovables": (
        "Energías Renovables: diseño e instalación de sistemas fotovoltaicos, eólicos "
        "y de almacenamiento; hidrógeno verde; ingeniería eléctrica de potencia; "
        "eficiencia energética industrial."
    ),
    "ia_tecnologia": (
        "IA y Tecnología: machine learning, deep learning, ciencia de datos, desarrollo "
        "de software, sistemas inteligentes, ciberseguridad, computación en la nube, "
        "automatización con IA."
    ),
    "astronomia": (
        "Astronomía: astrofísica observacional e instrumental, física teórica, "
        "procesamiento de imágenes astronómicas, manejo de telescopios, astroquímica."
    ),
    "oceanografia": (
        "Oceanografía y Ciencias del Mar: oceanografía física/química/biológica, "
        "biología marina, acuicultura, pesquerías sostenibles, cambio climático oceánico."
    ),
    "asia_pacifico": (
        "Asia-Pacífico: comercio exterior con Asia (China, Japón, Corea), idiomas "
        "asiáticos, relaciones internacionales, negociación intercultural, "
        "logística de exportaciones, diplomacia económica."
    ),
}

# ─── Prompt (tokens mínimos) ──────────────────────────────────────────────────
PROMPT = """\
Eres experto en educación superior chilena. Evalúa qué tan relevantes son estos \
programas para el sector indicado, considerando el perfil REAL del egresado.

SECTOR: {desc}

CRITERIO:
- "alta": egresados trabajan DIRECTAMENTE en este sector
- "media": aportan habilidades aplicables pero la carrera no es específica
- "baja": poca o ninguna relación con el sector

PROGRAMAS (formato: tipo | carrera):
{lista}

Responde SOLO con JSON array, sin markdown:
[{{"c":"NOMBRE_EXACTO_CARRERA","t":"tipo","r":"alta|media|baja","x":"razón ≤8 palabras"}}]
"""


# ════════════════════════════════════════════════════════════════════════════
# GROQ
# ════════════════════════════════════════════════════════════════════════════

def llamar_groq(sector: str, pares: list[tuple[str, str, int]]) -> list[dict]:
    """
    pares: [(nomb_carrera, tipo_inst_abrev, matriculados_total), ...]
    Devuelve: [{"c": carrera, "t": tipo, "r": relevancia, "x": razon}, ...]
    """
    if not GROQ_API_KEY:
        log.warning("  GROQ_API_KEY no configurada — skip")
        return []

    lista = "\n".join(
        f"- {t} | {c} ({mat:,} mat.)"
        for c, t, mat in pares
    )
    prompt = PROMPT.format(desc=SECTOR_DESC.get(sector, sector), lista=lista)

    payload = {
        "model":    GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens":  900,
    }
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }

    for intento in range(2):
        try:
            r = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
            if r.status_code == 429:
                espera = 30 if intento == 0 else 60
                log.warning(f"  Rate limit Groq — esperando {espera}s...")
                time.sleep(espera)
                continue
            r.raise_for_status()

            texto = r.json()["choices"][0]["message"]["content"].strip()
            for parte in texto.split("```"):
                parte = parte.strip().lstrip("json").strip()
                if parte.startswith("["):
                    texto = parte
                    break

            resultado = json.loads(texto)
            log.info(f"  Groq → {len(resultado)} evaluaciones")
            return resultado

        except json.JSONDecodeError as e:
            log.warning(f"  Groq respuesta no parseable: {e}")
            return []
        except Exception as e:
            log.error(f"  Groq error (intento {intento+1}): {e}")
            if intento == 0:
                time.sleep(10)

    return []


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Evalúa relevancia curricular por institución")
    parser.add_argument("--sector", default=None)
    parser.add_argument("--force",  action="store_true")
    args = parser.parse_args()

    if not ENTRADA.exists():
        log.error(f"No encontrado: {ENTRADA}")
        return

    with open(ENTRADA, encoding="utf-8") as f:
        datos = json.load(f)

    detalle  = datos.get("detalle", [])
    if not detalle:
        log.error("El campo 'detalle' está vacío — regenera carreras_estrategicas.json")
        return

    sectores = [args.sector] if args.sector else list(SECTOR_DESC.keys())
    programas_relevantes   = datos.get("programas_relevantes_por_sector", {})
    matriculados_ponderados = datos.get("matriculados_ponderados_por_sector", {})

    log.info("=" * 60)
    log.info("TALENTO PAÍS — preparar_mallas.py")
    log.info(f"Registros detalle: {len(detalle):,}  |  Sectores: {sectores}")
    log.info(f"Groq: {'OK' if GROQ_API_KEY else 'NO CONFIGURADO'}")
    log.info("=" * 60)

    for sector in sectores:
        # ── Skip si ya evaluado ──────────────────────────────────────────────
        if not args.force and programas_relevantes.get(sector):
            n   = len(programas_relevantes[sector])
            mat = matriculados_ponderados.get(sector, 0)
            log.info(f"\n  {sector}: ya evaluado ({n} programas, {mat:,} mat. pond.) — skip")
            continue

        # ── Filtrar registros del sector ─────────────────────────────────────
        registros_sector = [r for r in detalle if sector in r.get("sectores", [])]
        if not registros_sector:
            log.warning(f"  {sector}: sin registros en detalle — skip")
            continue

        log.info(f"\n=== {sector.upper()} ({len(registros_sector)} registros) ===")

        # ── Top-N pares únicos (carrera, tipo_inst) por matrícula total ───────
        sumas: dict[tuple, int] = defaultdict(int)
        for r in registros_sector:
            tipo = TIPO_ABREV.get(r.get("tipo_inst_1", ""), r.get("tipo_inst_1", "")[:4])
            sumas[(r["nomb_carrera"], tipo)] += r["matriculados"]

        top_pares = sorted(sumas.items(), key=lambda x: -x[1])[:TOP_N_PARES]
        mat_top   = sum(v for _, v in top_pares)
        mat_total = sum(r["matriculados"] for r in registros_sector)
        log.info(
            f"  Top-{TOP_N_PARES} pares ({mat_top:,}/{mat_total:,} mat. = "
            f"{round(mat_top/mat_total*100) if mat_total else 0}%)"
        )

        pares_groq = [(c, t, v) for (c, t), v in top_pares]

        # ── Llamada Groq ─────────────────────────────────────────────────────
        evaluaciones = llamar_groq(sector, pares_groq)

        # Mapa (carrera_upper, tipo) → evaluación
        eval_map: dict[tuple, dict] = {}
        for ev in evaluaciones:
            c = (ev.get("c") or ev.get("carrera") or "").upper().strip()
            t = (ev.get("t") or ev.get("tipo") or "").strip()
            if c:
                eval_map[(c, t)] = {
                    "relevancia": ev.get("r") or ev.get("relevancia") or "baja",
                    "razon":      ev.get("x") or ev.get("razon")       or "",
                }

        # ── Aplicar relevancia a TODOS los registros del sector ───────────────
        programas: list[dict] = []
        total_pond = 0

        for r in registros_sector:
            nombre = r["nomb_carrera"].upper().strip()
            tipo   = TIPO_ABREV.get(r.get("tipo_inst_1", ""), r.get("tipo_inst_1", "")[:4])

            ev = eval_map.get((nombre, tipo)) or eval_map.get((nombre, ""))
            # Fallback: coincidencia parcial por primeras 20 letras
            if not ev:
                for (k_c, k_t), v in eval_map.items():
                    if k_c[:20] == nombre[:20]:
                        ev = v
                        break
            # Default para carreras no evaluadas (estaban fuera del top-N)
            relevancia = (ev or {}).get("relevancia", "media")
            razon      = (ev or {}).get("razon", "no evaluada directamente")
            peso       = PESOS.get(relevancia, 0.0)
            mat_pond   = round(r["matriculados"] * peso)
            total_pond += mat_pond

            if relevancia != "baja":
                programas.append({
                    "nomb_inst":    r["nomb_inst"],
                    "region_sede":  r.get("region_sede", "No especificada"),
                    "nomb_carrera": r["nomb_carrera"],
                    "tipo_inst":    r.get("tipo_inst_1", ""),
                    "nivel":        r.get("nivel_carrera_1", ""),
                    "modalidad":    r.get("modalidad", ""),
                    "matriculados": r["matriculados"],
                    "relevancia":   relevancia,
                    "razon":        razon,
                    "matriculados_ponderados": mat_pond,
                })

        # Ordenar por matrícula ponderada desc
        programas.sort(key=lambda x: -x["matriculados_ponderados"])

        programas_relevantes[sector]    = programas
        matriculados_ponderados[sector] = total_pond

        alta  = sum(1 for p in programas if p["relevancia"] == "alta")
        media = sum(1 for p in programas if p["relevancia"] == "media")
        log.info(
            f"  Programas relevantes: {len(programas)} "
            f"(alta={alta}, media={media}) | mat. pond.: {total_pond:,}"
        )

        time.sleep(7)   # respetar rate limit Groq (~1 req/6s plan gratuito)

    # ── Guardar ──────────────────────────────────────────────────────────────
    datos["programas_relevantes_por_sector"]  = programas_relevantes
    datos["matriculados_ponderados_por_sector"] = matriculados_ponderados
    datos["fecha_evaluacion_mallas"]           = datetime.now().isoformat()

    with open(ENTRADA, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)

    log.info("\n" + "=" * 60)
    log.info(f"Guardado: {ENTRADA}")
    log.info("Matrícula ponderada por sector:")
    mat_raw = datos.get("matriculados_por_sector", {})
    for s in SECTOR_DESC:
        orig = mat_raw.get(s, 0)
        pond = matriculados_ponderados.get(s, 0)
        pct  = round(pond / orig * 100) if orig else 0
        n    = len(programas_relevantes.get(s, []))
        log.info(f"  {s:22s}: {orig:>7,} → {pond:>7,} ({pct:>3}%) | {n} programas")


if __name__ == "__main__":
    main()
