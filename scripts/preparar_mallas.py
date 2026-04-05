"""
preparar_mallas.py — Talento País
Evalúa la relevancia real de las carreras Mineduc para cada sector estratégico
usando Groq (Llama 3.3 70B, gratuito).  Corre UNA VEZ AL AÑO.

Eficiencia: 1 llamada Groq por sector = 6 llamadas totales (~3 000 tokens).
Input:  datos/raw/carreras_estrategicas.json
Output: datos/raw/carreras_estrategicas.json (actualizado in-place)
        Agrega en resumen_por_sector[sector]:
          "relevancia_malla": [{nomb_carrera, matriculados, relevancia, razon, matriculados_ponderados}]
          "matriculados_ponderados": int
        Agrega a nivel raíz:
          "matriculados_ponderados_por_sector": {sector: int}
          "fecha_evaluacion_mallas": ISO date

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
from datetime import datetime
from pathlib import Path

import requests

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("preparar_mallas.log", encoding="utf-8")],
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"
ENTRADA      = Path(__file__).parent.parent / "datos" / "raw" / "carreras_estrategicas.json"

# Pesos por nivel de relevancia
PESOS = {"alta": 1.0, "media": 0.5, "baja": 0.0}

# ─── Descripción de cada sector (qué perfil necesita) ────────────────────────
SECTOR_DESC = {
    "litio": (
        "Litio y Minería: profesionales que trabajen en extracción/procesamiento de litio "
        "(hidrometalurgia, electroquímica, salmueras), exploración geológica, ingeniería de minas, "
        "metalurgia extractiva, química industrial minera."
    ),
    "energias_renovables": (
        "Energías Renovables: diseño e instalación de sistemas fotovoltaicos, eólicos y de "
        "almacenamiento; hidrógeno verde (electrolizadores, fuel cells); ingeniería eléctrica "
        "de potencia; eficiencia energética industrial."
    ),
    "ia_tecnologia": (
        "IA y Tecnología: machine learning, deep learning, ciencia de datos, desarrollo de "
        "software, sistemas inteligentes, ciberseguridad, computación en la nube, "
        "automatización de procesos con IA."
    ),
    "astronomia": (
        "Astronomía: astrofísica observacional e instrumental, física teórica, procesamiento "
        "de imágenes astronómicas, manejo de telescopios y radiotelescopios, astroquímica."
    ),
    "oceanografia": (
        "Oceanografía y Ciencias del Mar: oceanografía física/química/biológica, biología "
        "marina, acuicultura, pesquerías sostenibles, cambio climático oceánico, "
        "gestión de ecosistemas costeros."
    ),
    "asia_pacifico": (
        "Asia-Pacífico: comercio exterior con Asia (China, Japón, Corea), idiomas asiáticos, "
        "relaciones internacionales, negociación intercultural, logística de exportaciones, "
        "diplomacia económica."
    ),
}

# ─── Prompt (tokens mínimos, respuesta estructurada) ─────────────────────────
PROMPT = """\
Eres un experto en educación superior chilena. Evalúa la relevancia de cada carrera \
para el sector indicado. Considera el perfil real del egresado en Chile, no solo el nombre.

SECTOR: {desc}

CRITERIO:
- "alta": egresados trabajan DIRECTAMENTE en este sector (formación específica)
- "media": aportan habilidades aplicables pero la carrera no es específica del sector
- "baja": poca o ninguna relación con el sector

CARRERAS:
{lista}

Responde SOLO con un JSON array, sin markdown ni texto adicional:
[{{"c": "NOMBRE_EXACTO", "r": "alta|media|baja", "x": "razón en ≤8 palabras"}}]
"""


def llamar_groq(sector: str, carreras: list[dict]) -> list[dict]:
    """
    Una sola llamada Groq para evaluar todas las carreras del sector.
    Reintenta una vez si hay rate-limit (429).
    """
    if not GROQ_API_KEY:
        log.warning("  GROQ_API_KEY no configurada — skip")
        return []

    lista = "\n".join(
        f"- {c['nomb_carrera']} ({c['matriculados']:,} mat.)"
        for c in carreras
    )
    prompt = PROMPT.format(desc=SECTOR_DESC.get(sector, sector), lista=lista)

    payload = {
        "model":    GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens":  600,
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

            # Limpiar posible markdown ```json ... ```
            for parte in texto.split("```"):
                parte = parte.strip().lstrip("json").strip()
                if parte.startswith("["):
                    texto = parte
                    break

            resultado = json.loads(texto)
            log.info(f"  Groq → {len(resultado)} evaluaciones para '{sector}'")
            return resultado

        except json.JSONDecodeError as e:
            log.warning(f"  Groq respuesta no parseable ({sector}): {e}")
            return []
        except Exception as e:
            log.error(f"  Groq error ({sector}, intento {intento+1}): {e}")
            if intento == 0:
                time.sleep(10)

    return []


def main():
    parser = argparse.ArgumentParser(description="Evalúa relevancia de carreras Mineduc por sector")
    parser.add_argument("--sector", default=None, help="Procesar solo un sector (ej: litio)")
    parser.add_argument("--force",  action="store_true", help="Reevaluar aunque ya exista")
    args = parser.parse_args()

    if not ENTRADA.exists():
        log.error(f"No encontrado: {ENTRADA}  — corre primero el pipeline con el CSV Mineduc")
        return

    with open(ENTRADA, encoding="utf-8") as f:
        datos = json.load(f)

    resumen  = datos.get("resumen_por_sector", {})
    sectores = [args.sector] if args.sector else list(SECTOR_DESC.keys())
    matriculados_ponderados = datos.get("matriculados_ponderados_por_sector", {})

    log.info("=" * 60)
    log.info("TALENTO PAÍS — Evaluación de mallas curriculares")
    log.info(f"Sectores: {sectores}  |  Force: {args.force}")
    log.info(f"Groq: {'configurado' if GROQ_API_KEY else 'NO CONFIGURADO'}")
    log.info("=" * 60)

    for sector in sectores:
        info = resumen.get(sector)
        if not info:
            log.warning(f"  '{sector}' no encontrado en carreras_estrategicas.json — skip")
            continue

        # No reevaluar si ya existe y no se forzó
        if not args.force and info.get("relevancia_malla"):
            mat_pond = info.get("matriculados_ponderados", 0)
            matriculados_ponderados[sector] = mat_pond
            log.info(
                f"  {sector}: ya evaluado "
                f"({len(info['relevancia_malla'])} carreras, {mat_pond:,} mat. pond.) — skip"
            )
            continue

        carreras = info.get("top_carreras", [])
        if not carreras:
            log.warning(f"  {sector}: sin carreras — skip")
            continue

        log.info(f"\n=== {sector.upper()} ({len(carreras)} carreras) ===")

        evaluaciones = llamar_groq(sector, carreras)

        # Mapa nombre-normalizado → evaluación (Groq puede devolver nombre distinto)
        eval_map: dict[str, dict] = {}
        for ev in evaluaciones:
            clave = (ev.get("c") or ev.get("carrera") or "").upper().strip()
            eval_map[clave] = {
                "relevancia": ev.get("r") or ev.get("relevancia") or "baja",
                "razon":      ev.get("x") or ev.get("razon")       or "",
            }

        # Calcular matrícula ponderada carrera a carrera
        total_pond   = 0
        resultado    = []
        alta_count   = 0
        media_count  = 0
        baja_count   = 0

        for c in carreras:
            nombre   = c["nomb_carrera"].upper().strip()
            ev       = eval_map.get(nombre, {})
            # Fallback: buscar coincidencia parcial si Groq abrevió el nombre
            if not ev:
                for k, v in eval_map.items():
                    if k[:20] == nombre[:20]:
                        ev = v
                        break
            relevancia = ev.get("relevancia", "media")   # default media si Groq no devolvió
            peso       = PESOS.get(relevancia, 0.0)
            mat_pond   = round(c["matriculados"] * peso)
            total_pond += mat_pond

            resultado.append({
                "nomb_carrera":            c["nomb_carrera"],
                "matriculados":            c["matriculados"],
                "relevancia":              relevancia,
                "razon":                   ev.get("razon", ""),
                "matriculados_ponderados": mat_pond,
            })

            if   relevancia == "alta":  alta_count  += 1
            elif relevancia == "media": media_count += 1
            else:                        baja_count  += 1

        info["relevancia_malla"]      = resultado
        info["matriculados_ponderados"] = total_pond
        matriculados_ponderados[sector] = total_pond

        mat_total = info.get("total_matriculados", 0)
        pct = round(total_pond / mat_total * 100) if mat_total else 0
        log.info(
            f"  Alta: {alta_count} | Media: {media_count} | Baja: {baja_count}\n"
            f"  Matrícula: {mat_total:,} → ponderada: {total_pond:,} ({pct}%)"
        )

        time.sleep(7)   # cortesía con Groq (límite ~1 req/6s en plan gratis)

    # Guardar resultado
    datos["matriculados_ponderados_por_sector"] = matriculados_ponderados
    datos["fecha_evaluacion_mallas"]            = datetime.now().isoformat()

    with open(ENTRADA, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)

    log.info("\n" + "=" * 60)
    log.info(f"Guardado: {ENTRADA}")
    log.info("Resumen matrícula ponderada:")
    mat_raw = datos.get("matriculados_por_sector", {})
    for s, v in matriculados_ponderados.items():
        orig = mat_raw.get(s, 0)
        pct  = round(v / orig * 100) if orig else 0
        log.info(f"  {s:22s}: {orig:>7,} bruto → {v:>7,} ponderado ({pct:>3}%)")


if __name__ == "__main__":
    main()
