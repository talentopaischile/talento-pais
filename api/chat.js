// api/chat.js - Usando gemini-2.0-flash (recomendado marzo 2026)
export default async function handler(req, res) {
  console.log("📥 Petición recibida en /api/chat");

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Método no permitido' });
  }

  const { message } = req.body;
  if (!message) {
    return res.status(400).json({ error: 'Mensaje requerido' });
  }

  const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

  if (!GEMINI_API_KEY) {
    console.error("❌ GEMINI_API_KEY no configurada en Vercel");
    return res.status(500).json({ error: "Clave de Gemini no configurada" });
  }

  try {
    const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_API_KEY}`;

    console.log("🔗 Llamando a Gemini 2.0 Flash");

    const response = await fetch(geminiUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{
          parts: [{
            text: `Eres el asistente oficial de Talento País, una plataforma chilena de capital humano estratégico con enfoque en Asia-Pacífico e inteligencia artificial.

CONTEXTO DE LA PLATAFORMA:
Talento País conecta el talento estratégico de Chile con los sectores donde el país tiene ventaja competitiva: litio, energías renovables (solar, eólica, hidrógeno verde) e inteligencia artificial. También opera como puente entre Chile y Asia-Pacífico (China, Corea del Sur, Japón, Singapur, Australia).

FUNCIONALIDADES DE LA PLATAFORMA:
1. Brechas de talento: Dashboard con perfiles que faltan en litio y renovables, cruzando datos de universidades con demanda laboral real.
2. Mapa institucional: Coordinación entre Min. Energía, Min. Educación, Cancillería, CORFO, AGCI y Min. Minería para detectar sinergias.
3. Guía de títulos: Reconocimiento de títulos extranjeros en Chile según país de origen (China, Corea, Japón, etc.).
4. Módulo Asia-Pacífico: Becas (CSC China, GKS Corea, MEXT Japón, AGCI), prácticas, empleos, investigación, cursos gratuitos (K-MOOC, JMOOC), fondos (FONDEF, ADB) y red de traductores chino/japonés/coreano.
5. Registro de talento: Formulario de 3 pasos para que profesionales se registren voluntariamente.

CONTEXTO CHILENO:
Chile lidera IA en LATAM (Índice ILIA 2024) pero pierde talento especializado hacia EE.UU., Luxemburgo y Suiza. Hay fuga de talento en sectores clave. 4 ministerios tienen programas similares sin coordinarse. La plataforma actúa como intermediaria, no conecta directamente empresas con personas, sino que informa a los tomadores de decisión del Estado para que actúen.

TONO: Institucional, claro, sin sesgos políticos, orientado a la acción. Usa datos cuando los tengas. Siempre responde en español.

Pregunta del usuario: ${message}`
          }]
        }]
      })
    });

    console.log("📡 Gemini status:", response.status);

    if (!response.ok) {
      const errorBody = await response.text();
      console.error("❌ Gemini error:", errorBody);
      return res.status(500).json({ 
        error: response.status === 429 ? "Límite de consultas alcanzado. Espera unos minutos." : `Gemini error ${response.status}`
      });
    }

    const data = await response.json();
    const reply = data.candidates?.[0]?.content?.parts?.[0]?.text || "No pude generar una respuesta.";

    return res.status(200).json({ reply });

  } catch (error) {
    console.error("💥 Error en /api/chat:", error.message);
    return res.status(500).json({ error: "Error interno al conectar con Gemini" });
  }
}