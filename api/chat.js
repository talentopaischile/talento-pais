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
            text: `Eres un asistente útil y profesional de la plataforma Talento País Chile. 
                   Responde siempre en español, de forma clara, concisa y útil. 
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