// api/chat.js - Versión ultra simple y con logs claros
export default async function handler(req, res) {
  console.log("📥 Recibida petición al /api/chat");

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Solo se permite POST' });
  }

  const { message } = req.body;
  if (!message) {
    return res.status(400).json({ error: 'Se requiere un mensaje' });
  }

  const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

  if (!GEMINI_API_KEY) {
    console.error("❌ ERROR: GEMINI_API_KEY no está configurada en Vercel");
    return res.status(500).json({ error: "Clave de Gemini no configurada en Vercel" });
  }

  if (GEMINI_API_KEY.length < 30) {
    console.error("❌ ERROR: La clave parece demasiado corta");
    return res.status(500).json({ error: "Clave de Gemini inválida" });
  }

  try {
    console.log("🔑 Usando Gemini con clave de longitud:", GEMINI_API_KEY.length);

    const geminiResponse = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key=${GEMINI_API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ 
            parts: [{ 
              text: `Responde en español de forma clara y útil. Pregunta: ${message}` 
            }] 
          }]
        })
      }
    );

    console.log("📡 Gemini respondió con status:", geminiResponse.status);

    if (!geminiResponse.ok) {
      const errorText = await geminiResponse.text();
      console.error("❌ Gemini error body:", errorText);
      return res.status(500).json({ 
        error: geminiResponse.status === 429 ? "Límite de Gemini alcanzado" : `Gemini error ${geminiResponse.status}` 
      });
    }

    const data = await geminiResponse.json();
    const reply = data.candidates?.[0]?.content?.parts?.[0]?.text || "No pude generar una respuesta.";

    return res.status(200).json({ reply });

  } catch (error) {
    console.error("💥 Error completo en /api/chat:", error.message);
    return res.status(500).json({ error: "Error interno al conectar con Gemini" });
  }
}