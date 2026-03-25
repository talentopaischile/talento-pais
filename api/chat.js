// api/chat.js
export default async function handler(req, res) {
  // Solo permitir POST
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Método no permitido' });
  }

  const { message } = req.body;

  if (!message) {
    return res.status(400).json({ error: 'Mensaje requerido' });
  }

  const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

  if (!GEMINI_API_KEY) {
    return res.status(500).json({ error: 'Clave de Gemini no configurada' });
  }

  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key=${GEMINI_API_KEY}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{
            parts: [{
              text: `Eres un asistente útil, profesional y motivador de la plataforma Talento País Chile. 
                     Responde siempre en español, de forma clara, concisa y útil. 
                     Contexto: Esta es una plataforma que conecta talento estratégico en litio y energías renovables con el Estado chileno.
                     Pregunta del usuario: ${message}`
            }]
          }]
        })
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error('Gemini error:', response.status, errorText);
      return res.status(500).json({ 
        error: response.status === 429 ? 'Límite de consultas alcanzado. Intenta más tarde.' : 'Error en Gemini' 
      });
    }

    const data = await response.json();
    const reply = data.candidates?.[0]?.content?.parts?.[0]?.text || "No pude generar una respuesta.";

    return res.status(200).json({ reply });

  } catch (error) {
    console.error('Error en serverless function:', error);
    return res.status(500).json({ error: 'Error interno del servidor' });
  }
}