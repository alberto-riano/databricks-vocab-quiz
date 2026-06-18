# Databricks Quiz App

Aplicación web desarrollada con **Django** para practicar preguntas tipo test relacionadas con **Databricks, Spark y conceptos de Data Engineering**.

La aplicación permite ejecutar distintos **exámenes tipo test** con preguntas de opción múltiple y ver la **explicación detallada** después de responder.

---

# Qué hace el proyecto

La web permite:

- Practicar **exámenes de Databricks**
- Soportar **múltiples exámenes** (exam 1, exam 2, exam 3, exam 4, exam 5…)
- Mostrar preguntas con:
  - 4 o más opciones
  - respuesta única
  - o multi-respuesta
- Mostrar **explicación completa** tras contestar
- Navegar entre preguntas
- Reiniciar el examen
- Tener preguntas en **inglés y español**

Las preguntas están definidas en estructuras JSON dentro del código Python.

---

# Requisitos

- Python 3
- Django`
- Entorno virtual recomendado

---

# Cómo ejecutar el proyecto

Antes de ejecutar el proyecto hay que activar el entorno virtual.

En Mac / Linux:

```bash
source vocabularioProject/bin/activate
```

Después, desde la carpeta donde está `manage.py`, ejecutar:

```bash
python manage.py runserver
```

Esto levantará el servidor de desarrollo de Django normalmente en:

http://127.0.0.1:8000/

También puedes acceder usando:

http://localhost:8000/


# Exponer la aplicación con un túnel

Si quieres acceder a la aplicación desde fuera de tu red local o compartirla con alguien, puedes usar un túnel.

## Opción 1 — Cloudflare Tunnel

Ejecuta:

```bash
cloudflared tunnel --url http://localhost:8000
```

o

```bash
cloudflared tunnel --url http://127.0.0.1:8000
```

Si funciona correctamente, aparecerá una URL pública similar a:

https://random-name.trycloudflare.com

Cualquier persona con esa URL podrá acceder a tu aplicación.


## Si Cloudflare falla

A veces Cloudflare puede fallar con errores de red como:

- failed to dial to edge with quic
- timeout: no recent network activity
- problemas con QUIC

Esto suele deberse a:

- VPN activa
- firewall corporativo
- bloqueo de tráfico UDP
- restricciones de red

Puedes probar forzar HTTP2:

```bash
cloudflared tunnel --url http://localhost:8000 --protocol http2
```

o

```bash
cloudflared tunnel --url http://127.0.0.1:8000 --protocol http2
```


## Opción 2 — ngrok

Otra alternativa es usar ngrok.

Ejecuta:

```bash
ngrok http 8000
```

Esto generará una URL pública similar a:

https://xxxxx.ngrok-free.app

Esa URL redirige al servidor Django que está corriendo en tu máquina.


## Opción 3 — LocalTunnel

Otra alternativa:

```bash
npx localtunnel --port 8000
```

Esto generará una URL pública que apunta a tu servidor local.


# Flujo típico para arrancar la aplicación

1. Ir a la carpeta del proyecto

```bash
cd ruta/del/proyecto
```

2. Activar el entorno virtual

```bash
source vocabularioProject/bin/activate
```

3. Levantar Django

```bash
python manage.py runserver
```

4. Abrir en el navegador

http://127.0.0.1:8000/

5. Si quieres compartir la aplicación

```bash
cloudflared tunnel --url http://localhost:8000
```

o

```bash
ngrok http 8000
```
http://127.0.0.1:8000/quiz/databricks/
http://127.0.0.1:8000/quiz/english/


## PROMPT GEMINI

Tengo un vídeo donde hago scroll por preguntas de un examen de práctica de Databricks.
Extrae TODAS las preguntas y devuélvelas EXACTAMENTE en el siguiente formato Python.

NOMBRE DE LA VARIABLE: DATABRICKS_PROFESSIONAL_QUIZ   ← usa este exacto
NÚMERO DE EXAMEN: 2   ← usa este exacto en el campo "exam"

═══════════════════════════════════════════════
FORMATO (añade solo los nuevos dicts, sin la variable, yo los pegaré):
═══════════════════════════════════════════════

    {
        "exam": 2,
        "id": "q01_nombre_corto_snake_case",
        "question": "Texto completo de la pregunta tal cual aparece.",
        "options": [
            "Opción A",
            "Opción B",
            "Opción C",
            "Opción D",
        ],
        "answer": "Opción correcta (texto IDÉNTICO al de options)",
        "explanation": "Por qué es correcta y por qué las demás están mal.",
    },

═══════════════════════════════════════════════
REGLAS OBLIGATORIAS:
═══════════════════════════════════════════════

1. "exam": siempre el número que yo te indique (aquí: 2).

2. "id": snake_case único, máx. 5 palabras, describe el tema. Numera en orden: q01_, q02_, ...

3. "question": texto completo tal cual aparece. Usa \n para saltos de línea si los hay.
   Para preguntas largas usa concatenación con paréntesis:
   "question": (
       "Primera parte...\n\n"
       "Segunda parte..."
   ),

4. "options": lista con TODAS las opciones en el mismo orden que en el vídeo.
   Si hay código dentro de una opción, usa \n para los saltos de línea.
   IMPORTANTE — comillas en opciones con código:
   • Si el texto de la opción contiene comillas dobles, usa comillas simples para la cadena:
     'df.withColumn("col", func())'   ← correcto
     "df.withColumn("col", func())"   ← INCORRECTO, rompe Python

5. "answer": texto IDÉNTICO (carácter por carácter) a como aparece en "options".
   - Una respuesta correcta → string: "answer": "Opción A"
   - Varias respuestas correctas → lista: "answer": ["Opción A", "Opción C"]
   IMPORTANTE: los valores que uses en "answer" deben coincidir exactamente 
   con cómo están escritos en "options" (incluida versión de API, mayúsculas, etc.)

6. "explanation": explica por qué la respuesta es correcta y por qué las demás están mal.
   Usa los mismos valores exactos (versiones de API, nombres de endpoints, etc.) 
   que aparecen en la pregunta y las opciones. No inventes variaciones.

7. Si no puedes leer bien un texto, márcalo:  # REVISAR: texto poco legible

8. Devuelve SOLO los dicts Python listos para pegar dentro de una lista, sin ningún 
   texto extra, sin bloques ```python, sin la variable, sin corchetes de lista.
   Empieza directamente con "    {" y termina con "    },"