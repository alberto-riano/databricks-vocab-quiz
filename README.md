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
- Django
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