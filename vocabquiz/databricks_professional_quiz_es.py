DATABRICKS_PROFESSIONAL_QUIZ = [
    # ---------------- EXAM 1 ----------------

    {
        "exam": 1,
        "id": "q01_delta_optimize_compaction",
        "question": "¿Cuál de los siguientes comandos puede usar un data engineer para compactar archivos pequeños (small files) de una tabla Delta en otros más grandes?",
        "options": ["OPTIMIZE", "VACUUM", "COMPACT", "ZORDER BY"],
        "answer": "OPTIMIZE",
        "explanation": (
            "El comando OPTIMIZE es la función nativa de Delta Lake diseñada específicamente para solucionar el problema de los "
            "\"archivos pequeños\". Realiza una operación de bin-packing, que consiste en leer muchos archivos pequeños y "
            "reescribirlos combinados en archivos más grandes (idealmente de 1 GB) para mejorar la velocidad de lectura.\n"
            "• VACUUM borra archivos viejos.\n"
            "• ZORDER ordena datos (se usa con optimize).\n"
            "• COMPACT no es un comando de Databricks (aunque describe la acción)."
        ),
    },

    # ---------------- EXAM 2 ----------------
    # TODO: añadir preguntas del examen 2 (ES)

    # ---------------- EXAM 3 ----------------
    # TODO: añadir preguntas del examen 3 (ES)
]