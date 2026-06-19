DATABRICKS_PROFESSIONAL_QUIZ = [
    # ---------------- EXAM 1 ----------------

    {
        "exam": 1,
        "id": "q01_delta_partition_candidate_activity_date",
        "question": (
            "Un data engineer quiere crear una tabla Delta Lake para almacenar las actividades de los usuarios de un sitio web. "
            "La tabla tiene el siguiente schema:\n\n"
            "user_id LONG, page STRING, activity_type LONG, ip_address STRING, activity_time TIMESTAMP,\n"
            "activity_date DATE\n\n"
            "Basado en el schema anterior, ¿qué columna es una buena candidata para el particionado (partitioning) de la tabla Delta?"
        ),
        "options": [
            "activity_type",
            "activity_date",
            "activity_time",
            "user_id"
        ],
        "answer": "activity_date",
        "explanation": (
            "Al elegir columnas de particionado, conviene tener en cuenta que los registros con un valor determinado "
            "(las actividades de un usuario concreto) seguirán llegando indefinidamente. En un caso así, se utiliza "
            "una columna de fecha y hora para el particionado. Esto permite optimizar las particiones y también archivar "
            "fácilmente las particiones de periodos anteriores, si fuera necesario."
        ),
    },
    {
        "exam": 1,
        "id": "q02_delta_file_statistics_benefit",
        "question": "¿Cuál de los siguientes es el beneficio de las Delta Lake File Statistics?",
        "options": [
            "Se aprovechan para el data skipping al ejecutar queries selectivas.",
            "Se aprovechan para la compresión de datos con el fin de mejorar el Delta Caching.",
            "Se utilizan como checksums para verificar la corrupción de datos en archivos parquet.",
            "Se aprovechan para la predicción del tiempo de procesamiento al ejecutar queries selectivas."
        ],
        "answer": "Se aprovechan para el data skipping al ejecutar queries selectivas.",
        "explanation": (
            "Las Delta Lake File Statistics almacenan metadatos por archivo como el número total de registros, "
            "los valores mínimos y máximos de cada columna (normalmente para las primeras columnas) y el número de valores nulos. "
            "Estas estadísticas se utilizan para el data skipping, permitiendo a Delta Lake evitar leer archivos completos "
            "que no cumplen las condiciones de una query.\n\n"
            "Esto mejora significativamente el rendimiento de queries selectivas, ya que el engine puede determinar qué archivos "
            "no contienen datos relevantes sin tener que escanearlos completamente."
        ),
    },
    {
        "exam": 1,
        "id": "q03_delta_file_statistics_default_columns",
        "question": "¿Qué afirmación sobre las Delta Lake File Statistics es correcta?",
        "options": [
            "Por defecto, Delta Lake captura estadísticas en el transaction log sobre las primeras 16 columnas de cada tabla.",
            "Por defecto, Delta Lake captura estadísticas en el transaction log sobre las primeras 32 columnas de cada tabla.",
            "Por defecto, Delta Lake captura estadísticas en el Hive metastore sobre las primeras 32 columnas de cada tabla.",
            "Por defecto, Delta Lake captura estadísticas en el Hive metastore sobre las primeras 16 columnas de cada tabla."
        ],
        "answer": "Por defecto, Delta Lake captura estadísticas en el transaction log sobre las primeras 32 columnas de cada tabla.",
        "explanation": (
            "Delta Lake captura automáticamente estadísticas en el transaction log por cada archivo de datos añadido a la tabla. "
            "Por defecto, recoge estas estadísticas para las primeras 32 columnas de cada tabla.\n\n"
            "Estas estadísticas incluyen:\n"
            "• Número total de registros\n"
            "• Valor mínimo por columna\n"
            "• Valor máximo por columna\n"
            "• Número de valores nulos por columna\n\n"
            "Estas métricas se utilizan para funcionalidades como data skipping, permitiendo que el motor evite leer archivos que no cumplen los filtros de una query, mejorando así el rendimiento."
        ),
    },
    {
        "exam": 1,
        "id": "q04_delta_streaming_available_now",
        "question": "El equipo de data engineering tiene una tabla bronze singleplex llamada ‘orders_raw’ donde se añaden (append) nuevos datos de pedidos cada noche. Crearon una nueva tabla Silver llamada ‘orders_cleaned’ para proporcionar una vista más refinada de los datos de pedidos.\n\nEl equipo quiere crear un pipeline de procesamiento batch para procesar todos los registros nuevos insertados en la tabla orders_raw y propagarlos a la tabla orders_cleaned.\n\n¿Qué solución minimiza los costes de compute para propagar este batch de datos?",
        "options": [
            "Usar las capacidades de time travel en Delta Lake para comparar la última versión de orders_raw con la versión anterior, y luego escribir la diferencia en la tabla orders_cleaned.",
            "Usar la lógica foreachBatch de Spark Structured Streaming para procesar los nuevos registros de orders_raw usando trigger(processingTime=\"24 hours\")",
            "Usar lógica batch de overwrite para reprocesar todos los registros en orders_raw y sobrescribir la tabla orders_cleaned.",
            "Usar Spark Structured Streaming para procesar los nuevos registros de orders_raw en modo batch usando la opción trigger availableNow."
        ],
        "answer": "Usar Spark Structured Streaming para procesar los nuevos registros de orders_raw en modo batch usando la opción trigger availableNow.",
        "explanation": (
            "La opción trigger(availableNow=True) es la más eficiente en términos de compute para ejecuciones batch programadas o puntuales, "
            "ya que procesa todos los datos disponibles en una sola ejecución y se detiene automáticamente, evitando mantener el cluster activo innecesariamente.\n\n"
            "Por el contrario, trigger(processingTime=\"24 hours\") mantiene el stream activo de forma continua, lo que implica más consumo de recursos incluso cuando no hay datos nuevos.\n\n"
            "También existe trigger(once=True) para procesamiento incremental en batch, pero está deprecated en versiones recientes de Databricks Runtime. "
            "Actualmente, Databricks recomienda utilizar trigger(availableNow=True) para este tipo de pipelines incrementales."
        ),
    },
    {
        "exam": 1,
        "id": "q05_delta_cdf_use_case",
        "question": "El equipo de data engineering mantiene una tabla Type 1 que se sobrescribe (overwritten) cada noche con nuevos datos recibidos del sistema origen.\n\nUn data engineer junior ha sugerido habilitar la funcionalidad Change Data Feed (CDF) en la tabla para identificar las filas que fueron actualizadas, insertadas o eliminadas.\n\n¿Qué respuesta a la sugerencia del data engineer junior es correcta?",
        "options": [
            "Los cambios de datos de la tabla capturados por CDF solo se pueden leer en modo streaming.",
            "CDF es útil cuando solo una pequeña fracción de los registros se actualiza en cada batch.",
            "CDF no se puede habilitar en tablas existentes. Solo se puede habilitar en tablas recién creadas.",
            "CDF es útil cuando la tabla es una Slowly Changing Dimension (SCD) de Type 2."
        ],
        "answer": "CDF es útil cuando solo una pequeña fracción de los registros se actualiza en cada batch.",
        "explanation": (
            "El Change Data Feed (CDF) de Delta Lake se utiliza para capturar cambios incrementales en una tabla "
            "(inserts, updates y deletes) y propagarlos a tablas downstream en arquitecturas multi-hop.\n\n"
            "CDF es especialmente útil cuando solo una pequeña fracción de los registros cambia en cada batch, "
            "ya que permite procesar únicamente esos cambios en lugar de volver a procesar toda la tabla.\n\n"
            "Si la mayor parte de los registros cambia o la tabla se sobrescribe completamente en cada ejecución "
            "(como ocurre en la pregunta con una tabla Type 1 que se overwrite cada noche), entonces CDF no aporta "
            "ventajas y puede incluso introducir sobrecoste innecesario.\n\n"
            "CDF se utiliza típicamente cuando:\n"
            "• Existen updates o deletes en la tabla\n"
            "• Solo una pequeña fracción de registros cambia por batch\n"
            "• Los datos provienen de fuentes externas en formato CDC\n"
            "• Se quieren propagar cambios incrementales a tablas downstream"
        ),
    },
    {
        "exam": 1,
        "id": "q06_delta_ctas_drop_source",
        "question": "El equipo de data engineering tiene una tabla Delta Lake creada con la siguiente query:\n\nCREATE TABLE target\nAS SELECT * FROM source\n\nUn data engineer quiere hacer drop de la tabla source con la siguiente query:\n\nDROP TABLE source\n\n¿Qué afirmación describe el resultado de ejecutar este comando drop?",
        "options": [
            "Solo se eliminará la tabla source, pero la tabla target dejará de ser consultable (queryable).",
            "Solo se eliminará la tabla source, mientras que la tabla target no se verá afectada.",
            "Ocurrirá un error indicando que otras tablas están basadas en esta tabla source.",
            "Tanto la tabla target como la source serán eliminadas."
        ],
        "answer": "Solo se eliminará la tabla source, mientras que la tabla target no se verá afectada.",
        "explanation": (
            "Las sentencias CREATE TABLE AS SELECT (CTAS) crean una nueva tabla Delta independiente utilizando el resultado de una query SELECT. "
            "Esto significa que los datos se copian físicamente en la nueva tabla, y no existe una dependencia directa con la tabla original.\n\n"
            "Por lo tanto, al ejecutar DROP TABLE sobre la tabla source, únicamente se elimina esa tabla, mientras que la tabla target "
            "permanece intacta y completamente accesible.\n\n"
            "En otras palabras, CTAS no crea una vista ni una referencia, sino una copia materializada de los datos."
        ),
    },
    {
        "exam": 1,
        "id": "q07_streaming_window_not_supported",
        "question": "Un data engineer junior está probando el siguiente bloque de código para obtener la entrada más reciente de cada artículo añadido en la tabla ‘sales’ desde la última actualización de la tabla.\n\nfrom pyspark.sql import functions as F\nfrom pyspark.sql.window import Window\n\nwindow = Window.partitionBy(\"item_id\").orderBy(F.col(\"item_time\").desc())\n\nranked_df = (spark.readStream\n    .table(\"sales\")\n    .withColumn(\"rank\", F.rank().over(window))\n    .filter(\"rank == 1\")\n    .drop(\"rank\")\n)\n\ndisplay(ranked_df)\n\nSin embargo, el comando falla al ejecutarse.\n\n¿Qué afirmación explica la causa de este fallo?",
        "options": [
            "Falta el watermarking. Debería añadirse para permitir el seguimiento de la información de estado para el window de tiempo.",
            "El output de la query no se puede mostrar con display. Deberían usar spark.writeStream para persistir el resultado de la query.",
            "Las window operations que no están basadas en el tiempo no están soportadas en streaming DataFrames. Necesitan implementarse dentro de una lógica foreachBatch en su lugar.",
            "El campo item_id no es único. Los registros deben ser deduplicados (de-duplicated) por el item_id usando la función dropDuplicates."
        ],
        "answer": "Las window operations que no están basadas en el tiempo no están soportadas en streaming DataFrames. Necesitan implementarse dentro de una lógica foreachBatch en su lugar.",
        "explanation": (
            "En Spark Structured Streaming no están soportadas las window functions no basadas en tiempo (como rank() sobre un Window clásico) "
            "directamente sobre streaming DataFrames.\n\n"
            "El código intenta aplicar una window function (rank over partition) sobre un readStream, lo cual genera un error porque este tipo de "
            "operaciones requieren acceso a todo el dataset, algo que no encaja con el modelo incremental del streaming.\n\n"
            "Para resolverlo, este tipo de lógica debe implementarse dentro de un foreachBatch, donde cada micro-batch se trata como un DataFrame batch normal "
            "y sí permite usar window functions como rank().\n\n"
            "Las window operations soportadas en streaming son únicamente las basadas en tiempo (time windows con watermarking), "
            "no las window functions tipo SQL analíticas (rank, row_number, etc.) directamente sobre streams."
        ),
    },

    # ---------------- EXAM 2 ----------------

    {
        "exam": 2,
        "id": "q01_trigger_job_run_now",
        "question": (
            "Un job programado falló debido a un problema con el data source en el upstream. Después de resolver el problema, "
            "el data engineer quiere usar la Jobs API para hacer un trigger del mismo job de nuevo sin esperar "
            "a su próxima ejecución programada.\n\n"
            "¿Cuál de las siguientes llamadas a la REST API logra este requisito?"
        ),
        "options": [
            "Enviar una petición GET al endpoint '/api/2.2/jobs/run'",
            "Enviar una petición POST al endpoint '/api/2.2/jobs/run'",
            "Enviar una petición POST al endpoint '/api/2.2/jobs/run-now'",
            "Enviar una petición POST al endpoint '/api/2.2/jobs/start'",
        ],
        "answer": "Enviar una petición POST al endpoint '/api/2.2/jobs/run-now'",
        "explanation": (
            "Enviar peticiones POST al endpoint '/api/2.2/jobs/run-now' te permite hacer trigger de un job run "
            "usando su job_id sin tener que esperar a la próxima ejecución programada.\n"
            "• Las peticiones GET son solo de lectura y no pueden lanzar acciones.\n"
            "• '/jobs/run' y '/jobs/start' no son endpoints válidos de la Jobs API."
        ),
    },
    {
        "exam": 2,
        "id": "q02_dynamic_file_pruning",
        "question": "¿Cuál de las siguientes afirmaciones describe mejor el dynamic file pruning en Apache Spark?",
        "options": [
            "Una técnica de optimización que duplica archivos de datos a través de los worker nodes para mejorar la localidad de datos y el rendimiento de la query.",
            "Una técnica de optimización que reparte dinámicamente los archivos en trozos más pequeños en runtime para balancear el workload entre los executors.",
            "Una técnica de optimización que se salta la lectura de archivos de datos irrelevantes durante la ejecución de la query basándose en información de filtros en runtime.",
            "Una técnica de optimización que comprime automáticamente archivos grandes durante la ejecución del Spark job para podar (prune) el uso de almacenamiento.",
        ],
        "answer": "Una técnica de optimización que se salta la lectura de archivos de datos irrelevantes durante la ejecución de la query basándose en información de filtros en runtime.",
        "explanation": (
            "El dynamic file pruning es una técnica de optimización que salta la lectura de data files irrelevantes durante la ejecución de la query "
            "aprovechando la información de filtros en runtime. Spark evita escanear archivos que no coinciden con los predicados de la query, "
            "mejorando así el rendimiento y reduciendo el I/O.\n"
            "• Duplicar archivos aumenta el almacenamiento, no el rendimiento.\n"
            "• Repartir (repartitioning) en runtime describe el adaptive query execution, no file pruning.\n"
            "• Comprimir archivos es una optimización de storage, no relacionada con file pruning."
        ),
    },
    {
        "exam": 2,
        "id": "q03_repair_job_run",
        "question": (
            "Un data engineer reparó un job run multi-task fallido en Databricks. Antes de hacer clic en Repair run, "
            "cambiaron el valor de un task parameter en el cuadro de diálogo de Repair run.\n\n"
            "¿Cuál de las siguientes opciones describe mejor el efecto de este cambio?"
        ),
        "options": [
            "El cambio se ignora porque los parámetros del job siempre sobrescriben a los parámetros del run.",
            "El repair run fallará porque esta funcionalidad solo soporta añadir nuevos parámetros, no actualizar los existentes.",
            "El parámetro actualizado se aplica solo al repair run actual y no modifica los parámetros guardados del job.",
            "El valor del parámetro actualizado se guarda permanentemente en la configuración del job.",
        ],
        "answer": "El parámetro actualizado se aplica solo al repair run actual y no modifica los parámetros guardados del job.",
        "explanation": (
            "Cuando se utiliza 'Repair run' para un job que ha fallado, el cuadro de diálogo permite ajustar parámetros para ese run en particular. "
            "Estos cambios no sobrescriben la configuración original del job — solo aplican para este repair run. "
            "Esto es útil para testear un fix sin alterar permanentemente la definición del job."
        ),
    },
    {
        "exam": 2,
        "id": "q04_retrieve_job_metadata",
        "question": (
            "Un data engineer quiere usar la Databricks REST API para obtener los metadatos de un job run utilizando su run_id.\n\n"
            "¿Cuál de las siguientes llamadas a la REST API logra este requisito?"
        ),
        "options": [
            "Enviar una petición POST al endpoint '/api/2.2/jobs/runs/get'",
            "Enviar una petición GET al endpoint '/api/2.2/jobs/runs/get-metadata'",
            "Enviar una petición GET al endpoint '/api/2.2/jobs/runs/get-output'",
            "Enviar una petición GET al endpoint '/api/2.2/jobs/runs/get'",
        ],
        "answer": "Enviar una petición GET al endpoint '/api/2.2/jobs/runs/get'",
        "explanation": (
            "Enviar peticiones GET al endpoint '/api/2.2/jobs/runs/get' permite recuperar los metadatos de un job run "
            "usando su run_id.\n"
            "• POST se utiliza para crear/lanzar acciones, no para obtener datos.\n"
            "• '/runs/get-metadata' y '/runs/get-output' no son nombres de endpoints válidos."
        ),
    },
    {
        "exam": 2,
        "id": "q05_spark_ui_metrics",
        "question": (
            "Un data engineer está analizando un Spark job a través de la Spark UI. Tienen las siguientes métricas de resumen "
            "para 27 tasks completadas en un stage en particular:\n\n"
            "Metric         | Min              | 25th pct         | Median           | 75th pct         | Max\n"
            "Duration       | 311 ms           | 311 ms           | 311 ms           | 311 ms           | 311 ms\n"
            "GC Time        | 0 ms             | 0 ms             | 0 ms             | 0 ms             | 0 ms\n"
            "Shuffle Read   | 10.0 MB / 51     | 105.1 MB / 188   | 120.3 MB / 217   | 140.5 MB / 257   | 167.9 MB / 270\n"
            "Shuffle Write  | 9.5 MB / 49      | 101.4 MB / 191   | 115.5 MB / 203   | 138.1 MB / 241   | 160.2 MB / 289\n\n"
            "¿Qué conclusión puede sacar el data engineer a partir de las estadísticas anteriores?"
        ),
        "options": [
            "Todos los tasks están operando sobre particiones con cantidades uniformes de datos.",
            "Una serie de tasks están operando sobre particiones casi vacías.",
            "Todos los tasks están operando sobre particiones casi vacías.",
            "Una serie de tasks están operando sobre particiones con cantidades grandes de datos sesgados (skewed)."
        ],
        "answer": "Una serie de tasks están operando sobre particiones casi vacías.",
        "explanation": (
            "Si la computación fuera completamente simétrica entre tasks, todas las estadísticas estarían agrupadas estrechamente alrededor de la mediana. "
            "Aquí, la distribución parece razonable excepto por los valores 'Min' (10 MB vs. 120 MB de mediana). "
            "Esta gran brecha en el mínimo sugiere que un subconjunto de tasks está procesando particiones casi vacías, "
            "no que todos los tasks tengan el problema."
        ),
    },
    {
        "exam": 2,
        "id": "q06_dynamic_reference_timezone",
        "question": (
            "Un data engineer usa la referencia dinámica {{job.start_time_iso_datetime}} para configurar el valor "
            "de un task parameter en un job.\n\n"
            "¿Cuál de las siguientes afirmaciones describe correctamente el timezone del timestamp devuelto?"
        ),
        "options": [
            "El timestamp está en UTC.",
            "El timestamp se basa en la hora local del usuario que hizo el trigger del job.",
            "El timestamp se basa en la hora local de la región cloud del workspace.",
            "El timestamp se basa en la hora local de la máquina virtual del cluster."
        ],
        "answer": "El timestamp está en UTC.",
        "explanation": (
            "En Databricks jobs, todas las referencias dinámicas basadas en tiempo — incluyendo {{job.start_time_iso_datetime}}, "
            "{{job.start_time}}, y {{run_date}} — están siempre en UTC. "
            "Esto asegura consistencia a través de regiones, clusters y usuarios independientemente de sus zonas horarias locales."
        ),
    },
    {
        "exam": 2,
        "id": "q07_liquid_clustering_auto",
        "question": '¿Cuál de los siguientes comandos puede usar un data engineer para crear una tabla Delta "orders" con el Automatic Liquid Clustering habilitado?',
        "options": [
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY (id, updated_date);",
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY NONE;",
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY AUTO;",
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY ALL;",
        ],
        "answer": "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY AUTO;",
        "explanation": (
            "El Automatic Liquid Clustering se habilita con CLUSTER BY AUTO. "
            "Delta maneja automáticamente el clustering basándose en patrones de queries y distribución de datos sin necesidad de especificar manualmente las columnas.\n"
            "• CLUSTER BY (id, updated_date) especifica manualmente las columnas — no es automático.\n"
            "• CLUSTER BY NONE deshabilita explícitamente el liquid clustering.\n"
            "• CLUSTER BY ALL no es una sintaxis válida de Delta Lake."
        ),
    },
    {
        "exam": 2,
        "id": "q08_window_function_tier",
        "question": (
            "Un data engineer tiene un DataFrame de PySpark con las columnas: employee_name, department, y salary. "
            "Quieren asignar un tier (nivel) único a cada empleado dentro de su departamento basándose en el salario (descendente), "
            "incluso si tienen el mismo salario.\n\n"
            "El window spec está definido como:\nwindow_spec = Window.partitionBy('department').orderBy(col('salary').desc())\n\n"
            "¿Cuál de las siguientes funciones calcula correctamente la columna de tier?"
        ),
        "options": [
            'df.withColumn("tier", rank().over(window_spec))',
            'df.withColumn("tier", dense_rank().over(window_spec))',
            'df.withColumn("tier", percent_rank().over(window_spec))',
            'df.withColumn("tier", row_number().over(window_spec))',
        ],
        "answer": 'df.withColumn("tier", row_number().over(window_spec))',
        "explanation": (
            "row_number() genera un número secuencial y único para cada fila dentro del window, "
            "garantizando la unicidad incluso cuando múltiples empleados comparten el mismo salario.\n"
            "• rank() asigna el mismo rank a los empates y se salta números (ej. 1, 1, 3).\n"
            "• dense_rank() asigna el mismo rank a los empates sin saltarse números (ej. 1, 1, 2).\n"
            "• percent_rank() devuelve un valor entre 0 y 1, no un número de tier."
        ),
    },
    {
        "exam": 2,
        "id": "q09_over_partitioned_table",
        "question": (
            "El equipo de data engineering quiere saber si las tablas que mantienen en el Lakehouse están excesivamente particionadas (over-partitioned).\n\n"
            "¿Cuál de los siguientes es un indicador de que una tabla Delta Lake está over-partitioned?"
        ),
        "options": [
            "Si la mayoría de las particiones en la tabla tienen más de 1 GB de datos.",
            "Si los datos en la tabla continúan llegando indefinidamente.",
            "Si la mayoría de las particiones en la tabla tienen menos de 1 GB de datos.",
            "Si el número de particiones en la tabla es demasiado bajo.",
        ],
        "answer": "Si la mayoría de las particiones en la tabla tienen menos de 1 GB de datos.",
        "explanation": (
            "Las tablas over-partitioned sufren una degradación significativa del rendimiento: los archivos no se pueden combinar a través de "
            "los límites de las particiones, aumentando los costes de storage y el número de archivos a escanear.\n"
            "La pauta general es que cada partición debe contener al menos 1 GB de datos. "
            "Si la mayoría de las particiones están por debajo de 1 GB, la tabla probablemente esté over-partitioned y debería ser re-particionada o "
            "migrada a Liquid Clustering."
        ),
    },
    {
        "exam": 2,
        "id": "q10_cluster_init_script",
        "question": (
            "Un equipo de data engineering quiere asegurarse de que una librería específica de Python esté disponible cada vez que un cluster se inicie.\n\n"
            "¿Qué enfoque logra mejor este objetivo?"
        ),
        "options": [
            "Usar un init script para instalar la librería durante el inicio del cluster.",
            "Instalar la librería cuando se ejecuta un notebook de Python.",
            "Instalar manualmente la librería en el driver node cada vez que el cluster arranca.",
            "Usar la Databricks CLI para subir los archivos de la librería al cluster después del inicio.",
        ],
        "answer": "Usar un init script para instalar la librería durante el inicio del cluster.",
        "explanation": (
            "Los init scripts se ejecutan automáticamente en cada nodo cada vez que el cluster arranca, asegurando que la librería esté "
            "consistentemente disponible antes de que cualquier job o notebook se ejecute.\n"
            "• La instalación por notebook solo aplica para la sesión de ese notebook.\n"
            "• La instalación manual requiere intervención humana en cada reinicio.\n"
            "• Las subidas por CLI después del arranque son propensas a errores y no se garantiza que se ejecuten antes de que comiencen los workloads."
        ),
    },
    {
        "exam": 2,
        "id": "q11_delta_check_constraint",
        "question": (
            "El equipo de data engineering tiene una tabla Delta llamada 'users'. Recientemente se ha añadido un constraint CHECK a la tabla mediante el siguiente comando:\n\n"
            "ALTER TABLE users\n"
            "ADD CONSTRAINT valid_age CHECK (age >= 0);\n\n"
            "El equipo intentó hacer un insert de un batch de nuevos registros en la tabla, pero había algunos registros con valores de edad negativos que provocaron que la escritura fallara debido a la violación del constraint.\n\n"
            "¿Qué afirmación describe el resultado de este batch insert?"
        ),
        "options": [
            "Ninguno de los registros ha sido insertado en la tabla.",
            "Todos los registros excepto aquellos que violan el constraint de la tabla han sido insertados. Los registros que violan el constraint han sido ignorados.",
            "Todos los registros excepto aquellos que violan el constraint han sido insertados. Los registros que violan el constraint han sido registrados en el transaction log.",
            "Solo los registros procesados antes de alcanzar el primer registro infractor han sido insertados en la tabla.",
        ],
        "answer": "Ninguno de los registros ha sido insertado en la tabla.",
        "explanation": "Las operaciones de escritura fallaron por la violación del constraint. Sin embargo, las garantías ACID en Delta Lake aseguran que todas las transacciones sean atómicas. Como se dice, o tendrán éxito o fallarán por completo. Así que en este caso, ninguno de estos registros ha sido insertado en la tabla, ni siquiera aquellos que no violan el constraint.",
    },
    {
        "exam": 2,
        "id": "q12_unit_testing_definition",
        "question": "¿Cuál de las siguientes afirmaciones describe correctamente el Unit Testing?",
        "options": [
            "Es un enfoque para verificar si cada feature de la aplicación funciona según los requerimientos de negocio.",
            "Es un enfoque para testear unidades individuales de código para determinar si siguen funcionando como se espera si se les hacen nuevos cambios en el futuro.",
            "Es un enfoque para testear la interacción entre subsistemas de una aplicación para asegurar que los módulos funcionan correctamente como grupo.",
            "Es un enfoque para medir la fiabilidad, velocidad, escalabilidad y capacidad de respuesta de una aplicación.",
        ],
        "answer": "Es un enfoque para testear unidades individuales de código para determinar si siguen funcionando como se espera si se les hacen nuevos cambios en el futuro.",
        "explanation": "El Unit testing es un enfoque para testear unidades de código, como funciones. Así, si haces cambios en ellos en el futuro, puedes usar tests para determinar si siguen funcionando como esperas. Los assertions se usan en los unit tests para comprobar si ciertas suposiciones siguen siendo verdaderas mientras desarrollas código.",
    },
    {
        "exam": 2,
        "id": "q13_ldp_pipeline_permissions",
        "question": "¿Cuál de las siguientes opciones ordena correctamente los permisos del Lakeflow Declarative pipeline de menor a mayor privilegio?",
        "options": [
            "CAN VIEW < CAN MANAGE < CAN RUN",
            "CAN RUN < CAN VIEW < CAN MANAGE",
            "CAN VIEW < CAN RUN < CAN MANAGE",
            "CAN MANAGE < CAN VIEW < CAN RUN",
        ],
        "answer": "CAN VIEW < CAN RUN < CAN MANAGE",
        "explanation": "En las jerarquías de permisos, menor privilegio a mayor privilegio significa empezar con el acceso mínimo y terminar con el control total. CAN VIEW permite solo ver los detalles del pipeline, la Spark UI y los logs del driver. CAN RUN permite ejecutar el pipeline pero no modificarlo. CAN MANAGE permite el control total, incluyendo ejecutar, editar, eliminar y gestionar permisos.",
    },
    {
        "exam": 2,
        "id": "q14_ldp_expectation_functions",
        "question": (
            "Un data engineer junior ha sido asignado para implementar validación de data quality en un Lakeflow Declarative Pipeline (LDP). Añadió varias expectation functions para asegurar que los datasets entrantes cumplan ciertas condiciones antes de ser procesados más a fondo. Después de que el ingeniero junior enviara un pull request, un data engineer senior empezó a revisar el código y se dio cuenta de que una de las llamadas a la función utilizadas para la validación no era válida según la documentación de Databricks.\n\n"
            "Como parte de la revisión, el ingeniero senior quiere asegurarse de que todas las expectation functions utilizadas en el pipeline sean válidas según la documentación de Databricks.\n\n"
            "¿Cuál de las siguientes llamadas a funciones NO es una expectation function válida en Lakeflow Declarative Pipelines?"
        ),
        "options": [
            "dlt.expect_or_fail",
            "dlt.expect_or_drop",
            "dlt.expect_or_warn",
            "dlt.expect",
        ],
        "answer": "dlt.expect_or_warn",
        "explanation": "dlt.expect_or_warn no es una expectation function soportada en Lakeflow Declarative Pipelines (LDP). LDP soporta las siguientes expectation functions: dlt.expect (escribe los registros no válidos en el target conservando la semántica), dlt.expect_or_drop (descarta las filas no válidas antes de escribirlas en el target), y dlt.expect_or_fail (hace fallar la actualización si se produce una violación).",
    },
    {
        "exam": 2,
        "id": "q15_table_partitioning_benefits",
        "question": (
            "El equipo de data engineering tiene un pipeline que ingiere datos de origen Kafka a una tabla bronze Multiples. Esta tabla Delta está particionada basándose en las columnas topic y mes.\n\n"
            "Un data engineer nota que el topic 'user_activity' contiene Información de Identificación Personal (PII) que necesita ser eliminada cada dos meses basándose en el Service-Level Agreement (SLA) de la compañía.\n\n"
            "¿Qué afirmación describe cómo el table partitioning puede ayudar a cumplir con este requisito?"
        ),
        "options": [
            "El table partitioning permite que las queries delete aprovechen los límites de las particiones.",
            "El table partitioning no permite el time travel a los datos PII después del borrado.",
            "El table partitioning permite borrar archivos inmediatamente sin ejecutar el comando VACUUM.",
            "El table partitioning reduce la latencia de la query al eliminar archivos de datos grandes.",
        ],
        "answer": "El table partitioning permite que las queries delete aprovechen los límites de las particiones.",
        "explanation": "El particionado en columnas datetime se puede aprovechar a la hora de borrar datos antiguos de la tabla. Por ejemplo, puedes decidir borrar los datos de meses anteriores. En este caso, el borrado de archivos se hará de forma limpia a lo largo de los límites de las particiones. Del mismo modo, los datos se podrían archivar o guardar un backup en los límites de la partición enviándolos a una capa de storage más económica. Esto supone un gran ahorro en el storage en cloud.",
    },
    {
        "exam": 2,
        "id": "q16_scd_type_identification",
        "question": (
            "Dadas las siguientes dos versiones de una tabla Delta Lake antes y después de un update:\n\n"
            "Before:\n"
            "user_id | name | address | city | current | start_date | end_date\n"
            "001 | John | 4, Oxford Street | London | True | 1/1/2022 | NULL\n"
            "002 | Sarah | 99, Victor Hugo Street | Paris | True | 1/1/2022 | NULL\n\n"
            "After:\n"
            "user_id | name | address | city | current | start_date | end_date\n"
            "001 | John | 4, Oxford Street | London | False | 1/1/2022 | 25/1/2023\n"
            "002 | Sarah | 99, Victor Hugo Street | Paris | True | 1/1/2022 | NULL\n"
            "001 | John | 25, King's Road | London | True | 25/1/2023 | NULL\n\n"
            "¿Qué SCD Type es esta tabla?"
        ),
        "options": [
            "SCD Type 0",
            "SCD Type 2",
            "SCD Type 1",
            "Es una combinación de SCDs Type 0 y Type 2",
        ],
        "answer": "SCD Type 2",
        "explanation": "En una tabla SCD Type 2, se añade un nuevo registro con los valores de datos modificados, y este nuevo registro pasa a ser el registro activo actual, mientras que el antiguo se marca como ya no activo. Por tanto, el SCD Type 2 retiene todo el historial de valores.",
    },
    {
        "exam": 2,
        "id": "q17_ldp_quarantine_pattern",
        "question": (
            "Un equipo de data engineering está construyendo un LDP pipeline para limpiar y validar data products haciendo streaming desde múltiples sources. Mientras gestionan los registros en la tabla bronze_products, algunas tablas contienen valores de price no válidos, concretamente algunos precios son cero o negativos, lo que viola las reglas de negocio.\n\n"
            "Para gestionar este problema, implementaron el siguiente código LDP:\n\n"
            "@dlt.table\n"
            "def silver_products():\n"
            "    return (\n"
            '        dlt.read_stream("bronze_products")\n'
            '        .filter("price > 0")\n'
            "    )\n\n"
            "@dlt.table\n"
            "def quarantine_products():\n"
            "    return (\n"
            '        dlt.read_stream("bronze_products")\n'
            '        .filter("price <= 0")\n'
            "    )\n\n"
            "¿Cuál de las siguientes describe correctamente el resultado de ejecutar este pipeline?"
        ),
        "options": [
            'Todos los registros se cargan en la tabla silver_products, con un flag "quarantine_products" que indica si el precio es válido o no.',
            "Los registros con precios positivos se cargan en la tabla silver_products, mientras que los registros con precios cero o negativos se cargan en la tabla quarantine_products.",
            'Todos los registros se actualizan en la tabla bronze_products con un flag "quarantine_products" que indica si el precio es válido o no.',
            "Los registros con precios positivos se cargan en la tabla silver_products, mientras que los registros con precios cero o negativos se borran de la tabla bronze_products.",
        ],
        "answer": "Los registros con precios positivos se cargan en la tabla silver_products, mientras que los registros con precios cero o negativos se cargan en la tabla quarantine_products.",
        "explanation": "Este LDP pipeline usa un patrón común para poner en cuarentena registros mediante la creación de una segunda tabla que almacena los registros no válidos. La tabla silver_products solo filtra los registros que cumplen la condición price > 0, mientras que los registros con precios cero o negativos se seleccionan hacia la tabla quarantine_products. La tabla bronze_products original permanece sin cambios, y no se borra ningún registro de ella.",
    },
    {
        "exam": 2,
        "id": "q18_query_profile_panels",
        "question": "¿Cuál de los siguientes paneles NO está incluido en la vista del Query Profile dentro de Databricks SQL?",
        "options": [
            "Details",
            "Query text",
            "Top operators",
            "Query source",
        ],
        "answer": "Query source",
        "explanation": "El Query Profile proporciona tres paneles: Details, Top operators, y Query text, que dan información sobre las métricas de ejecución de la query, las principales operaciones implicadas, y el código SQL real.",
    },
    {
        "exam": 2,
        "id": "q19_databricks_objects_for_analytics",
        "question": (
            "Un analista de datos de una compañía de retail se encarga de generar reportes diarios sobre el performance de ventas en múltiples regiones y categorías de productos. La compañía ingiere continuamente datos de transacciones de sus tiendas online usando Lakeflow Declarative Pipelines. El analista necesita crear un objeto relacional que pueda precomputar de manera eficiente agregaciones a nivel de negocio, como los ingresos totales, el valor promedio de pedido y las unidades vendidas por categoría, de modo que el reporting y los dashboards en el downstream puedan acceder a los datos rápidamente sin recalcularlos cada vez.\n\n"
            "¿Cuál de los siguientes objetos es el más adecuado para este caso de uso?"
        ),
        "options": [
            "Streaming table",
            "Materialized view",
            "Standard view",
            "Temporary view",
        ],
        "answer": "Materialized view",
        "explanation": "El objeto más adecuado para este caso de uso es un materialized view porque permite al data analyst precomputar y almacenar agregaciones a nivel de negocio, de modo que los reportes y dashboards posteriores puedan acceder a los resultados rápidamente sin recalcularlos cada vez, a diferencia de una vista temporal o estándar, que o bien solo existen para la sesión o bien requieren computación repetida, y a diferencia de un streaming table, que está diseñada para procesar streams de eventos crudos en tiempo real en lugar de dashboards pre-agregados.",
    },
    {
        "exam": 2,
        "id": "q20_lakehouse_federation",
        "question": (
            "Una empresa de servicios financieros gestiona carteras de inversión de clientes altamente confidenciales en bases de datos Oracle y mantiene datos de mercado transaccionales en Microsoft SQL Server. Debido a regulaciones HIPAA, los datos deben permanecer en su lugar y no duplicarse ni exportarse innecesariamente. Sin embargo, los equipos de auditoría interna necesitan generar informes unificados a través de ambos sistemas en Databricks manteniendo un estricto control de accesos.\n\n"
            "¿Qué solución debería utilizar el equipo de datos para permitir la consulta directa (querying) a estas bases de datos sin duplicar los datos?"
        ),
        "options": [
            "Shallow clone",
            "Lakehouse Federation",
            "Delta Sharing",
            "Partner Connect",
        ],
        "answer": "Lakehouse Federation",
        "explanation": "Lakehouse Federation es una feature en Databricks que permite a los usuarios hacer queries directamente a datos en bases de datos externas, como Oracle y SQL Server, sin necesidad de replicación de datos, ingesta o movimiento. Proporciona una capa analítica unificada sobre múltiples fuentes de datos y permite queries federadas, donde los datos de varias plataformas se pueden combinar en una sola vista lógica. Esto se alinea perfectamente con las necesidades de la empresa.",
    },
    {
        "exam": 2,
        "id": "q21_multi_task_job_creation",
        "question": "¿Cuál de los siguientes métodos NO permite a los data engineers crear un multi-task job en Databricks?",
        "options": [
            "Databricks Asset Bundles (DABs)",
            "Lakeflow Declarative Pipelines",
            "REST API",
            "Workspace UI",
        ],
        "answer": "Lakeflow Declarative Pipelines",
        "explanation": "El método que no permite a los data engineers crear un multi-task job en Databricks son los Lakeflow Declarative Pipelines. Éstos están destinados a definir lógica de transformación de forma declarativa dentro de un pipeline, y pueden funcionar como un solo task dentro de un job en lugar de crear multi-task jobs por sí mismos. Por otro lado, la Workspace UI y la REST API sí te permiten definir jobs con múltiples tasks, y Databricks Asset Bundles (DABs) pueden empaquetar y desplegar definiciones de multi-task jobs.",
    },
    {
        "exam": 2,
        "id": "q22_dbutils_secrets_get",
        "question": (
            "Un data engineer está trabajando en un proyecto que requiere integrar datos desde un API endpoint externo en un Databricks workspace. Por razones de seguridad, decidieron no hardcodear la clave de la API directamente en sus notebooks. En su lugar, usaron Databricks Secrets para almacenar de manera segura y gestionar credenciales sensibles, de la siguiente forma:\n\n"
            "databricks secrets create-scope api_scope\n"
            "databricks secrets put-secret api_scope api_key\n\n"
            "Ahora quieren leer el API key para usarla en el endpoint externo desde un Databricks notebook.\n\n"
            "¿Cuál de las siguientes líneas de código permite al data engineer lograr este objetivo?"
        ),
        "options": [
            'api_key = dbutils.secrets.get("api_scope", "api_key")',
            'api_key = dbutils.secrets.get("api_key", "api_scope")',
            'api_key = dbutils.secrets.read("api_scope", "api_key")',
            'api_key = dbutils.secrets.read("api_key", "api_scope")',
        ],
        "answer": 'api_key = dbutils.secrets.get("api_scope", "api_key")',
        "explanation": "dbutils.secrets.get(scope, key) se usa para recuperar un secreto de forma segura, donde el primer argumento es el nombre del scope (api_scope) y el segundo argumento es el secret key (api_key).",
    },
    {
        "exam": 2,
        "id": "q23_streaming_deduplication_issue",
        "question": (
            "Un data engineer junior está usando el siguiente código para desduplicar (de-duplicate) datos de streaming en crudo e insertarlos en una tabla target Delta:\n\n"
            "(\n"
            "    spark.readStream\n"
            '    .table("raw_data")\n'
            '    .dropDuplicates(["order_id", "order_timestamp"])\n'
            "    .writeStream\n"
            '    .option("checkpointLocation", "/data/checkpoints")\n'
            '    .toTable("orders")\n'
            ")\n\n"
            "Un data engineer senior señaló que este enfoque no es suficiente para tener registros únicos en la tabla target cuando hay registros duplicados que llegan tarde (late-arriving).\n\n"
            "¿Cuál de las siguientes podría explicar la observación del data engineer senior?"
        ),
        "options": [
            "También se necesita una window function para aplicar la desduplicación para cada intervalo sin solapamiento.",
            "Los nuevos registros también deben desduplicarse con respecto a los datos insertados previamente en la tabla.",
            "También se necesita Watermarking para seguir únicamente la información de estado en un margen de tiempo en el que esperamos que los registros puedan retrasarse.",
            "También se requiere una ranking function para asegurar que se procesen solo los registros más recientes."
        ],
        "answer": "Los nuevos registros también deben desduplicarse con respecto a los datos insertados previamente en la tabla.",
        "explanation": "Para realizar la desduplicación en el stream, usamos la función dropDuplicates() para eliminar registros duplicados dentro de cada nuevo micro-batch. Además, debemos asegurarnos de que los registros a insertar no estén ya presentes en la tabla Delta de destino. Podemos lograr esto utilizando un merge del tipo insert-only.",
    },
    {
        "exam": 2,
        "id": "q24_structured_streaming_retry_policy",
        "question": "Para jobs en producción con Structured Streaming, ¿cuál de las siguientes retry policies (políticas de reintentos) se recomienda utilizar?",
        "options": [
            "No Retries, con Unlimited Concurrent Runs",
            "No Retries, con 1 Maximum Concurrent Run",
            "Unlimited Retries, con Unlimited Concurrent Runs",
            "Unlimited Retries, con 1 Maximum Concurrent Run",
        ],
        "answer": "Unlimited Retries, con 1 Maximum Concurrent Run",
        "explanation": "Para reiniciar queries en streaming en caso de fallo, se recomienda configurar los jobs de Structured Streaming con la siguiente configuración: Retries: Establecido en Unlimited. Maximum concurrent runs: Establecido en 1. Debe haber solo una instancia de cada query activa concurrentemente.",
    },
    {
        "exam": 2,
        "id": "q25_autoloader_merge_schema",
        "question": (
            "Un data engineer quiere usar Auto Loader para ingestar input data en una tabla target, y evolucionar automáticamente el schema de la tabla cuando se detecten nuevos campos.\n\n"
            "Utilizan la siguiente query con spark.readStream:\n\n"
            "spark.readStream\n"
            '    .format("cloudFiles")\n'
            '    .option("cloudFiles.format", "json")\n'
            '    .option("cloudFiles.schemaLocation", checkpointPath)\n'
            "    .load(source_path)\n"
            "    .writeStream\n"
            '    .option("checkpointLocation", checkpointPath)\n'
            "    .option(_________________)\n"
            '    .start("target_table")\n\n'
            "¿Qué opción rellena correctamente el espacio en blanco para cumplir con el requerimiento especificado?"
        ),
        "options": [
            'option("cloudFiles.schemaEvolutionMode","addNewColumns")',
            "schema(schema_definition, mergeSchema=True)",
            'option("mergeSchema","True")',
            'option("cloudFiles.mergeSchema","True")',
        ],
        "answer": 'option("mergeSchema","True")',
        "explanation": "La evolución del schema (Schema evolution) es una feature que permite añadir nuevos campos detectados a la tabla. Se activa añadiendo option(\"mergeSchema\",\"True\") a tu comando Spark write o writeStream.",
    },
    {
        "exam": 2,
        "id": "q26_autoloader_schema_rescue_mode",
        "question": (
            "Un data engineer está diseñando un pipeline de ingesta streaming usando Auto Loader. "
            "El requisito es que el pipeline nunca debe fallar por cambios de schema, pero debe capturar "
            "cualquier nueva columna que llegue en los datos para una inspección posterior.\n\n"
            "¿Qué configuración debería usar el engineer?"
        ),
        "options": [
            "rescue",
            "none",
            "addNewColumns",
            "failOnNewColumns",
        ],
        "answer": "rescue",
        "explanation": "El modo 'rescue' asegura que el schema no evolucione, de modo que el stream no fallará si se añaden columnas nuevas. En su lugar, cualquier nueva columna se almacena en la columna de datos rescatados (rescued data column), lo que permite inspeccionarlas más tarde sin interrumpir el stream. Esto cumple el requisito de mantener el stream en funcionamiento sin fallos y capturar los nuevos elementos del schema.",
    },
    {
        "exam": 2,
        "id": "q27_cdf_change_data_folder",
        "question": (
            "El equipo de data engineering mantiene una tabla Delta Lake de tipo SCD Type 1. Un data engineer notó una carpeta llamada '_change_data' en el directorio de la tabla, y quiere entender para qué se usa esta carpeta.\n\n"
            "¿Cuál de las siguientes describe el propósito de esta carpeta?"
        ),
        "options": [
            "La feature de Optimized Writes está habilitada en la tabla. La carpeta '_change_data' es la ubicación donde se almacenan los datos optimizados.",
            "La feature de CDF está habilitada en la tabla. La carpeta '_change_data' es donde se almacena la data de CDF.",
            "Todas las tablas SCD Type 1 tienen la carpeta '_change_data' para rastrear los updates aplicados a los datos de la tabla.",
            "La carpeta '_change_data' es el directorio por defecto para rastrear la evolución en la definición del schema.",
        ],
        "answer": "La feature de CDF está habilitada en la tabla. La carpeta '_change_data' es donde se almacena la data de CDF.",
        "explanation": "Databricks registra la información de los cambios para las operaciones UPDATE, DELETE y MERGE en la carpeta _change_data dentro del directorio de la tabla. Los archivos en la carpeta _change_data siguen la política de retención de la tabla. Por tanto, si se ejecuta el comando VACUUM, también se eliminan los datos del change data feed (CDF).",
    },
    {
        "exam": 2,
        "id": "q28_stream_static_join_behavior",
        "question": (
            "Un data engineer tiene un streaming job que actualiza una tabla Delta llamada 'user_activities' con los resultados de un join entre un streaming Delta table 'activity_logs' y una static Delta table 'users'.\n\n"
            "Notaron que el añadir nuevos usuarios a la tabla 'users' no dispara automáticamente updates en la tabla 'user_activities', incluso cuando hubo actividades para esos usuarios en la tabla 'activity_logs'.\n\n"
            "¿Cuál de las siguientes opciones probablemente explique este problema?"
        ),
        "options": [
            "La parte estática del stream-static join dirige este proceso de join solo en modo batch.",
            "La tabla de usuarios (users) debe refrescarse con el comando REFRESH TABLE para cada micro-batch de este join.",
            "La porción streaming de este stream-static join dirige el proceso de join. Solo los nuevos datos que aparezcan en la parte streaming del join desencadenarán el procesamiento.",
            "Este stream-static join no guarda el estado (stateful) por defecto, a menos que configuren la propiedad de spark delta.statefulStreamStaticJoin a true.",
        ],
        "answer": "La porción streaming de este stream-static join dirige el proceso de join. Solo los nuevos datos que aparezcan en la parte streaming del join desencadenarán el procesamiento.",
        "explanation": "En un stream-static join, la parte de streaming de este join dirige el proceso. Así que, solo la nueva data que aparezca en el lado del streaming desencadenará el procesamiento. Por contra, añadir nuevos registros en la tabla estática no activará automáticamente actualizaciones en los resultados del stream-static join.",
    },
    {
        "exam": 2,
        "id": "q29_databricks_run_target_flag",
        "question": (
            "Un data engineer es responsable de gestionar y orquestar los workflows de datos en el entorno de Databricks de su organización. Han desplegado un job llamado events_process_job usando Databricks Asset Bundles. Para ejecutar este job, el engineer lanza el siguiente comando desde su terminal:\n\n"
            "databricks bundle run events_process_job\n\n"
            "Al observar el comando, un data engineer senior sugiere que podrían mejorar el proceso de ejecución añadiendo la opción -t al ejecutar el comando.\n\n"
            "¿Cuál de las siguientes explicaciones detalla el propósito principal de esta opción?"
        ),
        "options": [
            "Para lanzar un dry run del job sin procesar realmente los datos.",
            "Para habilitar un logging temporal durante la ejecución del job.",
            "Para seleccionar el entorno objetivo (target environment) para la ejecución del job.",
            "Para especificar el tamaño de cluster objetivo (target cluster size) para la ejecución del job.",
        ],
        "answer": "Para seleccionar el entorno objetivo (target environment) para la ejecución del job.",
        "explanation": "El propósito principal de la opción -t en el comando databricks bundle run es seleccionar el entorno target para la ejecución del job. Cuando un data engineer lanza un job usando Databricks Asset Bundles, el flag -t (o --target) les permite especificar en qué entorno—como development, staging o production—debe ejecutarse el job. Esto ayuda a asegurar que los jobs se ejecuten usando los recursos y datasets correctos para ese entorno, evitando cambios accidentales en procesamientos del contexto equivocado, y optimizando los flujos de despliegue en múltiples entornos.",
    },
    {
        "exam": 2,
        "id": "q30_pandas_udf_apache_arrow",
        "question": "¿Cuál de los siguientes formatos se utiliza por las Pandas UDFs para mejorar el performance de ejecución en Apache Spark?",
        "options": [
            "Apache Iceberg",
            "Apache Arrow",
            "Delta Lake",
            "Apache Kafka",
        ],
        "answer": "Apache Arrow",
        "explanation": "Apache Arrow proporciona un eficiente formato de datos en memoria en columnas que permite a Spark transferir datos entre la JVM y los procesos Python sin la sobrecarga de la serialización. Esto acelera significativamente el procesamiento de datos en comparación con los formatos estándar basados en filas.",
    },
    {
        "exam": 2,
        "id": "q31_delta_sharing_cloudflare_r2",
        "question": "Una organización planea utilizar Delta Sharing para permitir el acceso a grandes datasets por parte de múltiples clientes distribuidos en AWS, Azure y GCP. Un data engineer senior ha recomendado migrar el dataset a Cloudflare R2 object storage antes de iniciar el proceso de data sharing.\n\n¿Qué beneficio ofrece Cloudflare R2 en esta configuración de Delta Sharing?",
        "options": [
            "Proporciona una API estándar para evitar el vendor lock-in con los proveedores de cloud.",
            "Ofrece soporte integrado para datos en streaming con checkpointing automático.",
            "Elimina el coste de salida (egress cost) de los proveedores de la nube para transferencias de datos de salida.",
            "Proporciona soporte nativo para el data masking dinámico.",
        ],
        "answer": "Elimina el coste de salida (egress cost) de los proveedores de la nube para transferencias de datos de salida.",
        "explanation": "Cloudflare R2 elimina completamente los egress costs, lo cual minimiza drásticamente los gastos normalmente incurridos al compartir datos entre múltiples proveedores de cloud y equipos analíticos externos a través del protocolo de Delta Sharing.",
    },
    {
        "exam": 2,
        "id": "q32_delta_sharing_supported_assets",
        "question": "¿Cuál de las siguientes implementaciones de Delta Sharing permite compartir Unity Catalog Volumes, Unity Catalog Models y notebooks además de las tablas Delta estáticas?",
        "options": [
            "La implementación administrada por el cliente del servidor open source de Delta Sharing.",
            "El protocolo de Databricks-to-Databricks sharing.",
            "El protocolo de Databricks open sharing.",
            "Ninguna de las opciones listadas soporta compartir estos activos.",
        ],
        "answer": "El protocolo de Databricks-to-Databricks sharing.",
        "explanation": "El protocolo Databricks-to-Databricks sharing soporta de forma nativa la compartición de activos avanzados de Unity Catalog, como Volumes, Models y Notebooks. Por el contrario, el protocolo open sharing y las implementaciones gestionadas por el cliente se limitan a tablas Delta estáticas.",
    },
    {
        "exam": 2,
        "id": "q33_sql_grant_all_privileges",
        "question": "¿Cuál de los siguientes comandos puede usar un data engineer para conceder permisos completos al equipo de RRHH (HR team) sobre la tabla employees?",
        "options": [
            "GRANT SELECT, MODIFY, CREATE, READ_METADATA ON TABLE employees TO hr_team",
            "GRANT ALL PRIVILEGES ON TABLE employees TO hr_team",
            "GRANT ALL PRIVILEGES ON TABLE hr_team TO employees",
            "GRANT FULL PRIVILEGES ON TABLE employees TO hr_team",
        ],
        "answer": "GRANT ALL PRIVILEGES ON TABLE employees TO hr_team",
        "explanation": "En Databricks SQL, la palabra clave ALL PRIVILEGES se utiliza para conceder todos los permisos disponibles de un objeto a un usuario o a un grupo. FULL PRIVILEGES es una sintaxis gramaticalmente incorrecta.",
    },
    {
        "exam": 2,
        "id": "q34_ldp_violation_clause_default",
        "question": "Un data engineer ha definido el siguiente data quality constraint en un pipeline LDP:\n\nCONSTRAINT valid_id EXPECT (id IS NOT NULL) _______________\n\n¿Qué cláusula rellena correctamente el espacio en blanco de modo que los registros que violan este constraint sean escritos en la tabla target, pero reportados en las métricas?",
        "options": [
            "ON VIOLATION ADD ROW",
            "ON VIOLATION NULL",
            "No hay necesidad de añadir la cláusula ON VIOLATION. Por defecto, los registros que violan el constraint se mantienen y se reportan como inválidos en las métricas del pipeline.",
            "ON VIOLATION WARNING",
        ],
        "answer": "No hay necesidad de añadir la cláusula ON VIOLATION. Por defecto, los registros que violan el constraint se mantienen y se reportan como inválidos en las métricas del pipeline.",
        "explanation": "Por defecto, si no se suministra ninguna cláusula adicional como ON VIOLATION DROP ROW u ON VIOLATION FAIL UPDATE, un constraint EXPECT registra la infracción en las métricas pero transmite la fila directamente de forma segura al destino target.",
    },
    {
        "exam": 2,
        "id": "q35_table_partitioning_pii_security",
        "question": "El equipo de data engineering quiere crear una tabla Delta bronze de múltiples fases (multiphase) a partir de una fuente Kafka. La tabla Delta tiene el siguiente schema:\n\nkey BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG\n\nDado que la columna 'value' contiene Información de Identificación Personal (PII) para algunos topics, el equipo desea aplicar Listas de Control de Acceso (ACLs) en los límites de la partición (partition boundaries) para restringir el acceso a estos datos PII.\n\nBasándonos en el esquema anterior y en el requisito especificado, ¿qué columna es un buen candidato para el partitioning?",
        "options": [
            "key",
            "timestamp",
            "topic",
            "partition",
        ],
        "answer": "topic",
        "explanation": "Particionar la tabla por la columna 'topic' aísla los registros relevantes que contienen datos PII en directorios específicos, lo cual permite límites de control de acceso granulares y una gestión selectiva de la información histórica.",
    },
    {
        "exam": 2,
        "id": "q36_delta_shallow_clone_vacuum",
        "question": "Un data engineer junior ha creado la tabla 'orders_backup' como una copia de la tabla 'orders'. Recientemente, el equipo empezó a recibir un error al consultar orders_backup indicando que algunos data files ya no están presentes. Los transaction logs de la tabla orders muestran una ejecución reciente del comando VACUUM.\n\n¿Cuál de las siguientes explica cómo creó la tabla orders_backup el data engineer?",
        "options": [
            "La tabla orders_backup fue creada a través de la funcionalidad SHALLOW CLONE de Delta Lake a partir de la tabla orders.",
            "La tabla orders_backup fue creada a través de la funcionalidad DEEP CLONE de Delta Lake a partir de la tabla orders.",
            "La tabla orders_backup fue creada usando un statement CTAS desde la tabla orders.",
            "La tabla orders_backup fue creada usando un statement CRAS desde la tabla orders.",
        ],
        "answer": "La tabla orders_backup fue creada a través de la funcionalidad SHALLOW CLONE de Delta Lake a partir de la tabla orders.",
        "explanation": "Un SHALLOW CLONE solo copia los transaction logs de Delta sin duplicar los data files subyacentes. Si se ejecuta una operación VACUUM sobre la tabla padre (source), los archivos históricos referenciados por el shallow clone son podados (pruned), resultando en errores por archivos faltantes al consultar el clone.",
    },
    {
        "exam": 2,
        "id": "q37_pyspark_assert_data_frame_equal",
        "question": "Un data engineer está probando un pipeline de transformación que añade una nueva columna a un DataFrame existente. Quieren asegurar que el DataFrame resultante coincida con el output esperado.\n\n¿Cuál de las siguientes funciones puede usar un data engineer para verificar la igualdad?",
        "options": [
            "assertDataFrameEqual(actual_df, expected_df)",
            "assertEqual(actual_df, expected_df)",
            "verifyEquality(actual_df, expected_df)",
            "assert(actual_df == expected_df)",
        ],
        "answer": "assertDataFrameEqual(actual_df, expected_df)",
        "explanation": "En PySpark, assertDataFrameEqual es la función de utilidad nativa diseñada para comparar schemas y datos a nivel de fila entre dos DataFrames dentro de frameworks de testing.",
    },
    {
        "exam": 2,
        "id": "q38_merge_into_limitation_multiple_matches",
        "question": "¿Cuál de las siguientes se considera una limitación al usar el comando MERGE INTO?",
        "options": [
            "Merge no soporta la eliminación de registros. Solo soporta operaciones de upsert.",
            "El merge no puede llevarse a cabo si una sola fila del origen (source row) hace match e intenta modificar múltiples filas target en la tabla.",
            "El merge no puede llevarse a cabo si múltiples filas del origen hacen match e intentan modificar la misma fila target en la tabla.",
            "El merge no se puede realizar en streaming jobs a menos que utilice Watermarking.",
        ],
        "answer": "El merge no puede llevarse a cabo si múltiples filas del origen hacen match e intentan modificar la misma fila target en la tabla.",
        "explanation": "Una operación MERGE fallará con un error si múltiples registros del source entrante coinciden con una única fila de la tabla target, ya que crea ambigüedad sobre qué fila del origen debe tener prioridad para la actualización.",
    },
    {
        "exam": 2,
        "id": "q39_alter_table_file_retention_properties",
        "question": "Un equipo de data engineering gestiona una tabla Delta llamada orders. Quieren asegurar que pueden acceder a la data histórica de la tabla mediante time travel durante el mismo tiempo que la retención por defecto del transaction log de Delta Lake.\n\n¿Cuál de los siguientes comandos cumple este requisito?",
        "options": [
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 365 days")',
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 30 days")',
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 7 days")',
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 90 days")',
        ],
        "answer": 'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 30 days")',
        "explanation": "Por defecto, los Delta log files se preservan durante 30 días. Para igualar esta línea temporal y permitir búsquedas de time travel en filas físicas antes de la limpieza permanente vía VACUUM, la propiedad de configuración delta.deletedFileRetentionDuration debe ser fijada a 30 días.",
    },
    {
        "exam": 2,
        "id": "q40_autoloader_binary_format_glob",
        "question": "Se ha asignado a un data engineer la tarea de ingestar archivos de imágenes de rayos X de tipo .JPEG en una tabla Delta usando Auto Loader.\n\n¿Cuál de los siguientes fragmentos de código (code snippets) puede usar el data engineer para lograr esta tarea?",
        "options": [
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'image') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'binaryFile') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'binary') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'files') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
        ],
        "answer": "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'binaryFile') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
        "explanation": "Para manejar datos no estructurados de imagen con Auto Loader, la opción cloudFiles.format debe establecerse en 'binaryFile'. La opción pathGlobFilter extrae únicamente archivos que terminen con la extensión especificada.",
    },
    {
        "exam": 2,
        "id": "q41_drop_table_managed_outcome",
        "question": (
            "El equipo de data engineering tiene una tabla Delta Lake creada con la siguiente query:\n\n"
            "CREATE TABLE customers_clone\n"
            "AS SELECT * FROM customers;\n\n"
            "Un data engineer quiere eliminar la tabla (drop) con la siguiente query:\n\n"
            "DROP TABLE customers_clone;\n\n"
            "¿Qué afirmación describe el resultado de ejecutar este comando drop?"
        ),
        "options": [
            "Solo se eliminarán los metadatos de la tabla del catalog, mientras que los data files se conservarán en el storage.",
            "Ocurrirá un error, ya que la tabla es un deep clone de la tabla customers.",
            "La tabla no se eliminará hasta que se ejecute el comando VACUUM.",
            "Se eliminarán tanto los metadatos de la tabla como los data files.",
        ],
        "answer": "Se eliminarán tanto los metadatos de la tabla como los data files.",
        "explanation": "La tabla fue creada sin definir explícitamente una cláusula externa LOCATION, lo que significa que se ha registrado como una managed table (tabla gestionada). Hacer un drop de una managed table elimina tanto su definición de metadatos en el catalog como todos los archivos de datos en crudo subyacentes del almacenamiento.",
    },
    {
        "exam": 2,
        "id": "q42_predictive_optimization_benefits",
        "question": "¿Cuáles dos de las siguientes opciones describen los beneficios de habilitar la predictive optimization en managed tables dentro de Unity Catalog?\n\nElige 2 respuestas.",
        "options": [
            "Mejora la privacidad de los datos cifrándolos automáticamente al escribirlos y enmascarando columnas sensibles.",
            "Mejora el rendimiento de las queries al recolectar estadísticas según los datos se van escribiendo en la tabla.",
            "Reduce el coste general prediciendo el uso de storage y reasignando datos a través de diferentes tiers.",
            "Mejora el data profiling prediciendo automáticamente los valores que faltan en las columnas de la tabla.",
            "Simplifica el mantenimiento al ejecutar automáticamente operaciones de mantenimiento en la tabla.",
        ],
        "answer": [
            "Mejora el rendimiento de las queries al recolectar estadísticas según los datos se van escribiendo en la tabla.",
            "Simplifica el mantenimiento al ejecutar automáticamente operaciones de mantenimiento en la tabla.",
        ],
        "explanation": "La Predictive Optimization ejecuta automáticamente tareas operacionales como OPTIMIZE y VACUUM para asegurar una estructura de storage óptima. También recopila estadísticas de datos en la escritura, potenciando la query optimization sin requerir labor manual de data engineering.",
    },
    {
        "exam": 2,
        "id": "q43_delta_sharing_with_history",
        "question": (
            "Un data analyst de una empresa retail compartió una gran tabla Delta con una empresa de análisis externa usando Delta Sharing sin history. Sin embargo, la compañía notó retrasos en la ejecución al realizar queries sobre los datos compartidos.\n\n"
            "Un data engineer senior sugirió usar el siguiente comando para compartir los datos con history, con el fin de mejorar el rendimiento de la query:\n\n"
            "ALTER SHARE sales_share ADD TABLE products WITH HISTORY;\n\n"
            "¿Qué beneficio se obtiene al usar WITH HISTORY?"
        ),
        "options": [
            "Aprovecha el caching de disco del servidor de Delta Sharing, lo que da como resultado un rendimiento comparable al del acceso directo a las tablas source.",
            "Replica la tabla para balancear las solicitudes de vacancia a través del servidor de Delta Sharing.",
            "Aprovecha las credenciales de seguridad temporales del cloud storage, limitadas (scoped-down) al directorio root de la tabla Delta compartida por el proveedor.",
            "Realiza un shallow clone de la tabla para compartir únicamente el transaction log de la tabla.",
        ],
        "answer": "Aprovecha las credenciales de seguridad temporales del cloud storage, limitadas (scoped-down) al directorio root de la tabla Delta compartida por el proveedor.",
        "explanation": "Compartir una tabla WITH HISTORY permite a los destinatarios realizar queries de time-travel. Esto optimiza las rutas de ejecución permitiendo a Databricks aprovechar de forma segura las credenciales cloud pre-autenticadas y scoped-down directamente desde la capa de object storage subyacente del proveedor.",
    },
    {
        "exam": 2,
        "id": "q44_alter_table_row_filter",
        "question": (
            "Un equipo de data engineering gestiona una tabla Delta Lake en Unity Catalog llamada employees, con columnas id, name, salary, y region. Quieren aplicar un row filter sobre esta tabla de forma que solo los miembros del equipo de RRHH (hr_team) puedan acceder a todos los registros. Si un usuario que no es de HR hace una query a la tabla, solo debería mostrar registros de la región de Francia (FR). Para lograrlo, implementaron la siguiente user-defined function:\n\n"
            "CREATE FUNCTION fn_filter(region STRING)\n"
            "RETURN IS_ACCOUNT_GROUP_MEMBER('hr_team') OR region = 'FR';\n\n"
            "¿Cuál de los siguientes comandos puede usar el equipo para aplicar esta función como row filter a la tabla?"
        ),
        "options": [
            "ALTER TABLE employees SET ROW FILTER fn_filter;",
            "ALTER TABLE employees ALTER COLUMN region SET ROW FILTER fn_filter;",
            "SET ROW FILTER fn_filter ON TABLE employees COLUMN region",
            "ALTER TABLE employees SET ROW FILTER fn_filter ON (region);",
        ],
        "answer": "ALTER TABLE employees SET ROW FILTER fn_filter ON (region);",
        "explanation": "La sintaxis SQL correcta para añadir una función de row filter existente a una tabla target en Unity Catalog es ALTER TABLE <table_name> SET ROW FILTER <function_name> ON (<column_names>).",
    },
    {
        "exam": 2,
        "id": "q45_cdf_batch_read_behavior",
        "question": (
            "Dada la siguiente query sobre la tabla Delta 'customers' en la que Change Data Feed está habilitado:\n\n"
            "spark.read\n"
            '  .option("readChangeFeed", "true")\n'
            '  .option("startingVersion", 1)\n'
            '  .table("customers")\n'
            "  .write\n"
            '  .option("append")\n'
            '  .saveAsTable("customers_orders")\n\n'
            "¿Qué afirmación describe el resultado de esta query cada vez que se ejecuta?"
        ),
        "options": [
            "El historial completo de los registros actualizados sobrescribirá (overwrite) la tabla target en cada ejecución.",
            "Los registros recién actualizados sobrescribirán la tabla target.",
            "Los registros recién actualizados se añadirán (append) a la tabla target.",
            "El historial completo de registros actualizados se añadirá a la tabla target en cada ejecución, lo que provocará entradas duplicadas.",
        ],
        "answer": "El historial completo de registros actualizados se añadirá a la tabla target en cada ejecución, lo que provocará entradas duplicadas.",
        "explanation": "Como esto se ejecuta como una lectura en batch (spark.read) con una startingVersion fija de 1 en lugar de una lectura streaming, cada ejecución escanea y procesa todo el ledger histórico de cambios desde la versión 1 en adelante, agregando (appending) de forma continua registros redundantes a la tabla target.",
    },
    
    {
        "exam": 2,
        "id": "q46_window_tumbling_streaming",
        "question": (
            "Un data engineer tiene la siguiente query en streaming con un espacio en blanco:\n\n"
            "spark.readStream\n"
            "    .table(\"orders_source\")\n"
            "    .groupBy(\n"
            "        ________(\"order_timestamp\", \"15 minutes\")\n"
            "    )\n"
            "    .agg(\n"
            "        count(\"order_id\").alias(\"orders_count\"),\n"
            "        avg(\"quantity\").alias(\"avg_quantity\")\n"
            "    )\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", \"/path/to/checkpoint\")\n"
            "    .table(\"orders_stats\")\n\n"
            "Quieren calcular la cantidad de pedidos y la cantidad media por cada intervalo de 15 minutos sin solapamiento.\n\n"
            "¿Qué opción rellena correctamente el espacio en blanco para cumplir con este requisito?"
        ),
        "options": [
            "window(\"order_timestamp\", \"15 minutes\")",
            "trigger(processingTime=\"15 minutes\")",
            "withWatermark(\"order_timestamp\", \"15 minutes\")",
            "withWindow(\"order_timestamp\", \"15 minutes\")",
        ],
        "answer": "window(\"order_timestamp\", \"15 minutes\")",
        "explanation": "La función window() de PySpark permite agrupar datos basados en ventanas de tiempo. Al especificar únicamente la duración de la ventana ('15 minutes') sin definir un tiempo de deslizamiento secundario (slideDuration), se crean automáticamente ventanas disjuntas o no superpuestas (tumbling windows). Las otras opciones presentadas no cumplen este propósito o no existen en la API estándar.",
    },
    {
        "exam": 2,
        "id": "q47_liquid_clustering_optimization",
        "question": "Un equipo de data engineering está trabajando en una tabla de eventos de actividad de usuario (user activity events) guardada en Unity Catalog. Las queries a menudo implican filtros en múltiples columnas como user_id y event_date.\n\n¿Qué técnica de data layout debería implementar el equipo para evitar costosos table scans?",
        "options": [
            "Utilizar particionado sobre la columna event_date.",
            "Utilizar indexación Z-order sobre user_id.",
            "Utilizar particionado sobre la columna user_id, junto a la indexación Z-order sobre la columna event_date.",
            "Utilizar liquid clustering sobre la combinación de user_id y event_date.",
        ],
        "answer": "Utilizar liquid clustering sobre la combinación de user_id y event_date.",
        "explanation": "Liquid Clustering es la técnica moderna de optimización del diseño de datos en Databricks que reemplaza al particionamiento clásico y a Z-Order. Permite realizar agrupaciones de datos dinámicas y eficientes basadas en múltiples columnas (como combinaciones de alta y baja cardinalidad), evitando escaneos completos de tabla innecesarios y facilitando la optimización incremental.",
    },
    {
        "exam": 2,
        "id": "q48_ctas_statement_delta",
        "question": (
            "Un data engineer tiene la siguiente sentencia CTAS en un notebook SQL adjunto a un all-purpose cluster:\n\n"
            "CREATE TABLE course_students\n"
            "AS SELECT c.course_id, c.course_name, s.student_id, s.student_name\n"
            "FROM courses c\n"
            "LEFT JOIN (\n"
            "    SELECT student_id, s.student_name, e.course_id\n"
            "    FROM enrollments e\n"
            "    JOIN students s\n"
            "    ON e.student_id = s.student_id\n"
            ") s\n"
            "ON c.course_id = s.course_id\n"
            "WHERE c.active = true\n\n"
            "¿Qué afirmación describe a la tabla course_students resultante?"
        ),
        "options": [
            "Es una tabla a nivel de sesión (session scoped). La sentencia SELECT será ejecutada en la creación de la tabla, pero su output será guardado en caché de la sesión Spark actual activa.",
            "Es una tabla Delta Lake. La sentencia SELECT será ejecutada en la creación de la tabla, y su output será guardado en formato Delta en el storage subyacente.",
            "Es una tabla virtual que no tiene datos físicos. La sentencia SELECT se ejecutará cada vez que la tabla course_students sea consultada.",
            "Es una tabla a nivel de cluster. La sentencia SELECT será ejecutada al crearse la tabla, pero su output se almacenará en la memoria del cluster activo actual.",
        ],
        "answer": "Es una tabla Delta Lake. La sentencia SELECT será ejecutada en la creación de la tabla, y su output será guardado en formato Delta en el storage subyacente.",
        "explanation": "En Databricks SQL, las sentencias CREATE TABLE AS SELECT (CTAS) crean por defecto tablas persistentes basadas en el formato estructurado Delta Lake. Al ejecutarse la instrucción, la consulta SELECT se procesa por completo para guardar físicamente el resultado en el almacenamiento subyacente asignado.",
    },
    {
        "exam": 2,
        "id": "q49_delta_deletion_vectors",
        "question": "¿Cuál de las siguientes sentencias describe mejor a los deletion vectors en Delta Lake?",
        "options": [
            "Estructuras de metadatos que rastrean qué filas dentro de un data file han sido borradas de forma lógica sin necesidad de reescribir físicamente el archivo.",
            "Archivos temporales que guardan filas borradas hasta que son archivadas en una partición separada llamada \"_deletion_log\".",
            "Estructuras de datos que han eliminado permanentemente las filas borradas en todos los data files dentro de la tabla Delta Lake.",
            "Índices que aceleran las consultas en las filas borradas guardando sus ubicaciones físicas directamente en los Unity Catalog volumes.",
        ],
        "answer": "Estructuras de metadatos que rastrean qué filas dentro de un data file han sido borradas de forma lógica sin necesidad de reescribir físicamente el archivo.",
        "explanation": "Los deletion vectors son estructuras de metadatos integradas en Delta Lake que registran de forma lógica las filas eliminadas o modificadas dentro de un archivo de datos de tipo Parquet. Esto optimiza el rendimiento al evitar la necesidad inmediata de reescribir físicamente todo el archivo durante operaciones UPDATE, DELETE o MERGE.",
    },
    {
        "exam": 2,
        "id": "q50_multitask_job_partial_failure",
        "question": "Dado un job multi-task donde Task 2 y Task 3 dependen de Task 1:\n\nSi hay un error en el notebook asociado al Task 1, ¿qué afirmación describe el resultado de la ejecución de este job?",
        "options": [
            "Task 1 fallará completamente. Tasks 2 y 3 serán saltados (skipped).",
            "Task 1 fallará completamente. Tasks 2 y 3 correrán de forma exitosa.",
            "Task 1 fallará de forma parcial. Tasks 2 y 3 correrán de forma exitosa.",
            "Task 1 fallará de forma parcial. Tasks 2 y 3 serán saltados (skipped).",
        ],
        "answer": "Task 1 fallará de forma parcial. Tasks 2 y 3 serán saltados (skipped).",
        "explanation": "En Databricks Workflows, el fallo de una tarea basada en un notebook se cataloga como un fallo parcial debido a que las celdas y operaciones previas al error se completan y confirman correctamente en el almacenamiento. No obstante, por integridad del flujo secuencial, todas las tareas dependientes (Tareas 2 y 3) se omitirán automáticamente (skipped).",
    },
    {
        "exam": 2,
        "id": "q51_streaming_default_trigger",
        "question": (
            "Dada la siguiente query en Structured Streaming:\n\n"
            "spark.readStream\n"
            "    .table(\"orders\")\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .table(\"output_table\")\n\n"
            "¿Cuál de los siguientes es el trigger interval para esta query?"
        ),
        "options": [
            "La query correrá en modo batch procesando todos los datos disponibles a la vez, y después el trigger se detiene.",
            "Cada medio minuto",
            "Cada media hora",
            "Cada medio segundo",
        ],
        "answer": "Cada medio segundo",
        "explanation": "Por defecto, si no se define explícitamente ninguna política de Trigger en una consulta de Structured Streaming en Apache Spark, el motor procesa micro-lotes de forma continua con un tiempo de espera de procesamiento mínimo de 500 milisegundos, lo que equivale a procesar datos 'cada medio segundo'.",
    },
    {
        "exam": 2,
        "id": "q52_change_data_feed_cdc",
        "question": "¿Cuál de las siguientes opciones describe mejor esta característica?\n\n\"Una característica integrada en Delta Lake que permite generar automáticamente CDC feeds sobre las tablas Delta Lake.\"",
        "options": [
            "Auto Optimize",
            "Slowly Changing Dimension (SCD)",
            "Change Data Feed (CDF)",
            "Optimized writes",
        ],
        "answer": "Change Data Feed (CDF)",
        "explanation": "Change Data Feed (CDF) es una característica integrada de manera nativa en Delta Lake que registra de forma automática los cambios producidos a nivel de fila (inserciones, actualizaciones, borrados) con metadatos asociados, sirviendo como base nativa para flujos de captura de datos modificados (CDC).",
    },
    {
        "exam": 2,
        "id": "q53_foreachbatch_spark_session",
        "question": (
            "Un data engineer usa la lógica de foreachBatch para hacer un upsert de los datos en una tabla Delta objetivo.\n\n"
            "La función a llamar en cada nuevo procesamiento del microbatch se muestra a continuación con un espacio en blanco:\n\n"
            "def upsert_data(microBatchDF, batch_id):\n"
            "    microBatchDF.createOrReplaceTempView(\"updates_microbatch\")\n"
            "\n"
            "    sql_query = \"\"\"\n"
            "        MERGE INTO stats_order t\n"
            "        USING updates_microbatch s\n"
            "        ON t.item_id = s.item_id AND t.item_timestamp=s.item_timestamp\n"
            "        WHEN MATCHED THEN UPDATE SET *\n"
            "        WHEN NOT MATCHED THEN INSERT *\n"
            "    \"\"\"\n"
            "\n"
            "    ________________\n\n"
            "¿Qué opción rellena correctamente el hueco para ejecutar la sql query dentro de la función en un cluster con el Databricks Runtime por encima de 10.5?"
        ),
        "options": [
            "microBatchDF._jdf.sparkSession().sql(sql_query)",
            "microBatchDF.sparkSession.sql(sql_query)",
            "microBatchDF.sql(sql_query)",
            "spark.sql(sql_query)",
        ],
        "answer": "microBatchDF.sparkSession.sql(sql_query)",
        "explanation": "A partir de Databricks Runtime 10.5, cuando se trabaja con la lógica foreachBatch dentro de Structured Streaming, no se debe hacer uso de la variable global de sesión 'spark'. En su lugar, es necesario invocar la sesión de Spark activa directamente asociada al DataFrame del micro-lote actual utilizando la propiedad 'microBatchDF.sparkSession'.",
    },
    {
        "exam": 2,
        "id": "q54_broadcast_join_function",
        "question": (
            "Un data engineer quiere optimizar la siguiente operación de join permitiendo que el dataFrame más pequeño se mande a todos los nodos de ejecución en el cluster:\n\n"
            "target_df = left_df.join(right_df, \"user_id\")\n\n"
            "¿Cuál de las siguientes funciones puede ser utilizada para marcar a un dataFrame como lo suficientemente pequeño como para encajar en la memoria de todos los executors?"
        ),
        "options": [
            "pyspark.sql.functions.shuffle",
            "pyspark.sql.functions.explode",
            "pyspark.sql.functions.distribute",
            "pyspark.sql.functions.broadcast",
        ],
        "answer": "pyspark.sql.functions.broadcast",
        "explanation": "La función broadcast() le indica explícitamente al optimizador de consultas de Spark que marque un DataFrame específico para que sea copiado en su totalidad y distribuido hacia la memoria de cada uno de los nodos ejecutores en el clúster, permitiendo un Broadcast Hash Join rápido y sin necesidad de realizar un shuffle generalizado.",
    },
    {
        "exam": 2,
        "id": "q55_delta_cdf_vacuum_retention",
        "question": "¿Cuál de las siguientes declaraciones es cierta respecto a la política de retención en Delta Lake CDF?",
        "options": [
            "Los archivos de datos CDF pueden ser purgados al ejecutar el comando VACUUM CHANGES.",
            "La ejecución del comando VACUUM en la tabla no borra los datos CDF a menos que la cláusula CASCADE esté en true.",
            "La ejecución del comando VACUUM sobre la tabla borra también los datos de CDF.",
            "La ejecución del comando VACUUM sobre la tabla no borra los datos CDF.",
        ],
        "answer": "La ejecución del comando VACUUM sobre la tabla borra también los datos de CDF.",
        "explanation": "Los archivos generados por Change Data Feed (CDF) se almacenan en el directorio interno '_change_data' dentro de la estructura de la tabla Delta. Al ejecutar el comando convencional VACUUM sobre la tabla, los archivos CDF que queden fuera del periodo de retención configurado también se purgarán de manera automática junto con los archivos de datos obsoletos.",
    },
    {
        "exam": 2,
        "id": "q56_delta_sharing_cross_cloud",
        "question": "Una compañía de venta retail guarda sus datos de venta dentro de tablas Delta en un Unity Catalog de Databricks. Tienen la necesidad de compartir ciertas tablas de forma segura con una empresa externa de auditoría, la cual emplea Databricks en un cloud provider distinto.\n\n¿Cuál de las siguientes opciones permite lograr esto sin replicación de datos?",
        "options": [
            "Un esquema externo en Unity Catalog",
            "Shallow clone",
            "Databricks Connect",
            "Databricks-to-Databricks Delta Sharing",
        ],
        "answer": "Databricks-to-Databricks Delta Sharing",
        "explanation": "Databricks-to-Databricks Delta Sharing habilita el intercambio directo y seguro de datos entre distintas organizaciones o entornos de Databricks, incluso si se encuentran alojados en proveedores de nube diferentes (cross-cloud). Permite al receptor consultar la información en tiempo real sin necesidad de replicar físicamente los archivos o las bases de datos origen.",
    },
    {
        "exam": 2,
        "id": "q57_sha256_hash_length_behavior",
        "question": (
            "Un data engineer desea almacenar contraseñas de manera segura en una managed table de Unity Catalog. Tienen que aplicar un hash sobre las contraseñas usando sha2(password, 256) antes de almacenarlas. Para asegurar que se almacena adecuadamente, el data engineer tiene que fijar un constraint sobre la longitud de la columna de manera que quepa todo el hash completo.\n\n"
            "El ingeniero hace una prueba del hash de las contraseñas \"sparkV23\" y \"ApacheSpark117\".\n\n"
            "¿Qué advertirá el data engineer sobre la longitud del hash devuelto?"
        ),
        "options": [
            "Que el hash de \"sparkV23\" será más corto que el hash de \"ApacheSpark117\".",
            "Ambos hashes tendrán idéntica longitud puesto que ésta depende de la cantidad de números.",
            "Que el hash de \"ApacheSpark117\" será más corto que el hash de \"sparkV23\".",
            "Ambos hashes tendrán la misma longitud, a pesar de las diferencias en el tamaño de origen."
        ],
        "answer": "Ambos hashes tendrán la misma longitud, a pesar de las diferencias en el tamaño de origen.",
        "explanation": "Las funciones criptográficas de la familia SHA-256 de longitud fija (empleadas con sha2(..., 256)) generan siempre un resultado final con un tamaño constante de 256 bits (representado comúnmente en formato de cadena de texto hexadecimal fija de 64 caracteres), con total independencia de las dimensiones o volumen del texto original que se introduzca en la entrada.",
    },
    {
        "exam": 2,
        "id": "q58_cron_syntax_job_scheduling",
        "question": "¿Cuál de las siguientes describe a la sintaxis Cron dentro de Databricks Jobs?",
        "options": [
            "Es una expresión para representar una planificación compleja en el job que se puede definir de manera programada.",
            "Es una expresión para representar a las ejecuciones simultáneas máximas en el job.",
            "Es una expresión para señalar los límites de tiempo máximo del run de un job.",
            "Es una expresión para marcar una política de intentos múltiples en un job."
        ],
        "answer": "Es una expresión para representar una planificación compleja en el job que se puede definir de manera programada.",
        "explanation": "La sintaxis estándar Cron dentro de Databricks Workflows se utiliza como mecanismo formal para modelar programáticamente planificaciones y esquemas de ejecución de tareas complejas e intermitentes (por ejemplo, definir ejecuciones repetitivas en periodos horarios alternos o días específicos de la semana).",
    },
    {
        "exam": 2,
        "id": "q59_minimal_permissions_attach_notebook",
        "question": "¿Cuál de las opciones indica los permisos básicos indispensables que un data engineer debe tener a la hora de arrancar un cluster previamente creado, y conectarle o enlazarle (attach) un notebook?",
        "options": [
            "El privilegio \"Can Manage\" sobre el cluster.",
            "El privilegio \"Can Attach To\" sobre el cluster.",
            "El privilegio \"Can Restart\" sobre el cluster.",
            "Creación de cluster habilitada + los privilegios \"Can Restart\" en el cluster.",
        ],
        "answer": "El privilegio \"Can Restart\" sobre el cluster.",
        "explanation": "El privilegio o permiso 'Can Restart' (Puede reiniciar) engloba y hereda implícitamente todas las capacidades básicas de nivel inferior como 'Can Attach To' (Puede asociar a). En consecuencia, para arrancar un clúster ya existente que se encuentra detenido y vincular un notebook con el fin de trabajar, el permiso mínimo necesario e indispensable es 'Can Restart'.",
    },


    # ---------------- EXAM 3 ----------------
    {
        "exam": 3,
        "id": "q01_dlt_expect_or_drop",
        "question": (
            "Un data engineer define la siguiente función en su pipeline LDP:\n\n"
            "@dlt.table\n"
            '@dlt.expect_or_drop("quantity_within_range", "quantity BETWEEN 0 AND 1000")\n'
            '@dlt.expect_or_drop("recent_transaction", "transaction_date >= \'2025-01-01\'")\n'
            '@dlt.expect_or_drop("valid_transaction", "transaction_id IS NOT NULL")\n'
            "def silver_sales():\n"
            '    return dlt.read_stream("bronze_sales")\n\n'
            "¿Cuál de las siguientes opciones describe correctamente el resultado de ejecutar este pipeline?"
        ),
        "options": [
            "Las filas que violan las expectations definidas son eliminadas de ambas tablas.",
            "Las filas que violan las expectations definidas son filtradas (filtered out), y solo las filas válidas se escriben en silver_sales.",
            "Las filas que violan las expectations definidas son eliminadas de la tabla bronze_sales.",
            "Las filas que violan las expectations definidas son transmitidas por streaming a la tabla silver_sales."
        ],
        "answer": "Las filas que violan las expectations definidas son filtradas (filtered out), y solo las filas válidas se escriben en silver_sales.",
        "explanation": (
            "La función expect_or_drop es una regla de enforcement de data quality en LDP (anteriormente conocido como DLT).\n"
            "La parte expect define el constraint de calidad (ej., 'quantity BETWEEN 0 AND 1000').\n"
            "La parte or_drop dicta la acción a tomar cuando se viola la expectation: significa que la fila infractora se descarta (es filtrada) y no se escribirá en la tabla objetivo (silver_sales).\n\n"
            "En este ejemplo, solo las filas que pasen con éxito las tres expectations definidas (quantity_within_range, recent_transaction, y valid_transaction) se incluirán en la tabla silver_sales. Las filas que fallen cualquiera de ellas serán descartadas."
        ),
    },
    {
        "exam": 3,
        "id": "q02_multitask_job_notifications",
        "question": (
            "Un equipo de data engineering gestiona un job multi-task donde cada task puede reintentarse múltiples veces. Han notado que no se envían notificaciones del job cuando se reintentan las tasks fallidas.\n\n"
            "¿Cuál de las siguientes configuraciones asegurará que se reciba una notificación de fallo por cada task que haya fallado?"
        ),
        "options": [
            "Implementar una lógica de notificación personalizada dentro de cada task.",
            "Crear un job separado por cada task con reintentos a nivel de job.",
            "Deshabilitar todos los task retries (reintentos de tareas) para depender de las notificaciones a nivel de job.",
            "Utilizar notificaciones a nivel de task en la definición del job."
        ],
        "answer": "Utilizar notificaciones a nivel de task en la definición del job.",
        "explanation": (
            "En un job multi-task, las notificaciones se pueden configurar en dos niveles:\n\n"
            "1. Notificaciones a nivel de job (Job-level): Se disparan solo cuando el job entero tiene éxito o falla. Esto significa que si una task individual falla pero se reintenta con éxito, no se enviará ninguna notificación hasta que el job global se complete o falle.\n"
            "2. Notificaciones a nivel de task (Task-level): Se disparan por cada evento de task, incluyendo los fallos o terminaciones exitosas.\n\n"
            "Configurar notificaciones a nivel de task asegura que se enviará una notificación por cada task fallida, incluso si es posteriormente reintentada."
        ),
    },
    {
        "exam": 3,
        "id": "q03_delta_sharing_external_vendor",
        "question": (
            "Un data engineer de una compañía de logística global necesita compartir determinados datasets y notebooks de análisis con un proveedor (vendor) de analíticas externo, quien también es cliente de Databricks. Los datos están almacenados como tablas Delta en Unity Catalog, y el proveedor no tiene acceso a la cuenta de Databricks de la empresa.\n\n"
            "¿Cuál es la manera más efectiva y segura de compartir los datos y notebooks con el proveedor externo?"
        ),
        "options": [
            "Compartir las tablas Delta usando Delta Sharing, y enviar todos los notebooks juntos en un único archivo DBC.",
            "Compartir las tablas Delta usando Delta Sharing, y publicar los notebooks como páginas HTML programáticamente.",
            "Compartir las tablas Delta y los notebooks usando Delta Sharing.",
            "Compartir las tablas Delta usando Delta Sharing, y dar acceso a cada notebook mediante su funcionalidad de colaboración (collaboration feature) incorporada."
        ],
        "answer": "Compartir las tablas Delta y los notebooks usando Delta Sharing.",
        "explanation": (
            "Databricks-to-Databricks Delta Sharing permite el intercambio seguro, abierto y en tiempo real de tablas, notebooks, volúmenes (volumes) y Modelos de ML con otros clientes de Databricks. Esto no requiere que ellos tengan acceso a tu mismo workspace o cuenta de Databricks. Con Unity Catalog, la compañía puede asegurar un control de accesos de grano fino (fine-grained) y gobernanza. Este enfoque es eficiente, escalable y cumple con los estándares de seguridad empresariales."
        ),
    },
    {
        "exam": 3,
        "id": "q04_dabs_github_authentication",
        "question": (
            "Un data engineer quiere usar Databricks Asset Bundles (DABs) en un pipeline de automatización CI/CD totalmente automatizado en GitHub.\n\n"
            "¿Cuál es el método recomendado de autenticación para DABs hacia el workspace de Databricks target en este escenario?"
        ),
        "options": [
            "Personal Access Token para un service principal de Databricks.",
            "Client secret de OAuth para un service principal de Databricks.",
            "OAuth token federation para un service principal de Databricks.",
            "Personal Access Token para un usuario administrador."
        ],
        "answer": "OAuth token federation para un service principal de Databricks.",
        "explanation": (
            "Databricks Asset Bundles son una característica del Databricks CLI. Para que la CLI se autentique con Databricks sin gestionar secretos de Databricks directamente, se recomienda utilizar OAuth token federation para un service principal de Databricks en el workspace destino."
        ),
    },
    {
        "exam": 3,
        "id": "q05_rest_api_jobs_list",
        "question": (
            "Un equipo de data engineering desea automatizar la monitorización de jobs y mejorar la observabilidad mediante la extracción de los jobs disponibles en el workspace de producción en Databricks usando REST API.\n\n"
            "¿Cuál de las siguientes llamadas a la API REST cumple con este requisito?"
        ),
        "options": [
            "Enviar una solicitud POST al endpoint '/api/2.0/jobs/list'",
            "Enviar una solicitud POST al endpoint '/api/2.1/jobs/list'",
            "Enviar una solicitud GET al endpoint '/api/2.0/jobs/list'",
            "Enviar una solicitud GET al endpoint '/api/2.1/jobs/list'"
        ],
        "answer": "Enviar una solicitud GET al endpoint '/api/2.1/jobs/list'",
        "explanation": (
            "Enviar solicitudes GET al endpoint '/api/2.1/jobs/list' te permite recuperar los jobs disponibles en un workspace de Databricks."
        ),
    },
    {
        "exam": 3,
        "id": "q06_databricks_cli_jobs_list_runs",
        "question": (
            "¿Cuál de los siguientes comandos de Databricks CLI le permite a un data engineer listar todos los runs de un job que comenzaron en o después de una hora específica?"
        ),
        "options": [
            "databricks jobs list-runs --job-id <job-id> --time-from <time-value>",
            "databricks jobs list-runs --job-id <job-id> --start <time-value>",
            "databricks jobs list-runs --job-id <job-id> --start-time <time-value>",
            "databricks jobs list-runs --job-id <job-id> --start-time-from <time-value>"
        ],
        "answer": "databricks jobs list-runs --job-id <job-id> --start-time-from <time-value>",
        "explanation": (
            "El comando correcto de Databricks CLI que permite a un data engineer listar todos los runs de un job que comenzaron en o después de una hora concreta es: databricks jobs list-runs --job-id <job-id> --start-time-from <time-value>.\n\n"
            "'--start-time-from' es el parámetro adecuado que se usa para filtrar los job runs basándose en su tiempo de inicio (start time) en la CLI de Databricks."
        ),
    },
    {
        "exam": 3,
        "id": "q07_trigger_multitask_job_tools",
        "question": "¿Cuál de las siguientes herramientas NO permite a los data engineers realizar un trigger programático de un job run multi-task?",
        "options": [
            "Command-line interface (CLI)",
            "Workspace Jobs UI",
            "REST API",
            "Databricks SDKs"
        ],
        "answer": "Workspace Jobs UI",
        "explanation": (
            "La Workspace Jobs UI no permite a los data engineers realizar el trigger de manera programática para un job run multi-task. Es una interfaz gráfica que requiere interacción manual y no puede utilizarse para una ejecución de jobs automatizada o basada en código.\n\n"
            "En cambio, la REST API, Command-line interface (CLI) y los SDKs de Databricks proporcionan formas programáticas para ejecutar jobs."
        ),
    },
    {
        "exam": 3,
        "id": "q08_databricks_secrets_plain_text",
        "question": (
            "Un data engineer escuchó recientemente que los usuarios que tienen acceso a Databricks Secrets podrían ser capaces de mostrar los valores de los secrets en notebooks.\n\n"
            "¿Cuál de los siguientes podría ser un workaround (solución alternativa) para imprimir el valor de un secret de Databricks en plain text (texto plano)?"
        ),
        "options": [
            'db_password = dbutils.secrets.get("prod-scope", "db-password")\ndisplay(db_password)',
            'db_password = dbutils.secrets.get("prod-scope", "db-password")\nprint(db_password, redacted=False)',
            'db_password = dbutils.secrets.get("prod-scope", "db-password", redacted=False)\nprint(db_password)',
            'db_password = dbutils.secrets.get("prod-scope", "db-password")\nfor char in db_password:\n    print(char)'
        ],
        "answer": 'db_password = dbutils.secrets.get("prod-scope", "db-password")\nfor char in db_password:\n    print(char)',
        "explanation": (
            "Databricks censura (redact) los valores de los secrets que se leen usando dbutils.secrets.get(). Cuando se muestran en el output de una celda de notebook, los valores de los secrets son reemplazados por el string [REDACTED].\n\n"
            "Sin embargo, existe un workaround para poder imprimir los valores de los secrets de Databricks en texto sin formato iterando a través del secret e imprimiendo cada carácter."
        ),
    },
    {
        "exam": 3,
        "id": "q09_auto_compaction_zorder",
        "question": (
            "Un data engineer está usando las siguientes configuraciones spark en un pipeline para habilitar Optimized Writes y Auto Compaction:\n\n"
            'spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)\n'
            'spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)\n\n'
            "Además, quieren habilitar el Z-order indexing con Auto Compaction para aprovechar el data skipping en todas las tablas del pipeline.\n\n"
            "¿Cuál de las siguientes soluciones permite al data engineer completar esta tarea?"
        ),
        "options": [
            "No hay manera de habilitar Z-order indexing con Auto Compaction puesto que no soporta Z-Ordering.",
            'Usar spark.conf.set("spark.databricks.delta.autoZorder.enabled", True)',
            'Usar spark.conf.set("spark.databricks.delta.autoCompact.zorder.enabled", True)',
            "El Z-order indexing con Auto Compaction solo puede ser habilitado en cada tabla por separado usando:\n\nALTER TABLE table_name\nSET TBLPROPERTIES (delta.autoOptimize.zorder.enabled = true)"
        ],
        "answer": "No hay manera de habilitar Z-order indexing con Auto Compaction puesto que no soporta Z-Ordering.",
        "explanation": (
            "Auto Compaction no soporta Z-Ordering debido a que el Z-Ordering es significativamente más caro en recursos computacionales que solo hacer la compaction."
        ),
    },
    {
        "exam": 3,
        "id": "q10_lakehouse_declarative_pipelines_cdc",
        "question": (
            "Un data engineer usa el siguiente código para procesar datos CDC en Lakehouse Declarative Pipelines:\n\n"
            "CREATE OR REFRESH STREAMING TABLE cdc_target;\n\n"
            "APPLY CHANGES INTO LIVE.cdc_target\n"
            "FROM stream(users_cdc)\n"
            "KEYS (user_id)\n"
            "APPLY AS DELETE WHEN operation = 'DELETE'\n"
            "SEQUENCE BY sequenceNum\n"
            "COLUMNS * ;\n\n"
            "Después de ejecutar este código, el data engineer notó que se crearon dos objetos en el metastore además de la tabla users_target:\n\n"
            "* Una vista (view) llamada users_target.\n"
            "* Una tabla llamada __apply_changes_storage_users_target.\n\n"
            "¿Cuál de las siguientes explica correctamente el propósito de estos objetos?"
        ),
        "options": [
            "La view users_target es un snapshot materializado de los datos crudos, mientras que la tabla __apply_changes_storage_users_target guarda los logs de actividad del usuario por motivos de auditoría.",
            "La view users_target y la tabla __apply_changes_storage_users_target son objetos temporales creados para write optimization y son borrados automáticamente tras la ejecución del pipeline.",
            "Estos objetos se usan internamente para procesamiento CDC aprovechando el sequence_by, junto con otra información tal como 'tombstones' (marcas de borrado) y versiones requeridas para tratar los datos fuera de orden.",
            "La vista users_target es un índice virtual en la tabla target para acelerar las queries, y la __apply_changes_storage_users_target es un backup del índice users_target original."
        ],
        "answer": "Estos objetos se usan internamente para procesamiento CDC aprovechando el sequence_by, junto con otra información tal como 'tombstones' (marcas de borrado) y versiones requeridas para tratar los datos fuera de orden.",
        "explanation": (
            "En Lakehouse Declarative Pipelines, cuando creas un CDC flow con operaciones como APPLY AS DELETE y SEQUENCE BY, el sistema necesita una manera de manejar actualizaciones incrementales, eventos fuera de orden, actualizaciones y borrados, al tiempo que mantiene el orden correcto de eventos basados en la clave de secuencia.\n\n"
            "La tabla interna '__apply_changes_storage_users_target' guarda rastro de todos los cambios con la información necesaria del 'tombstone' (registro marcado para borrar) y metadatos de las versiones para asegurar que las llegadas tardes y eventos fuera del orden se aplican del modo adecuado. Esta tabla interna no es consultable directamente por el usuario, pero es fundamental para la fiabilidad y el control interno de CDC.\n\n"
            "La view 'users_target', por otro lado, es una capa lógica sobre esta tabla interna y sirve como vista consultable, que muestra el estado actualizado de la tabla como un snapshot impecable (clean, queryable snapshot). Lo que permite tener encapsulada y protegida la lógica del CDC."
        ),
    },
    {
        "exam": 3,
        "id": "q11_share_delta_table_sensitive_data",
        "question": (
            "El equipo de data engineering está buscando una solución simple para compartir una parte de una gran tabla Delta Lake con el equipo de data science. Solo necesitan compartir las columnas específicas de cada departamento en la tabla, pero con nombres diferentes. Además, existe una cantidad de información sensible que se debe filtrar (filter out) antes de ser compartida.\n\n"
            "¿Cuál de los siguientes objetos se puede crear para cumplir los requerimientos especificados?"
        ),
        "options": [
            "Una nueva Delta Table creada usando un SHALLOW CLONE desde la tabla existente.",
            "Una nueva Delta Table creada usando un DEEP CLONE desde la tabla existente.",
            "Una nueva Delta Table creada usando una instrucción CTAS desde la tabla existente.",
            "Una vista almacenada (stored view) de la tabla existente."
        ],
        "answer": "Una vista almacenada (stored view) de la tabla existente.",
        "explanation": (
            "La solución para este caso en concreto es la de fabricar una view (vista) a partir de la tabla principal, donde se pueda alterar los nombres en el SELECT y filtrar información sensible en la cláusula WHERE (filtering out)."
        ),
    },
    {
        "exam": 3,
        "id": "q12_materializing_results_cost_latency",
        "question": (
            "El equipo de data engineering tiene una tabla Silver de nombre 'sales_cleaned' adonde llegan appended de manera continua y casi en real-time los registros crudos.\n\n"
            "Pretenden configurar otra tabla en un capa Gold contra la de 'sales_cleaned' para llevar el cálculo del year-to-date (YTD) de los volúmenes de ventas. La tabla nueva constará de este esquema:\n\n"
            "country_code STRING, category STRING, ytd_total_sales FLOAT, updated TIMESTAMP\n\n"
            "Es suficiente que se calculen estas métricas únicamente una vez al día de forma agregada. Sin embargo, dado a la alta concurrencia por parte de los business teams, los ingenieros optan por recortar los costes generados por queries y la propia materialización del proceso.\n\n"
            "¿Cuál de las siguientes acciones y estrategias se amoldan en estos parámetros?"
        ),
        "options": [
            "Construir múltiples tablas particionadas por cada rama de analítica en lugar de una para evitar un embotellamiento.",
            "Configurar esta entidad en formato global temporary view dado que facilita un scope de ejecución superior entre clusters.",
            "Programar una batch job diaria/nocturna (nightly) que recalcule estos volúmenes y grabe los resultados sobrescribiendo un tabla Delta real.",
            "Definir esta identidad en formato regular view evadiendo recalcular (penalizar) todo a nivel físico con lo que ahorrarás dinero."
        ],
        "answer": "Programar una batch job diaria/nocturna (nightly) que recalcule estos volúmenes y grabe los resultados sobrescribiendo un tabla Delta real.",
        "explanation": (
            "Un data engineer debe tener un claro dominio acerca de los tradeoffs sobre una 'view' y una tabla puramente materializada con Delta y ver cómo cada diseño afecta un presupuesto computacional global.\n\n"
            "Considera usar un simple view (vista) si:\n"
            "- Su consulta es muy sencilla y ligera. Como se generan de manera instantánea o bajo demanda (on-demand compute), al invocar una vista enrevesada de JOIN y cálculos te expone a costes recurrentes inasumibles de procesamiento.\n"
            "- Tienes prisa y careces de storage real. Obvio, los Views son efímeros de guardarse a sí mismos.\n\n"
            "Considerar el uso de tabla (tabla Gold) si:\n"
            "- Multitudes de peticiones a nivel dashboarding demandan consultar los mismos pre-agregados o métricas sin recálculos subyacentes.\n"
            "- Debes incorporar historial persistente donde el stream/source original se recicla/borra con alta frecuencia."
        ),
    },
    {
        "exam": 3,
        "id": "q13_structured_streaming_late_data",
        "question": "¿Cuál de las siguientes técnicas puede usar un data engineer para gestionar data que llegue tarde (late-arriving data) en un Spark Structured Streaming?",
        "options": [
            "Windowing",
            "Checkpointing",
            "Watermarking",
            "Partitioning"
        ],
        "answer": "Watermarking",
        "explanation": (
            "Para un entorno de Structured Streaming, dispones del concepto fundamental the watermarking que habilita al motor a trackear la ventana y temporalidad del estado actual y especificar de una forma concreta durante cuánto se ignorará un retardo/llegada tardía (late data) frente a cuando esa ventana se considerará cerrada inamoviblemente."
        ),
    },
    {
        "exam": 3,
        "id": "q14_cluster_minimal_permissions",
        "question": "¿Cuál de las siguientes opciones describe el nivel de permiso básico obligatorio que ha de poseer un data engineer al menos para poder inicializar y terminar/abortar un entorno de ejecución (cluster) ya creado?",
        "options": [
            'Permiso de crear cluster general + privilegios "Can Restart" en dicho entorno',
            'Privilegio de "Can Manage" en el cluster de destino',
            'Privilegio de "Can Restart" en el cluster de destino',
            'Privilegio de "Can Attach To" en el cluster de destino'
        ],
        "answer": '"Can Restart" privilege on the cluster',
        "explanation": (
            "Dispones a grandes rasgos de permisos en dos ramas operacionales:\n"
            "1. Permiso global de crear clusters en general del usuario.\n"
            "2. Nivel granular/Cluster-level: el que aplica de forma aislada a cada cluster. Van desde el 'No permissions', sube al 'Can attach to' (conectar tu notebook a la marcha), luego a 'Can Restart' que provee poder físico para parar y reactivar. El 'Can manage' abarca modificaciones en sus trips y metadatos vitales."
        ),
    },
    {
        "exam": 3,
        "id": "q15_stream_static_joins_delta",
        "question": "¿Qué afirmación en relación al uso de static Delta tables dentro de procesos dinámicos tipo 'Stream-Static joins' es estrictamente correcta?",
        "options": [
            "Las tablas estáticas de Delta en este mix precisan invocarse vía comando REFRESH TABLE explícitamente en el inicio de cada microbatch del stream.",
            "Solamente la foto actual/latest version es cargada de la static tabla en el arranque del job/primer batch. Después el engine recicla un snapshot caheado permanentemente por ahorro de CPU.",
            "Deben mantenerse dichas estáticas reducidas en tamaño dado que los broadcast son mandatorios y excluyentes.",
            "La versión más reciente de la tabla estática de Delta es invocada en todos y cada uno de los instantes en los que el loop/microbatch recurre internamente al dataset estático."
        ],
        "answer": "La versión más reciente de la tabla estática de Delta es invocada en todos y cada uno de los instantes en los que el loop/microbatch recurre internamente al dataset estático.",
        "explanation": (
            "Estos modelos híbridos Stream-static joins se nutren de la agilidad ACID y control de versionado subyacente en el Delta Lake al dictaminar bajo el capó (under the hood) que las relecturas en cada ciclo o bucle recargarán garantizadamente de un modo dinámico y sin caches caducadas el commit más avanzado vigente en la ruta apuntada."
        ),
    },
    {
        "exam": 3,
        "id": "q16_spark_ui_stage_metrics",
        "question": "Dentro de un monitor tipo Spark UI, en los paneles o vistas detalladas de Stages, ¿Cuál de los siguientes términos analíticos NO forma parte oficial de lo observable?",
        "options": [
            "Duration (Duración)",
            "Spill (Disk and Memory) / Derramamientos a disco/ram",
            "DBU Cost",
            "GC time (Garbage collection delays)"
        ],
        "answer": "DBU Cost",
        "explanation": (
            "Buscando en la métrica desglosada por defecto del Stage (la página del Spark UI Stage detail), presenciarás variables que perfilan el peso y las cargas ejecutadas tales como:\n"
            "- Duration of tasks (Tiempos)\n"
            "- GC time (Tiempos en limpieza/Garbage JVM)\n"
            "- Spill Shuffle Memory / Disk Spill (Derramamientos a RAM/SSD)\n"
            "- and others.\n\n"
            "Lo que escapa radicalmente de este ámbito es la unidad DBU Cost, un acrónimo facturador puramente comercial propio y único de la plataforma general Databricks Unit (asociado a billing), el que carece de cualquier nexo o visualización interna intrínseca del motor de Apache Spark original."
        ),
    },
    {
        "exam": 3,
        "id": "q17_scheduling_notebooks_production",
        "question": "A grandes rasgos y desde el sentido común, ¿qué práctica debe suprimirse sin duda de los scripts y notebooks previo a pasarlos y encapsularlos como artefactos estáticos (jobs/tasks) para un workflow production pipeline en el día a día?",
        "options": [
            "Manejar magic commands (ej %sh %fs)",
            "Librerías import function calls",
            "Celdas de markdown o markups",
            "Acciones visuales o displays tipo df.display"
        ],
        "answer": "Acciones visuales o displays tipo df.display",
        "explanation": (
            "En pre-despliegue es aconsejable pasar un linter mental por un script en aras de refactorizar el notebook base. Se recomienda vehementemente neutralizar comandos inútiles que ralentizan el runtime en segundo plano tales como:\n"
            "- Acciones visualizadoras tipo display(), show(), y demás impresiones masivas.\n"
            "- Displays extra que uno solapó o dejó por puro testing ciego o depuración ad-hoc."
        ),
    },
    {
        "exam": 3,
        "id": "q18_scd_type_0_definition",
        "question": "¿Cuál es la frase que concuerda en su totalidad con el funcionamiento teórico/dogmático de un modelo Slowly Changing Dimension of Type 0 (SCD Type 0)?",
        "options": [
            "Consiste en una tabla propensa al cambio perpetuo donde el último dato sobrescribe despiadadamente al dato primario original de un modo irrecuperable.",
            "Consiste en poseer registros con flag/timestamp o fechas vigentes manteniendo el status vivo a lo largo de un eje cronológico.",
            "Consiste en un diseño perenne/fijo en el cual las filas pasadas no cambian.",
            "It's a table where no changes are allowed. (Donde su edición se penaliza al máximo o ni siquiera se formula)"
        ],
        "answer": "It's a table where no changes are allowed.",
        "explanation": (
            "Bajo un modelo rígido Type 0 SCD, los atributos son perpetuos o pasivos ante cualquier input novedoso o actualización posterior y jamás admiten ediciones en un entorno natural productivo. Sirva como ejemplos fijos como los de código ISO o un Lookup/Mapping table estático puro."
        ),
    },
    {
        "exam": 3,
        "id": "q19_delta_lake_file_statistics",
        "question": "¿Cuál declaración con respecto a las particularidades internas del Delta Lake File Statistics es ERRÓNEA?",
        "options": [
            "Los campos anidados no se contabilizan para el dictamen del conteo de las primeras 32 top columns de forma intrínseca.",
            "Tales rastros/métricas actúan como punta de lanza en favor de un salto (data skipping) de lotes al procesar queries altamente restrictivas/selectivas.",
            "A su libre albedrío, un sistema Delta Lake registra estadísticos predefinidos nativos localizados de pleno dentro del registro de transacciones anexando cada nuevo Parquet.",
            "La recolección es estéril/inútil si se encauzan por completo frente a una columna genérica tipo STRING colapsada por variables sumamente caóticas, es decir, altísima cardinalidad."
        ],
        "answer": "Los campos anidados no se contabilizan para el dictamen del conteo de las primeras 32 top columns de forma intrínseca.",
        "explanation": (
            "Se equivocan. El sistema Delta Lake compila sí o sí y autogenera metadatos a gran escala empujándolos dentro del archivo delta transaction log por un cada archivo parquet volcado, escudriñando para ello un máximo de las 32 origin columns por tabla base. Se computan en bloque (count as equal/valid) las jerarquías anidadas nested structs.\n\n"
            "Ejemplo visual: Contar 4 estructuraciones raíz conformadas a base de 8 subniveles cada una ya suman tu listón base de las 32 referidas."
        ),
    },
    {
        "exam": 3,
        "id": "q20_insert_only_merge",
        "question": "¿Cuál es la sintaxis (command lines) orientada a la ingeniería de forma unívoca a materializar un merge acotado bajo la consigna singular 'insert-only merge'?",
        "options": [
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN MATCHED\n    INSERT *",
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN MATCHED\n    INSERT *\nWHEN NOT MATCHED\n    INSERT *",
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN NOT MATCHED\n    INSERT *",
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN NOT MATCHED\n    UPDATE *"
        ],
        "answer": "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN NOT MATCHED\n    INSERT *",
        "explanation": (
            "A la hora de aplicar un append deduplicador (insert-only merge), el framework te demanda estructurar los pases así:\n\n"
            "MERGE INTO target_table\n"
            "USING source_table\n"
            "ON merge_condition\n"
            "WHEN NOT MATCHED\n"
            "    THEN INSERT *\n\n"
            "No te preocupas de codificar ninguna cláusula colateral en caso de match exacto, bastará eludir o prescindir tácitamente de un bloque 'MATCHED' lo cual ignorará limpiamente las filas ya presentes o solapadas. Se incorporan tan sólo del Source al Target los registros 'vírgenes' que no lograron entablar una relación en el merge_condition base."
        ),
    },
    {
        "exam": 3,
        "id": "q21_automatic_liquid_clustering",
        "question": "¿De qué manera el framework Automatic Liquid Clustering selecciona de forma determinista y sin intervención (keys) qué columnas gobernarán sobre las capas agrupables/ordenables (clustering) de una base regida por un Unity Catalog managed Delta table?",
        "options": [
            "Impulsa complejos procesos randomizados y random sampling de volcado en los que prioriza uniformidad ante lo agnóstico de la fuente.",
            "Toma directrices basándose unívocamente en lo especificado manualmente en un bloque cluster by genérico en la fase originaria.",
            "Infiere la distribución predefinida de una forma automática acotada simplemente basándose en el tipo y jerarquía del schema estricto originario.",
            "Se sustenta en la funcionalidad transversal de Predictive Optimization preevaluando/ingiriendo el patrón conductual y comportamiento en las ejecuciones recientes de queries de usuario."
        ],
        "answer": "Se sustenta en la funcionalidad transversal de Predictive Optimization preevaluando/ingiriendo el patrón conductual y comportamiento en las ejecuciones recientes de queries de usuario.",
        "explanation": "El denominado Automatic Liquid Clustering presente en los ecosistemas de Databricks consiste en un mecanismo que propicia una reestructuración inteligente (layout físico) por lo que es la piedra angular, interactuando fluidamente a partir del Predictive Optimization. A mayor acierto estadístico y estudio del footprint de queries del usuario, logra inferir y aplicar las clustering keys oportunas dinámicamente y con una visión macro."
    },
    {
        "exam": 3,
        "id": "q22_delta_file_statistics_query",
        "question": (
            "Dada la tabla subyacente Delta 'products' modelada con el presente esquema (schema):\n\n"
            "name STRING, category STRING, expiration_date DATE, price FLOAT\n\n"
            "A la hora de procesar o lanzar la query subsiguiente:\n\n"
            "SELECT * FROM products\n"
            "WHERE price = 90.5\n\n"
            "¿A qué tipo de información acudirá en tiempo de compilación tu query optimizer en aras de descartar archivos subyacentes e interrogar las fracciones deseadas eficientemente?"
        ),
        "options": [
            "A metadatos (Files statistics) centralizados a un nivel alto o global en el Unity Catalog metastore.",
            "Al análisis crudo incrustado localmente y estancado dentro de un nivel base del footer en el fichero binario físico Parquet.",
            "A un índice min/max dinámico albergado internamente como Files statistics en el Delta transaction log de origen.",
            "A los registros y mapeo legacy incrustados de metadatos Columns statistics propios del Hive metastore."
        ],
        "answer": "A un índice min/max dinámico albergado internamente como Files statistics en el Delta transaction log de origen.",
        "explanation": (
            "De forma transparente, un registro cronológico, el log transaccional de un entorno Delta Lake se va plagando de apuntes micro (estadísticos) colgados a expensas de la metadata base. Estas señales arrojan detalles minuciosos:\n"
            "- Cantidades base numéricas totales/Row counts\n"
            "- Umbrales mínimo/máximo evaluados exhaustivamente entre los bloques de las primeras 32 columnas rankeadas.\n"
            "- Conteos brutos referidos a un status nulo (NULL) computado sobre dicho subset de las 32 columnas.\n\n"
            "A posteriori, un query optimizer actuará con perspicacia devorando las trazas transaccionales, detectando las cotas (límites inferior/superior) de las variables como las del precio para descartar ficheros irrelevantes al escaneo de lectura (price = 90.5)."
        )
    },
    {
        "exam": 3,
        "id": "q23_delta_table_over_partitioned",
        "question": (
            "El grupo de ingenieros data engineers se topó con un escollo considerable en una de sus tablas particionadas de entorno Delta Lake, la misma reporta mermas notables de velocidad (lag/slowdowns) ante ejecuciones transversales de simple carácter.\n\n"
            "Tratando de aliviarlo lanzaron infructuosamente sin ningún alivio notorio la purga agrupable OPTIMIZE.\n\n"
            "¿Cuál a priori, en vistas al colapso relatado, es una de las respuestas a esta paradoja de ralentización insondable?"
        ),
        "options": [
            "Esa base peca de un síntoma de fragmentación masivo. Consiste en una sobre-partición errónea (over-partitioned). Remediarla o enmendarla requiere el esfuerzo total implacable del borrado y reescribir de lleno todo su esqueleto.",
            "Incurrían en torpeza al aplicar un comando plano OPTIMIZE sin un atributo acotado suplementario. Deberían vincular la Z-ordering contra la propia llave de la partición.",
            "Caían en el espejismo de invocar de una sola atacada un OPTIMIZE gigantesco; debería aplicarse fraccionado, lanzando procesos delimitados iterativos enfocándose individualmente en sub/particiones aisladas.",
            "Acumulaban gran peso físico de escombros binarios y archivos moribundos; debían forzar una ejecución higiénica drástica utilizando VACUUM."
        ],
        "answer": "Esa base peca de un síntoma de fragmentación masivo. Consiste en una sobre-partición errónea (over-partitioned). Remediarla o enmendarla requiere el esfuerzo total implacable del borrado y reescribir de lleno todo su esqueleto.",
        "explanation": (
            "Un conjunto particionado de una manera en exceso insalubre acabará pagando un peaje gigantesco en el rendimiento base. Los ficheros (small files phenomenon) se aíslan dentro de murallas artificiales lo cual los excluye y les coarta el aglutinarse fluidamente, lastrando costes tanto operacionales del storage como un volumen bestial de ficheros para escanear a la fuerza. Subsanarlo recae drásticamente sobre una labor extrema de reescritura masiva full rewrite para reconstruir y amoldar el tamaño de la partición."
        )
    },
    {
        "exam": 3,
        "id": "q24_lakeflow_streaming_table",
        "question": (
            "Un grupo de data engineering localizado en una sede del área operativa usa un ecosistema Lakeflow Declarative Pipelines, operan a granel sobre datasets crudos en su fuente matriz 'inventory_raw' que recoge adiciones únicamente (append-only) como si fueran latidos referidos a la disponibilidad product_id, un monto quantity, sumado de forma indisoluble al registro event_timestamp.\n\n"
            "El objetivo ineludible pasa por dotar a la nueva entidad proyectada, la tabla 'inventory_latest', del reflejo crudo de los remolques (cambios) cerca del estado real-time asimilados de esta fuente matriz. Esta flamante vista constará de la llave identificativa product_id, current_quantity, y finalmente un updated_timestamp.\n\n"
            "Considerando todos estos enfoques pragmáticos, ¿a qué prototipo o capa te dirigirías prioritariamente como molde de encaje para erigir 'inventory_latest'?"
        ),
        "options": [
            "Live table",
            "Materialized view",
            "Temporary view",
            "Streaming table"
        ],
        "answer": "Streaming table",
        "explanation": (
            "Frente a este ecosistema lo más pragmático e inherente pasa innegablemente por construir un formato dinámico, un 'Streaming table', ya que su código de desarrollo innato propulsa y garantiza un seguimiento continuo a niveles near real-time frente a un origen estático como una 'append-only' log como recae sobre el inventory_raw. Materializar el 'inventory_latest' como Streaming table favorece un flujo de inmersión en vivo donde converger o hacer updates sobre los CDC de manera instantánea y constante sobre ese estado per-producto (product_id). Cada alteración actualiza y refleja el delta de updated_timestamp evadiendo el cálculo del barrido (batch) a la brava.\n\n"
            "Un ente más rígido, como puede ser la Materialized View propicia una computación pesada orientada unívocamente a los lotes agendados periódicos precomputando visiones completas sin el control del ciclo de vida natural a pequeña escala en el tiempo. Y las tablas volátiles o las temporal views caducan o su ciclo o latido finaliza erradicando los apuntes vitales en la sesión viva en ese instante."
        )
    },
    {
        "exam": 3,
        "id": "q25_liquid_clustering_prerequisites",
        "question": "A grandes rasgos, enumera los 2 requisitos de base subyacentes e indiscutibles para poner en funcionamiento el Automatic Liquid Clustering operando nativamente en una tabla moldeada con Delta:\n\nElige 2 opciones:",
        "options": [
            "Configurarla y administrarla rigurosamente a través del marco normativo del Unity Catalog (Managed Table).",
            "La base de la tabla ha de gozar de los perfiles intrínsecos de rastreo basados en Deletion vectors.",
            "Debe persistir referenciada operando libre y externamente de lleno vía Unity Catalog (External table).",
            "Estar arropada operando bajo los influjos computacionales estadísticos activados del marco Predictive optimization.",
            "La tabla original demanda obligatoriamente su agrupación originaria en rangos de fechas (date column partition)."
        ],
        "answer": [
            "Configurarla y administrarla rigurosamente a través del marco normativo del Unity Catalog (Managed Table).",
            "Estar arropada operando bajo los influjos computacionales estadísticos activados del marco Predictive optimization."
        ],
        "explanation": (
            "De manera inequívoca, con el fin de asimilar o instigar el Automatic Liquid Clustering recayendo sobre el backend de Databricks, las directrices ineludibles acotan este terreno en:\n\n"
            "1. Consolidación de un ente supervisado a fondo por el panel maestro, Tabla Managed del Unity Catalog (UC Managed Table).\n"
            "- Su activación se deniega ante entornos referenciales External Tables sin gobierno interno profundo.\n\n"
            "2. Co-operación conjunta arropado por los escaneos de Predictive optimization en estado On.\n"
            "- Este supervisor recaba el comportamiento predictivo y métricas dinámicas orientadas al acceso, nutriendo con tal retroalimentación estadística el rediseño volumétrico de ficheros que instigará por su cuenta el algoritmo Liquid."
        )
    },
    {
        "exam": 3,
        "id": "q26_python_wheels_databricks",
        "question": "¿Cuál enunciado enarbola fielmente en un Databricks context, el significado intrínseco detrás de un archivo en formato Python wheel?",
        "options": [
            "El formato es un arquetipo condensado enfocado como distribución de binario predestinado a suministrar recursos, módulos y software base incrustándolo a los Clusters ejecutores de Databricks.",
            "Supone una reingeniería e injerto de Databricks nativo para orquestar y gestionar dependencias y descargas como variante y clon superpuesto por encima de la instrucción clásica 'pip'.",
            "Alude de una u otra manera a la bóveda (repositorio) subyacente focalizada a la tarea de acopiar, gestionar un inventario y enviar los módulos y metadatos alojados en el ecosistema Databricks.",
            "Se ciñe a una jaula restrictiva (Virtual environment) orientada a la contención protectora aislando del colapso y fragmentando intérprete y submódulos frente a Notebooks compartidos limítrofes."
        ],
        "answer": "El formato es un arquetipo condensado enfocado como distribución de binario predestinado a suministrar recursos, módulos y software base incrustándolo a los Clusters ejecutores de Databricks.",
        "explanation": (
            "Efectivamente. Un archivo Python wheel conforma una entidad nativa (arquetipo o empaquetado puro binario) orientado sin paliativos a aglutinar el volcado, instalación e interconexión de recursos dependientes Python directamente incrustados a Databricks Clusters.\n\n"
            "Técnicamente se plasma bajo un estándar ZIP empaquetado bajo un diminutivo o sufijo final estandarizado catalogado como la terminación .whl"
        )
    },
    {
        "exam": 3,
        "id": "q27_cdf_overwrite_target",
        "question": (
            "Dada la ingesta secuencial referida a continuación orientada a someter (query) la tabla 'customers' en la que fluye e interfiere el subsistema perenne (Change Data Feed) operativo:\n\n"
            "spark.read\n"
            '    .option("readChangeFeed", "true")\n'
            '    .option("startingVersion", 0)\n'
            '    .table("customers")\n'
            '    .filter(" _change_type=\'update_postimage\'")\n'
            "    .write\n"
            '    .mode("overwrite")\n'
            '    .table("customers_updates")\n\n'
            "Bajo un análisis clínico ¿qué secuela se originará incesantemente en cada nuevo reintento y ejecución cronológica?"
        ),
        "options": [
            "De un barrido drástico, absolutamente todos los cimientos históricos amparados en las modificaciones borrarán de raíz (overwrite) al receptor y destino íntegro cada vez que opere el trigger.",
            "El grupo entrante de renovados o novedosos lotes sobrescribirá implacablemente en el destino base.",
            "Ese ínfimo conjunto renovado de los registros se incorporará apilándose sigilosamente añadiendo datos (appended) de una tacada a la fuente target.",
            "Como apunte drástico, su vastedad íntegra amparando a la data recabada en el histórico engrosará la lista, solapándose ininterrumpidamente sumiendo la tabla de destino final en la redundancia masiva duplicada."
        ],
        "answer": "De un barrido drástico, absolutamente todos los cimientos históricos amparados en las modificaciones borrarán de raíz (overwrite) al receptor y destino íntegro cada vez que opere el trigger.",
        "explanation": (
            "La invocación a través del marco nativo puro de lecturas limitadas del subconjunto (spark.read) orientadas al marco del Change Data Feed te adhiere y empuja un dictamen inamovible (estático). Resultando así una regresión a la etapa original y primigenia (0) en que el código acudirá ciegamente, leyéndolo desde el germen histórico subyacente cada vez.\n\n"
            "Escarbando en el final, este output reacciona escupiendo sus bytes y depositándolos en su sumidero final al mando regidor (mode \"overwrite\"). Concluyendo que el recipiente target y sus frutos quedarán fulminados de cuajo a base de la sobreescritura de todo en cada tirada."
        )
    },
    {
        "exam": 3,
        "id": "q28_optimize_default_file_size",
        "question": "Con respecto al motor transaccional implícito del comando ejecutor manual OPTIMIZE arrojando y reciclando diminutos fragmentos/archivos, ¿bajo qué techo o dimensión global meta se consolidan ciegamente en una tabla base?",
        "options": [
            "1024 MB",
            "256 MB",
            "512 MB",
            "128 MB"
        ],
        "answer": "1024 MB",
        "explanation": "Bajo la llamada manual, el mandato OPTIMIZE comprime pequeños segmentos disueltos apilándolos a los data files compactos. Actuando de un plumazo bajo su estandarte por defecto y anidándose a los 1073741824 bytes, esto recae e incrusta a ras o su equivalente final escalado a 1 GB."
    },
    {
        "exam": 3,
        "id": "q29_autoloader_pathglobfilter",
        "question": (
            "Dentro de las entrañas de una operativa pura productiva (S3 bucket) donde desfilan y se cuelan diariamente archivos e imágenes asimilados por extensión indiscriminadamente en (.png, .jpg, .gif). Se instaure a un perfil operativo, al data engineer, a reajustar esta incesante entrada a través del canal en streaming para constreñir y acotar exclusivamente la criba estricta del procesamiento de cara al rango y sufijo de .png.\n\n"
            'df = spark.readStream.format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "binaryFile") \\\n'
            '    .option(__________________, "*.png") \\\n'
            '    .load("s3://mybucket/incoming/")\n\n'
            "¿Bajo qué mandato imperativo exacto subsanarás las carencias del comando expuesto rellenando con pulso la laguna en blanco?"
        ),
        "options": [
            "fileExtension",
            "cloudFiles.pathGlobFilter",
            "pathGlobFilter",
            "cloudFiles.fileExtension"
        ],
        "answer": "pathGlobFilter",
        "explanation": (
            "Este atributo (pathGlobFilter) otorga el poder incondicional focalizado a discriminar (filter) frente a extensiones indiscriminadas a raíz o sumisas bajo un dictamen de expresión genérica acotada (glob pattern). Su asimilación tipo \"*.png\", se vincula inherentemente bajo un uso o sintaxis estricta propulsada por las mecánicas del Auto Loader."
        )
    },
    {
        "exam": 3,
        "id": "q30_cluster_permissions_attach",
        "question": "¿Cuál dictamen regidor refleja de cabo a rabo en el umbral operativo y la capa subyacente la asignación y rango ínfimo concedido al perfil de un data engineer para acoplar o vincular en su sesión (attach) la celda o script en el bloque físico a modo un cluster existente en la nube?",
        "options": [
            'Permiso global inherente de la cuenta (Cluster creation) complementado a rajatabla en el marco interno con el rol/privilegio de "Can Attach To".',
            'Sostener en estado de acceso únicamente el atributo singular "Can Attach To" apuntalado sobre esa base aislada (cluster).',
            'Otorgarse el mandato encriptado referenciado como "Can Restart".',
            'Gobernarlo omnímodamente abanderando el "Can Manage" sin impedimentos.'
        ],
        "answer": '"Can Attach To" privilege on the cluster',
        "explanation": (
            "A la hora de la configuración pura recae una bifurcación en el acceso globalizado:\n"
            "1- Su potestad subyacente (entitlement general) focalizándose a dar carta blanca para instanciar (create) nuevas esferas desde los cimientos.\n"
            "2- Un micro poder, la matriz granulada a nivel base u origen, en el cual se imparte un poder de alcance y usabilidad en un escenario físico único. Dividiéndose en rangos delimitados que abordan en orden piramidal: Carencia absoluta de potestad (No Permissions), Permisibilidad de enlace (Can Attach To), Poder de interrupción física (Can Restart), Administración rotunda global (Can Manage). Adherirse ciegamente a un esqueleto operativo a través del marco con tu entorno base (notebook to compute) se satisface íntegramente abrazando el peldaño de Can Attach To."
        )
    },
    {
        "exam": 3,
        "id": "q31_mitigate_data_skew_join",
        "question": (
            "En medio de un denso e infatigable escrutinio empujado por un data engineer acotado sobre flujos incesantes transaccionales (clickstream events) derivados del uso abrumador de una web puntera, se topan ante las barreras asimétricas. Tal corpus almacena su núcleo en variables como user_id, timestamp, event_type, url... Arrastran un choque de colisión insospechado a la hora de orquestar operaciones transversales (join) al cruzar ambos mundos contra una agenda de perfiles estática (user_id). Su runtime sucumbe, agonizando e infiriéndose que gran parte sucumbe por un reparto errático arrastrado por variables concentradas y acaparadas ciegamente por grupos reducidísimos de súper usuarios generadores de la carga desbalanceada o cuellos de botella.\n\n"
            "¿Cuál entre la batería de directrices descritas recae un craso e inapropiado desacierto a la hora de enmendar en pro y en el marco de mitigar este desajuste colosal?"
        ),
        "options": [
            "Esparcir o radiar forzosamente (broadcast) este set de las 'hot keys' reacias a expandirse sin pasar por el tamiz del desbarajuste y barajar (shuffle) a través de todos los rincones del join originario.",
            "Abstraer o desmenuzar las operaciones arrastrando a estos usuarios de hiper actividad hacia un ecosistema o job singular desligado del troncal.",
            "Acudir e inyectar atributos ficticios de manera aséptica anexando salazón (salting), forzando el empuje random en esos atributos desbocados y aplastándolos y expandiéndolos ante dispares parcelas o trozos (partitions).",
            "Trocear desde el cimiento, desarmando la tabla matriz e impartiendo drásticamente y desde abajo una partición nueva o barajarlo (repartition) ciegamente mediante sus atributos conflictivos en número."
        ],
        "answer": "Esparcir o radiar forzosamente (broadcast) este set de las 'hot keys' reacias a expandirse sin pasar por el tamiz del desbarajuste y barajar (shuffle) a través de todos los rincones del join originario.",
        "explanation": (
            "Acudir y apoyarse ciegamente propagando e impulsando bajo la orden (Broadcasting) orientando este esfuerzo contra la pequeña esfera asimétrica, supone un gran aporte al compartir fuentes base de lectura o dimensiones limitadas ante los rincones operacionales. Sin embargo, no solventará el dilema puro asimétrico o de sesgos de hiper actividad asumiendo en muchos flancos una embestida implacable penalizando el volcado de carga pesada subyacente y elevando su ratio del ahogo crítico presencial sobre la memoria física (memory pressure) estancado dentro de un executor originario.\n\n"
            "Abrazar en contrapartida estas alternativas sí subsanan susodicho caos asimétrico:\n"
            "- Asimilando al marco de trabajo el inyectar un relleno aséptico (salting/prefixing) se obliga e irrumpe desmembrando el núcleo rígido abrumador frente a una plétora o diversidad de divisiones sanando el embotellamiento del barajar en vuelo.\n\n"
            "- Redistribuir con un hacha implacable (repartition dataset) promueve e impulsa paralelos incrementales forzando las variables subyacentes.\n\n"
            "- Segregando las esferas conflictivas hacia vías y tuberías propias operacionales se elude que un coloso ciego arrastre a su ecosistema afín de flujos sanos o menores paralelos."
        )
    },
    {
        "exam": 3,
        "id": "q32_lakeflow_data_quality_expect_all",
        "question": (
            "Para un data engineer que anda embarcado afanosamente en cimentar una robusta canalización (Lakeflow Declarative Pipeline) con miras a acaparar transacciones originarias, requiere un férreo o contundente sistema base afianzado a las reglas rectoras del gobierno orgánico del dato:\n\n"
            'valid_products = "product_id IS NOT NULL", "recent_sales": "date >= \'2023-01-01\'", "quantity_within_range": "quantity BETWEEN 0 AND 1000"\n\n'
            "Asimilando estos hitos se propulsa un escenario benigno que no discrimine físicamente las infracciones, por el contrario, fluirán con la permisibilidad íntegra pero delatados, reportados en los sumarios y acorralados explícitamente en paneles estadísticos u observabilidad del motor analítico.\n\n"
            "Del abanico y amalgama subyacente mostrada a continuación, ¿cuál de todas estas directrices operacionales (configurations) suple de lleno lo postulado?"
        ),
        "options": [
            "@dlt.table\n@dlt.expect_all(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
            "@dlt.table\n@dlt.expect_all_fail(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
            "@dlt.table\n@dlt.expect_all_drop(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
            "@dlt.table\n@dlt.expect(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")"
        ],
        "answer": "@dlt.table\n@dlt.expect_all(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
        "explanation": (
            "Operando con esta orden (dlt.expect_all()), se obliga intrínsecamente a procesar el paquete global (grupo) empujándolo en el sistema de manera implacable, permitiendo la vía libre amparando filas viciadas y sanas depositadas en el fin, a la par que arrojan registros delatores engrosando sus bitácoras estadísticas operativas (metrics).\n\n"
            "Hacer uso genérico o aislado vía (dlt.expect()) acarreará una carencia intrínseca que obvia englobar todo unitariamente impidiendo reaccionar operativamente como un bulto único. Lo propio ocurrirá frente a sentencias de rechazo excluyente o filtros rígidos (expect_or_drop/expect_all_drop) u opciones beligerantes que paralicen sin tregua toda acción originaria a modo de muro y barrera final (expect_or_fail).\n\n"
            "Nota orientativa al pie: la cúpula corporativa y gestora ha impulsado y donado en masa, transmutando las raíces base de estas tecnologías inyectándolas de pleno al núcleo latente del proyecto original (Apache Spark ecosistema) bajo denominaciones análogas de código (Spark Declarative Pipelines/SDP)."
        )
    },
    {
        "exam": 3,
        "id": "q33_query_profiler_total_wall_clock",
        "question": (
            "Sometido bajo una indagación, un data engineer procedió en pos de cazar a una query que se eternizaba de manera anómala. Para diseccionarla minuciosamente recurre al estandarte nativo, Query Profiler anclado en su entorno con el foco perspicaz de recabar sus lapsos base mediante Total wall-clock duration metric.\n\n"
            "¿Cuál a priori, en relación al suceso subyacente y los dictámenes lógicos, es lo que define o plasma en términos temporales estrictamente este estadístico?"
        ),
        "options": [
            "Representa al cronómetro en su inicio y tope delimitados a una ejecución franca y dura operada frente a este volcado sin artificios.",
            "Representa a la barrera integral invertida ciegamente a lidiar con una limpieza e ignorando a un subproducto o el podado estructural orgánico.",
            "Estampa sin preámbulos el intervalo de tiempo empleado ciegamente en situarla o fijarla temporalmente (scheduling).",
            "Representa al conjunto exhaustivo del abanico temporal consumido arrancando desde lo pospuesto organizativo (scheduling) y feneciendo ante lo físico en etapa de query execution."
        ],
        "answer": "Representa al conjunto exhaustivo del abanico temporal consumido arrancando desde lo pospuesto organizativo (scheduling) y feneciendo ante lo físico en etapa de query execution.",
        "explanation": (
            "El susodicho término (Total wall-clock duration metric) acapara de punta a punta o se encarga de acaparar los intervalos aglutinando su inicio estático que recae o flanquea su propia temporalidad en fila (scheduling), los esfuerzos que acaparan limar asperezas optimizando e infiriendo de forma selectiva ficheros marginales y de poco peso (file pruning), clausurándose ininterrumpidamente una vez asimilado todo el volcado crudo."
        )
    },
    {
        "exam": 3,
        "id": "q34_delta_check_constraint_failure",
        "question": (
            "Sometido bajo el radar del equipo, poseen a un ente gigante subyacente, una gran tabla (Delta) apodada como 'users'. Resurgió a base de la inmersión analítica unos atributos asombrosos en el espectro o dimensión de 'age' recabando sumandos no orgánicos o incongruencias en escalas negativas.\n\n"
            "A modo de tapadera o salvavidas a base empírica, un operario de extracción o junior data engineer se embarca aplicando forzosamente un nuevo estandarte regulador CHECK inyectado al comando transaccional:\n\n"
            "ALTER TABLE users ADD CONSTRAINT valid_age CHECK (age> 0);\n\n"
            "Tras invocar a los elementos de proceso salta una inaudita traba abocando de pleno en error o en falla sin éxito.\n\n"
            "¿Bajo qué pretexto orgánico en este relato recae tamaña caída o error imperativo transaccional en este bloqueo fallido?"
        ),
        "options": [
            "Que, operativamente a estas alturas el recipiente de datos base albergaba y ocultaba registros viciados, mancillando de plano este decreto o normativa y se dictamina a sabiendas que antes deben purgarse todos.",
            "Que al estar la entidad consolidada per se en el almacenamiento, este comando estricto CHECK se ciñe en etapa de originación excluyente como amparo al código inicial del CREATE TABLE.",
            "Se comete fallos lógicos a la hora de codificar sintaxis operacionales en la cual se erró en: ALTER TABLE users ADD CONSTRAINT ON COLUMN age (CHECK > 0)",
            "Que habiendo albergado ya trazos, apuntes y huellas físicas el componente u organismo Delta; este yugo CHECK solo rige bajo el influjo de entornos ausentes o tabula rasa operando libre de filas."
        ],
        "answer": "Que, operativamente a estas alturas el recipiente de datos base albergaba y ocultaba registros viciados, mancillando de plano este decreto o normativa y se dictamina a sabiendas que antes deben purgarse todos.",
        "explanation": (
            "La invocación ejecutiva tras la orden (ADD CONSTRAINT command) comprueba o examina un nivel de purga forzado asegurando de antemano el escenario de paz validando si todas y cada una de las celdas colindantes se postulan fieles y sumisas. En caso anómalo la sentencia abortará con fiereza de modo irreconciliable y sin retorno, advirtiendo a base de mensajes internos alertando de unas anomalías crónicas persistentes."
        )
    },
    {
        "exam": 3,
        "id": "q35_validate_button_declarative_pipeline",
        "question": (
            "En el centro operativo de extracción y acopio analítico, el team transita inmiscuyéndose implementando un Lakeflow Declarative Pipeline, que sirva de apoyo incondicional operando con ingestas apabullantes masivas y escalables. Inmersos ciegamente al calor transaccional bajo estricta pericia, surge el dictamen rector abanderado de lleno desde su cúspide por el lider: antecediendo de pleno sin titubeos previo al accionar físico ciego, se dictamina ciegamente o se encamina siempre al botón o pulsador base llamado \"Validate\" ubicado en las filas interconectadas dentro del notebook propio transaccional o núcleo asociado.\n\n"
            "Desmenuzando, ¿qué gran aporte colosal propulsa el accionar ciego de esta pericia subyacente metódica?"
        ),
        "options": [
            "Afianzar las potestades inherentes de privilegios subyacentes corroborando que obre en pleno sin restricciones dentro del dictamen catalogador principal.",
            "Indagar sigilosamente en las deficiencias, incoherencias e irregularidades orgánicas al pie en los estandartes codificados sin llegar en la etapa de transmutar información física empírica de manera plena (procesamiento ciego).",
            "Obliga operativamente sin retorno al sistema a tragarse a base de simulacro escuetos pedazos microscópicos u orgánicos y arroja de un portazo visual un informe parcializado crudo del volcado (transformaciones).",
            "Erigirse y actuar simulando ejecuciones de fragmentos microscópicos testeadores u orgánicos aislando el núcleo operativo del unitario en el total."
        ],
        "answer": "Indagar sigilosamente en las deficiencias, incoherencias e irregularidades orgánicas al pie en los estandartes codificados sin llegar en la etapa de transmutar información física empírica de manera plena (procesamiento ciego).",
        "explanation": (
            "Este escudo y escudo orgánico subyacente a expensas de invocar a la base o accionar (\"Validate\"), aflora o rastrea fallos gramaticales subyacentes o de configuración base a la hora de orquestar operaciones operacionales antes de su estallido transaccional u orquestal. Salvando así contratiempos sin tregua a mitad de procesos críticos que desembocaran de otra forma a abocar transacciones físicas y mutaciones defectuosas truncadas."
        )
    },
    {
        "exam": 3,
        "id": "q36_databricks_rest_api_run_id",
        "question": "Convocando y poniendo en vigencia en la recámara a uno ya preexistente y estructurado, un job encauzado y regido vía directrices operativas (Databricks REST API), ¿cuál identificador ciego subyacente u orgánico plasma y rige y acapara un carácter incuestionable con rango incondicional de universal apuntalado a la nueva matriz activada recién nacida por el trigger?",
        "options": [
            "task_id",
            "job_id",
            "run_key",
            "run_id"
        ],
        "answer": "run_id",
        "explanation": (
            "Pulsando ciegamente en pleno vigor e inyectando un mandato u operativo incondicional a un organismo latente invocando al punto neurálgico ('/api/2.0/jobs/run-now'), devolverá indefectiblemente a modo implacable un registro del run_id con potestad a este run u operativo desatado. Lo que encumbra y apuntala a base férrea la singularidad del mismo (globally unique identifier)."
        )
    },
    {
        "exam": 3,
        "id": "q37_pyspark_window_dense_rank",
        "question": (
            "Armado el data engineer se afana albergando y moldeando este organismo de tipo PySpark DataFrame estructurándolo albergando al pie los valores a título: employee_name, department, and salary. Ambicionando en pro a este requerimiento y asignando férreamente o acoplando de lleno a base jerárquica u organigrama una dimensión (tier) ligada intrínsecamente al departamento descendente, sin obviar u omitir que aquél binomio de homónimos financieros cobren con las mismas cantidades recibiendo a título parejo mismo dictamen escalonado:\n\n"
            "| employee_name | department | salary | tier |\n"
            "|---------------|------------|--------|------|\n"
            "| Eve           | HR         | 4000   | 1    |\n"
            "| Frank         | HR         | 4000   | 1    |\n"
            "| David         | HR         | 3900   | 2    |\n"
            "| Alice         | Sales      | 5000   | 1    |\n"
            "| Bob           | Sales      | 4500   | 2    |\n"
            "| Charlie       | Sales      | 4500   | 2    |\n\n"
            "En pos de dar a luz, materializa u orienta dictando o definiendo su encapsulamiento ordenado por descendentes con este bloque orgánico:\n\n"
            'window_spec = Window.partitionBy("department").orderBy(df["salary"].desc())\n\n'
            "De este repertorio ciego e incuestionable subyacente, ¿cuál plasma y hace valer sin dudar la fórmula correcta dictando el escalonado tier exigido?"
        ),
        "options": [
            'df.withColumn("tier", percent_rank().over(window_spec))',
            'df.withColumn("tier", rank().over(window_spec))',
            'df.withColumn("tier", row_number().over(window_spec))',
            'df.withColumn("tier", dense_rank().over(window_spec))'
        ],
        "answer": 'df.withColumn("tier", dense_rank().over(window_spec))',
        "explanation": (
            "Acudir o engarzar bajo este comando con firmeza operando a base o bajo dictamen dense_rank() atribuye a esta tabla idéntica correlación de base o estatus a variables igualitarias o calcadas conservándose incorrupto la rampa descendente sin dar salto al vacío ni mermas en lo numérico ante variables venideras mermadas. Albergando así su idóneo y pretendido molde o requerimiento orgánico en donde los empatados Eve y Frank recaban o mantienen su número a pesar de que el contiguo menor salte correlativo empíricamente o de pleno como escalafón adyacente (tier 2).\n\n"
            "Escudriñando las adyacentes nos empujarían a un fallo: un comando row_number() operaría inmiscuyendo numerales secuenciales no asimilando al fin las repeticiones. Al igual que operaría un comando rank() abocándose y arrastrándonos al salto crudo evadiendo y coartando al numeral consecutivo subyacente."
        )
    },
    {
        "exam": 3,
        "id": "q38_udf_dynamic_data_masking",
        "question": (
            "Sometidos bajo escrutinio clínico corporativo u orgánico de gran volumen amparados en esferas de alta pericia, el equipo médico de ingeniería cimenta y gestiona sus operaciones o almacén en tabla Delta Lake patient_records desmenuzada y partida con las variables a nombrar como: patient_id, name, department, y diagnosis. Acuden y anhelan orquestar este bloqueo y acopio albergando y moldeando las funcionalidades definidas incrustando su propia función o tapadera encubridora (user-defined function/masking) propulsando a modo que las miradas ajenas al entorno estrictamente doctrinario, o para médicos autorizados evadiendo fisgones ante el atributo delicado del registro médico (diagnosis).\n\n"
            "Dentro de las pautas a esgrimir codificadas, ¿qué abanico obedece o cimienta y aporta de forma eficaz un tapujo transaccional e infiere tal fin en un entorno?"
        ),
        "options": [
            "CREATE FUNCTION patient_mask(doctors STRING)\nRETURN CASE WHEN is_account_group_member('doctors') THEN doctors ELSE 'CONFIDENTIAL' END;",
            "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN in_account_group_member('doctors') THEN diagnosis ELSE 'CONFIDENTIAL' END;",
            "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN is_account_group_member('doctors') THEN diagnosis ELSE 'CONFIDENTIAL' END;",
            "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN diagnosis IS NOT NULL THEN diagnosis ELSE 'CONFIDENTIAL' END;"
        ],
        "answer": "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN is_account_group_member('doctors') THEN diagnosis ELSE 'CONFIDENTIAL' END;",
        "explanation": (
            "Se implementa correctamente de una forma u operando a base u holgadamente la base orgánica recabando la membresía grupal de cuentas operacionales (RBAC o rol-based access control). Verificándose o escudriñando ciegamente a título intrínseco de modo que el individuo o ente pertenezca de lleno al entorno clínico (is_account_group_member('doctors')). Albergando si la validación aflora y retorna asertivamente se plasmarán datos, en revés actuará y sustituirá subyacentemente plasmando una cortina genérica de encubrimiento u anonimato protegiendo los datos con este flag text u string 'CONFIDENTIAL'.\n\n"
            "Cumpliendo íntegramente las prerrogativas o trabas burocráticas y normativas del sector y amparando las miradas ajenas."
        )
    },
    {
        "exam": 3,
        "id": "q39_dataframe_write_append",
        "question": (
            "A premiante paso le fue requerido y encargado al ingeniero enarbolar u operar con un lote de procesamiento de rutina cíclico y asincrónico por las madrugadas abordando una métrica u orquestando analíticas referidas a recursos operativos y su propio personal. La carga arrojará una cota de puntos a modo estimativo del lapso y balance transcurrido de antaño y almacenando de este modo al empleado en base a las premisas del Delta table \"employees_performance\". Sometiéndose orgánicamente a este esquema (schema):\n\n"
            "\"date DATE, employee_id STRING, rating DOUBLE\"\n\n"
            "Desean incrustar al seno orgánico estos balances a medida de modo que el equipo pueda contrastar el historial sin dilación.\n\n"
            "Con estas acotaciones operacionales ¿qué comando forzoso plasma u origina y resuelve tal coyuntura de manera satisfactoria?"
        ),
        "options": [
            'performance_df.write.format("delta").saveAsTable("employees_performance")',
            'performance_df.write.mode("append").saveAsTable("employees_performance")',
            'performance_df.write.mode("overwrite").saveAsTable("employees_performance")',
            'performance_df.write.saveAsTable("employees_performance")'
        ],
        "answer": 'performance_df.write.mode("append").saveAsTable("employees_performance")',
        "explanation": (
            "El componente y operador matriz base dictamina incondicionalmente las particularidades o conductas a adoptar (DataFrameWriter.mode) albergando y topándose ante datos o bases preexistentes o colindantes.\n"
            "A la cabeza dispone de las siguientes operativas base:\n"
            "- append: Solapa e incorpora en los cimientos del DataFrame albergándolas colindantemente sobre pre-existentes sin fisuras.\n"
            "- overwrite: Fulmina sin compasión todo el entramado de base de datos colindante.\n"
            "- error o errorifexists: Estalla e invalida su empuje rebotando errores tajantes avisando sin tregua.\n"
            "- ignore: Omite en su propio rastro y sin rechistar cualquier ejecución colindante a lo preestablecido.\n\n"
            "A sabiendas de estas normativas, para evitar un estallido fatal a la orden general el table debe nutrirse ciegamente de los sucesivos datos empujando al append. Permitiéndonos salvaguardar la historicidad y las líneas temporales pasadas de modo perenne orgánicamente."
        )
    },
    {
        "exam": 3,
        "id": "q40_repair_failed_multitask_job",
        "question": (
            "Sufriendo y arrastrando sin reparos operando de antemano el ingeniero porta un componente o job de gran tonelaje, aglutinando eslabones colindantes (multitask) estancando su periplo en una demorada brecha mayor de las 2 horas incondicionales previas a finalizar y clausurarse. Desencadenándose en las postrimerías operacionales un fallo no predicho del tramo fatal originario final (final task).\n\n"
            "¿Bajo qué potestad imperativa puede acogerse en su amparo el ingeniero mitigando estragos o dilapidaciones infructuosas de ciclos y horas reloj reestructurando esta fase abortada de modo veloz?"
        ),
        "options": [
            "Invocará y disparará de lleno de modo global (re-run) reciclando la maquinaria forzosamente a empujones a través del Job Run encadenando incesantemente toda labor a pesar del coste.",
            "Acudirá a barrer y depurar sin fisuras o de plano al núcleo de la operativa extinta fulminándola (failed Run), encauzando del cero absoluto una instancia de cero absoluto.",
            "Podrá resguardar y mantener tal evento fracasado a salvo sin repercusiones colaterales (failed Run), optando por encauzar y disparar una vertiente nueva ajena y renegada a este escollo original.",
            "Bajo su regazo y estandarte subyacente de (repair) en este Run amparado incondicionalmente a base de eslabones colindantes, abordará forzosamente tan solo la enmienda o laboriosa celda sin afectaciones del éxito de antaño."
        ],
        "answer": "Bajo su regazo y estandarte subyacente de (repair) en este Run amparado incondicionalmente a base de eslabones colindantes, abordará forzosamente tan solo la enmienda o laboriosa celda sin afectaciones del éxito de antaño.",
        "explanation": (
            "Albergando y arropando sin dilación una enmienda orgánica (repair failed multi-task jobs) transaccionas y acotas subyacentemente al componente averiado junto al apéndice remanente. Las preexistentes operacionales sanas y exentas de taras ignoran el tramo colindante librándose e interponiéndose evadiendo y blindando su gasto temporal. Repercutiendo en una disminución drástica temporal y de recursos malgastados ante contingencias transaccionales."
        )
    },
    {
        "exam": 3,
        "id": "q41_grant_least_privilege_access",
        "question": (
            "Un perfil data scientist forjado orgánicamente en el entorno de las estrategias (marketing department) amparado a base de estipendios operacionales demanda acatar y extraer sin impedimentos consultas y visiones (read-only access) a base de escarceos de una entidad matricial y acotada referida como 'customer_insights'. Confinada de lleno al acopio colindante (analytics schema), a la par que subsumida o arropada a un bloque catálogo de negocio o 'BI'. Operarán escudriñando a fin de esculpir el reporte sin cesar cuatrimestral o agendado trimestral. Albergándose fidedignamente bajo doctrinas protectoras imperantes y de base cimentadas sobre el dogma imperativo subyacente (principle of least privilege) acotando u operando ceñido al perímetro estricto mínimo operativo, el de mínima injerencia.\n\n"
            "Sopesando en abanico y amalgama el abecedario transaccional, ¿cuál dictamen colindante rige sin fisura esta enmienda de forma prístina operando ínfimamente y de lleno con esta prerrogativa operativa de base restrictiva?"
        ),
        "options": [
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;"
        ],
        "answer": "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;",
        "explanation": (
            "En esta travesía o acopio escalonado de permisos o autorizaciones unívocas se abordará jerárquicamente subyaciendo la potestad sin obviar pasos lógicos, infiriéndose el requerimiento transaccional imperante y base estipendiado (SELECT) a pie del receptor o del ente inmiscuyéndose subyacente en el escalafón intermedio u orgánico del (USE SCHEMA), arropándolo orgánicamente de pleno mediante un enlazado final transaccional base del padre o tutor (USE CATALOG). Todo ello confiere el mínimo absoluto y exacto a medida operando con agilidad blindándose sobre bases desmesuradas de alcance excesivo y de acceso colosal (overprovisioning)."
        )
    },
    {
        "exam": 3,
        "id": "q42_secret_scope_read_permission",
        "question": (
            "Operando en su feudo incondicional el escuadrón operativo asimila y despliega operando orgánicamente un acervo confidencial secret scope apellidado \"DataOps-Prod\" albergando fidedignamente en secreto su abanico confidencial resguardando un entorno matriz subyacente (production workspace).\n\n"
            "¿Cuál a priori, amparando al escalón estipendiado u organizativo, acata el grado ínfimo exigido imperativo para desenvolverse sin menoscabo en los arquetipos confidenciales base dentro de esta envoltura del secret scope?"
        ),
        "options": [
            "Conceder y apoderarse de permisos transaccionales a modo estricto MANAGE amparado y forzoso al pie del eslabón base de un secret operando dentro de \"DataOps-Prod\" scope.",
            "Tributar y apoderarse sin trabas de permisos transaccionales READ focalizados transversalmente amparando en pleno rigor orgánicamente el \"DataOps-Prod\" scope.",
            "Operar o subsumirse recabando poderes transaccionales referidos a base restrictiva del READ adentrándose individual e intrínsecamente a cada elemento base del \"DataOps-Prod\" scope.",
            "Ostentar el título transaccional mayor o la patente de corso base regido a base de un MANAGE imperativo transversal sobre \"DataOps-Prod\" scope."
        ],
        "answer": "Tributar y apoderarse sin trabas de permisos transaccionales READ focalizados transversalmente amparando en pleno rigor orgánicamente el \"DataOps-Prod\" scope.",
        "explanation": (
            "Abrazando el código normativo y rector subyacente de esta pericia estipendiada rige y dictamina así la criba perenne o peldaño organizativo:\n"
            "- MANAGE - Facultando obrar sin coto subyacente operando bajo directrices del sistema de control asimilando cambios ACLs, escarceos físicos o sobreescritura (read and write) sobre su núcleo secret scope.\n"
            "- WRITE - Dotado orgánicamente del accionar subyacente confinado al esculpido de registros transaccionales (read and write) en este seno del secret scope.\n"
            "- READ - Subsumiéndose orgánicamente con visibilidad transaccional a la lectura operando de escrutinio con el inventariado genérico subyacente (disponibilidad y listado).\n\n"
            "Esquivando cualquier laguna u objeción subyacente, un grado asimilado albergando dotes transaccionales absorbe subyacentemente al previo en rango imperativo del orden o escalera transaccional a medida de un (WRITE) acatando plenas prebendas operativas propias implícitas y operativas del (READ)."
        )
    },
    {
        "exam": 3,
        "id": "q43_extract_ldp_data_quality_metrics",
        "question": (
            "Operando a paso o ritmo dictaminado el ingeniero debe o se le demanda abocarse de forma sistemática y algorítmica para depurar la cosecha o frutos arrojados (data quality results) originarios orgánicamente de una arteria subyacente o tubería (LDP pipeline) asimilados de lleno de la entidad cronista o base matriz (event log table).\n\n"
            "Evaluando este reto y pericia en curso, ¿a cuál subconjunto estricto y orgánico transaccional o bloque de sintaxis operativa, se aferra un ente de manera infalible abordando exitosamente la meta o meta?"
        ),
        "options": [
            "SELECT expectations\nFROM catalog.schema.event_log\nWHERE event_type = 'metrics'",
            "SELECT data_quality\nFROM catalog.schema.event_log\nWHERE event_type = 'metrics'",
            "SELECT details:flow_progress.data_quality.expectations\nFROM catalog.schema.event_log\nWHERE event_type = 'flow_progress'",
            "SELECT data_quality\nFROM catalog.schema.event_log\nWHERE event_type = 'flow_progress'"
        ],
        "answer": "SELECT details:flow_progress.data_quality.expectations\nFROM catalog.schema.event_log\nWHERE event_type = 'flow_progress'",
        "explanation": (
            "Abstraídos orgánicamente en la bitácora o log inmiscuyéndose en pericias y entornos del LDP* pipeline, se asimilan subyacentemente trazas fidedignas orgánicas de variables sobre calidad asimilándose como parte orgánica encuadernados como variables base (event_type = 'flow_progress') incrustados de cuajo a base empírica forjados bajo esquemas encuadernados base tipo JSON en cascada u órgano colindante:\n\n"
            "- details:flow_progress: contiene fidedignamente al avance o empuje del pipeline's execution.\n"
            "- details:flow_progress.data_quality: compila o acapara y guarda recabando frutos y mermas (expectations, dropped_records, etc.).\n"
            "- details:flow_progress.data_quality.expectations: su resguardo absoluto y estricto recabando la prebenda transaccional y resoluciones exclusivas de dichas expectativas evaluadas.\n\n"
            "* Como apostilla organizativa subyacente a la par de un apunte, se atestigua por voz del equipo madre (Databricks) la reciente donación subyacente u obsequio empujándolo forzadamente asimilado de lleno hacia Apache Spark ecosistema, abanderado con títulos operativos orgánicos a título de Spark Declarative Pipelines (SDP)."
        )
    },
    {
        "exam": 3,
        "id": "q44_ldp_constraint_violation_drop_row",
        "question": (
            "Consolidando e impartiendo forzosamente trabas restrictivas acotando al vuelo a modo orgánico y férreo, un operario de perfil ingeniero cimentó un dique o regla data quality constraint estipendiada imperativa amparándose en entornos del LDP pipeline:\n\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ________________\n\n"
            "Operando de la amalgama de cláusulas subyacentes operacionales, ¿cuál de todas rellena empíricamente esta base sin mermas operativas provocando de un plumazo y de forma tajante o fulminante la supresión de elementos o renglones desobedientes operando orgánicamente sin borrar y a salvo las estadísticas (metrics) u bitácora?"
        ),
        "options": [
            "ON VIOLATION DISCARD ROW",
            "ON VIOLATION DELETE ROW",
            "ON VIOLATION DROP ROW",
            "ON VIOLATION FAIL UPDATE"
        ],
        "answer": "ON VIOLATION DROP ROW",
        "explanation": (
            "La pieza angular subyacente para colmatar orgánicamente a nivel operativo y sellar orgánicamente la laguna es ON VIOLATION DROP ROW. Moldeando férreamente y blindando todo albergado transaccionalmente a título de: CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW. Esto garantiza que cualquier anomalía operativa recabada u obscura nulidad sobre 'id' será cercenada, mutilada y excluida sin trabas del embudo pipeline, a pesar de que este revés quede impunemente registrado a expensas puras de salvaguardar un reporte analítico fidedigno subyacente u estadístico delatando estas trabas burlando mermar o aniquilar ciegamente a toda labor perenne subyacente y base de todo el operativo o job subyacente.\n\n"
            "Apunte: Como apostilla subyacente a la par de un apunte orgánico, se atestigua por voz de Databricks la reciente apertura asimilándolo a formato Open Source empujándolo forzadamente asimilado de lleno hacia el Apache Spark ecosistema abanderado al título de Spark Declarative Pipelines (SDP)."
        )
    },
    {
        "exam": 3,
        "id": "q45_cluster_policies_not_advantage",
        "question": "¿A qué se alude unívocamente o de pleno infiriendo o tachando a base certera o negativa (Not) como NO un punto a favor y virtud base tras el andamiaje regidor y asimilación u acopio de usar cluster policies?",
        "options": [
            "Ceñirse albergando un rol subyacente empujando normativas ciegas afianzando a todo clúster base las provisiones forzadas operando sin fallas en descargas e interconectividad subyacente a librerías predeterminadas (cluster-scoped library installations).",
            "Asegurar y consolidar orgánicamente un arranque idéntico y homogéneo dictaminando variables orgánicas en configuraciones de sistema (system settings), ambiente operativo, sumado orgánicamente al dictamen regulador y pautas base impuestas para Spark configuration.",
            "Controlar o ceñir sin fisuras la caja financiera operando orgánicamente a base delimitadora limitando los desembolsos tope base por ente u originarios (maximum cost).",
            "Someter y encuadernar forzosamente y cronométricamente bajo calendario férreo a arrancar y mermar o abortar en periodos ciegamente asignados a entornos orgánicos (clusters)."
        ],
        "answer": "Someter y encuadernar forzosamente y cronométricamente bajo calendario férreo a arrancar y mermar o abortar en periodos ciegamente asignados a entornos orgánicos (clusters).",
        "explanation": (
            "Someter al cronómetro el encendido orgánico a la par de un frenazo en seco y cese operativo a ciclos de tiempos rígidos a base orgánicos o cluster operations carece orgánicamente de sustento u operabilidad asimilados frente a un pilar cimentado como las denominadas 'cluster policies'.\n"
            "El peso colosal de aplicar y emplear orgánicamente tales directrices radican su fortaleza cimentando o consolidando férreamente reglas rectoras u homogéneas forzadas en normativas base para la provisión u encaje operando sin mermas, rigiendo descargas y bibliotecas forzosas e interconectadas a todo despliegue, velando por asentar techos orgánicos económicos (costos) sumado orgánicamente al dictamen sobre bases restrictivas de originación e inicio a título de 'defaults'."
        )
    },
    {
        "exam": 3,
        "id": "q46_delta_sharing_d2d_vs_ods",
        "question": (
            "Un grupo perito en analítica data engineering desea apoderarse imperiosamente explotando el recurso e hilos comunicadores de base compartida subyacente o modelo (Delta Sharing). Estancándose orgánicamente dudando frente al dilema y disyuntiva referida a cimentarse sobre un enfoque directo a título de 'Databricks-to-Databricks sharing' (D2D) versus la variante libre e interconectada en base a protocolo orgánico 'open Delta Sharing protocol' (ODS).\n\n"
            "Del abanico predefinido u orgánico, ¿qué postulado disecciona o desenmaraña fidedignamente esta ambivalencia separando operando a título fidedigno entre D2D versus ODS?"
        ),
        "options": [
            "La vertiente u operativo base de 'Databricks-to-Databricks sharing' (D2D) acapara y se rige orgánicamente apoderándose del ecosistema de base en declive o herencia 'Hive metastore', dejando paso libre y arropando sin miramientos en su sustitución al Open Sharing protocol (ODS) asimilándolo colindantemente anclado férreamente operando en Unity Catalog abrigando de cara al futuro los proyectos en ciernes.",
            "La vertiente u operativo base de 'Databricks-to-Databricks sharing' (D2D) propulsa interconexiones ciegamente a base libre sin trabas ante terceros ecosistemas foráneos soportados a expensas de compatibilidad u organicidad con base del estándar abierto 'Delta Sharing open standard', dejando por el revés o contrapeso a la variante Open Sharing protocol (ODS) asumiendo la barrera exclusiva de comunicación solo y únicamente reservada entre homónimos o clientes operando orgánicamente bajo dominios privativos Databricks.",
            "La vertiente u operativo base de 'Databricks-to-Databricks sharing' (D2D) instiga, acapara y ampara un uso exclusivo monopolizando el canje y nexo relacional circunscribiéndose celosamente ciegamente solo a los márgenes privativos o interclientes dentro del feudo Databricks (Databricks clients). Paralelamente la apertura o contrapeso a título del 'Open Sharing protocol' (ODS) ampara sin barreras permitiendo ciegamente que toda matriz externa operante que abrace el decálogo y estándares asimilados libres del 'Delta Sharing open standard' acceda compartiendo de forma recíproca a los insumos y depósitos o compartimentos de base.",
            "La vertiente u operativo base de 'Databricks-to-Databricks sharing' (D2D) limita y somete inmiscuyéndose ciegamente u operativamente compartimentando acervo de datos asimilados en rangos restringidos exclusivamente amparando al 'managed tables', evadiendo a título general la amplitud u orgánicos cimientos o variables contemplados o permitidos operativamente asimilables en el Open Sharing protocol (ODS) apoyando simultáneamente tanto a managed como external tables."
        ],
        "answer": "La vertiente u operativo base de 'Databricks-to-Databricks sharing' (D2D) instiga, acapara y ampara un uso exclusivo monopolizando el canje y nexo relacional circunscribiéndose celosamente ciegamente solo a los márgenes privativos o interclientes dentro del feudo Databricks (Databricks clients). Paralelamente la apertura o contrapeso a título del 'Open Sharing protocol' (ODS) ampara sin barreras permitiendo ciegamente que toda matriz externa operante que abrace el decálogo y estándares asimilados libres del 'Delta Sharing open standard' acceda compartiendo de forma recíproca a los insumos y depósitos o compartimentos de base.",
        "explanation": (
            "Se distinguen y diseccionan colindantemente un binomio base para desplegar operaciones y acervo interconectado (Delta Sharing):\n\n"
            "1- Databricks-to-Databricks sharing (D2D): Adecua un nudo asimilado ciegamente en tu propio Unity Catalog para irradiar frente o ante perfiles usuarios afines dotados o sustentados igualmente con un espacio UC (Unity Catalog-enabled Databricks workspace).\n\n"
            "Se forja a base empírica recayendo sobre raíles de un backend o esqueleto operante y de facto del servidor 'Delta Sharing server' imbricado forzosamente y de origen sobre las entrañas nativas en la base matriz Databricks prestando a su paso orgánicamente prebendas colosales de base a Notebook sharing, trazabilidad Unity Catalog data governance, sumando blindajes de inspección o logs auditables empíricos sobre trazas operativas rindiendo cuentas entre el dúo o ejes base de emisor y emisor de receptores.\n\n"
            "2- Databricks open sharing protocol (ODS): Cede a base genérica asimilándolo a los foráneos el manejo e inventario recabado desde tu cueva de Unity Catalog permitiendo escrutinio desde herramientas y perfiles externos u operadores sin raíces sobre Databricks de origen.\n\n"
            "Opera simultáneamente orgánicamente enraizado sobre los andamios primigenios operacionales asimilados de origen bajo el 'Delta Sharing server' imbricados forzosamente nativos al Databricks obsequiando y empujando una ventana asimilable de utilidad forzosa operando como nudo relacional en el caso exento y particularizado enfocado de lleno en repartir a entes externos al amparo o uso de la carencia unívoca o marginación operativa de cuentas, cimientos o despliegues operativos sobre Unity Catalog-enabled Databricks workspace.\n\n"
            "Corolario y conclusión base: El D2D exprime su jugo agilizando de manera sinérgica al interior orgánico base operando ciegamente a título y marco perenne Databricks ecosystem. Por contraste y contrapartida el ODS se extiende irradiando flexibilidad de tú a tú en lo operacional apuntando y entrelazando compatibilidad con nudos foráneos dotados operando o asimilando su marco abierto (open Delta Sharing protocol)."
        )
    },
    {
        "exam": 3,
        "id": "q47_autoloader_schema_evolution",
        "question": (
            "Un data engineer ha confeccionado orgánicamente moldeando en su base las canalizaciones transaccionales u operativo de flujo continuo empujándolo orgánicamente sobre el motor de base (Databricks Auto Loader):\n\n"
            "spark.readStream \\\n"
            '    .format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            '    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema") \\\n'
            '    .load("/mnt/incoming_data") \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", "/mnt/checkpoints/data") \\\n'
            '    .start("sales_data")\n\n'
            "¿Cuál a tenor empírico, colindantemente se vislumbra como un comportamiento reaccionario o rebote colosal estricto transaccionalmente de asomarse inmiscuyéndose en el lote (files JSON) a base de eslabón inesperado (nueva variable) burlando lo establecido de antaño ignorándose al schema original (original schema)?"
        ),
        "options": [
            "Fenece de lleno arrojando falla el pipeline, derivando orgánicamente y resguardando empíricamente de lleno el contenido a salvaguardar encubierto de pleno asimilándolo a título orgánico o contenedor (rescued data column) cediendo paso subyacentemente al escrutinio pospuesto.",
            "Penece el sistema o estalla orgánicamente el andamio cesando drásticamente el torrente en streaming el cual renuncia orgánicamente al rearme careciendo su reinicio inoperable amparado u operando a base ciega omitiendo un update manual u orgánico a título de un 'manual update' desmembrando o expurgando de origen su traba subyacente (problematic data file).",
            "Sufre un parón abrupto o quiebre a su paso empujando a un renacimiento, reanudando mecánicamente tras enmendar ciegamente de modo empírico la metamorfosis asimilando las lagunas operativas y adaptando un engarce o asimilándolo (schema con las new columns).",
            "Muestra rebote ciego u orgánico con frenazos efímeros escarbando de forma fugaz, rearmándose paulatinamente a expensas de la obviedad ciega en lo operativo (ignoring the new columns) zafándose y burlando al dictamen regulador u omisión plena operando sin alteración de su base rígida (without schema update)."
        ],
        "answer": "Penece el sistema o estalla orgánicamente el andamio cesando drásticamente el torrente en streaming el cual renuncia orgánicamente al rearme careciendo su reinicio inoperable amparado u operando a base ciega omitiendo un update manual u orgánico a título de un 'manual update' desmembrando o expurgando de origen su traba subyacente (problematic data file).",
        "explanation": (
            "Anclándose y amparándose de base forzosamente u oprimiendo las cláusulas ciegamente al modo 'failOnNewColumns', la represa operante o torrente en base orgánica se delata inmiscuyéndose y rebotando de bruces (fails immediately) asimilándolo todo de cara e interponiendo a pulso base normativa o muro empírico innegociable a base del 'strict schema consistency'. Se aborta su propia o mecánica propulsión evadiendo todo latido renaciente operante a no ser a título explícito u obligado o intervención donde operando al rescate (manually updated) abarque y supla a su nueva progenie u organicidad o aborde extirpar las disonancias (files) que originaban tan desdichado abismo o brecha. Bloqueando por ello a su modo todo intento u amalgama fantasma 'silent schema drift', cerciorándose orgánicamente de imponer peritaje u rigor férreo (deliberate schema management)."
        )
    },
    {
        "exam": 3,
        "id": "q48_delta_sharing_identifier",
        "question": (
            "En medio de las gestiones y el afianzamiento o enhebrado y acoplamiento (setup) asimilándolo en un contexto u operativo 'Delta Sharing' forjando alianzas recabando y extendiendo una pericia foránea (external partner), el operador o ingeniero le impone y le interpela orgánicamente e inquisitorialmente asimilando datos con título de 'sharing identifier'.\n\n"
            "¿Cuál a base de estos dogmas define u orienta el cauce de la organicidad al término estricto y de acrónimo 'sharing identifier' operando inmiscuyéndose dentro de un marco operativo Databricks-to-Databricks sharing?"
        ),
        "options": [
            "Funge de parapeto u operativo arrojando base a título encriptado sirviendo orgánicamente forjándose con llave asimétrica operando a ciego durante un torrente transaccional y vaciado apuntalando en los reservorios foráneos operacionales del compañero (partner's tables).",
            "Provee de señal operante desvelando de origen un rastro orgánico subyacente referido al paradero informático o cota (IP address) en afán de proveer acceso selectivo sorteando murallas bajo escudo perenne o firewall whitelisting.",
            "Desempeña y rinde cuentas sirviendo a las credenciales o eslabón (authentication token) enlazando orgánicamente las llamadas ciegamente orientadas en el operativo a base relacional de un puente u endpoint ajeno albergado receptor.",
            "Provee de placa operante asimilando rango y exclusividad (unique reference) forjando e incrustándose un hilo asimilado y con nexo operante a un Unity Catalog metastore afianzado a la cuenta y receptora foránea o recipient's."
        ],
        "answer": "Provee de placa operante asimilando rango y exclusividad (unique reference) forjando e incrustándose un hilo asimilado y con nexo operante a un Unity Catalog metastore afianzado a la cuenta y receptora foránea o recipient's.",
        "explanation": (
            "El susodicho (Delta Sharing identifier) funge a título único amparando a modo string asimilando e incrustándose de base y de rastro en el Databricks-to-Databricks sharing abarcando un escrutinio asimilando origen sobre Unity Catalog metastore ajeno. Semejante y referencial placa arrojará directrices permitiendo al dador asimilado transaccionalmente (data provider) franquear paso posponiendo traba alguna operando asimilado al acceso con los bultos referenciados.\n\n"
            "Dicha placa de la organicidad o (sharing identifier) se escudriña mediante rangos forzosos u formato:\n"
            "<cloud>:<region>:<uuid>\n\n"
            "Ejemplo referencial:\n"
            "aws:us-west-2:10a8dbea-54bc-43ad-87de-0320b91cb818\n\n"
            "Amparando y abocándose a título orgánico:\n"
            "- 'aws' se referirá acatando rango nube matriz (Amazon Web Services).\n"
            "- 'us-west-2' apuntalando coto forzoso físico acotado asimilado orgánicamente al AWS region.\n"
            "- '10a8dbea-54bc-43ad-87de-0320b91cb818' engrosa operando al identificador general UUID de pleno derecho del Unity Catalog metastore del ente o compañero.\n\n"
            "A tenor y por vía libre, tal camarada extrae de modo propio su placa y código de la base transaccional acudiendo a su ecosistema de origen Databricks valiéndose de su visor u orgánico 'Catalog Explorer' operando al correr y tirar del hilo 'SQL query' asimilando 'SELECT CURRENT_METASTORE()'. Acto seguido tal placa es transferida ciegamente a título y entrega base para su portador 'data provider', quien armará y desplegará en su nido a un beneficiario (recipient) abriéndole y cediéndole paso expidiendo (shares)."
        )
    },
    {
        "exam": 3,
        "id": "q49_materialized_view_definition",
        "question": (
            "¿Cuál ente asimilado u organicidad subyacente cobra vigor en la presente premisa?\n\n"
            '"Un artefacto u objeto matriz almacenando de manera forzosa el jugo o fruto precomputado asimilándolo transaccionalmente derivado orgánicamente en un query results, auto-manteniéndose al día o espoleándose a plazos (schedule) paliando y dotando al sistema o mermando lagunas de agilidad paliando con carga de arrastre a modo engorroso (complex aggregations) apuntalando cimientos operativos a ecosistemas BI workloads"'
        ),
        "options": [
            "Materialized view",
            "Temporary view",
            "Standard view",
            "Streaming table"
        ],
        "answer": "Materialized view",
        "explanation": (
            "Se forja a título orgánico retratando ciegamente a una materialized view. Inmiscuyéndose en el operativo 'Databricks SQL', un 'materialized views' rigen operando de lleno al amparo y albergando o incrustándose en tablas regidas UC (Unity Catalog managed tables) incrustando y salvaguardando de modo fidedigno la herencia a base transaccional u operativa de una 'query'. Alejándose de su antítesis en la contraparte u 'standard views', forjada a título efímero operando a un compás ciego exento o aéreo (on-the-fly), recabando un acopio subyacente o 'cache' de acervo regido a un volcado o resguardo orgánico ciegamente y recabando trazas actualizándose paulatinamente según ceda organicidad colindante operando a su regazo (source tables) de modo programado o engranajes espontáneos automáticos (schedule or automatically). Rebuscando operativamente de base asimilada amortizando y paliando ciegamente cálculos densos u hostiles que merman agilidad, los 'Materialized views' merman de pleno latencias (latency) cediendo respiros al entorno de recursos o consumo originario. Alentando de pleno operando organicidad sobre mermas o ahogos matemáticos masivos empujando y acelerando visores y tableros base albergando o colmando metas de los llamados BI dashboard performance.\n\n"
            "Resumiendo de manera encapsulada a tenor ciego: una materialized view atesora de lleno y empíricamente:\n"
            "- Acopio en la parte estructural de registros u acervos (query).\n"
            "- Goza orgánicamente en vigor de su purga u regeneración automática y agendada.\n"
            "- Moldeado a ciegamente en afianzar cálculos a modo titánico aupando al BI (business intelligence) workloads.\n\n"
            "El por qué al desechar los entes adyacentes a título de contraste:\n"
            "- Standard view: Renuncia operando estructuralmente o ciegamente de poseer masa o asimilando trazos y datos, conformando sólo espejismo u invocación textual ciegamente guardada.\n"
            "- Temporary view: Rige su estadía confinándose a base y soplo de sesión cediendo todo amarre de persistencia (no persistent).\n"
            "- Streaming table: Engulle de forma secuencial operando de base colindantemente trazos de arrastre o carga (ingests data), esquivando y distanciándose amparando un uso colindante y diferente de operar ciegamente a pre-masticar y asimilar jugos masivos empujándolos subyacentemente 'precomputed query results'."
        )
    },
    {
        "exam": 3,
        "id": "q50_integration_testing_definition",
        "question": "¿De esta terna y abanico de pautas, cuál acota orgánicamente en la directriz a título de pericia asimilada y afianzada como un 'Integration Testing'?",
        "options": [
            "Funge de andamio o metodología abocándose e interrogando empíricamente vínculos o nudos comunicantes (interaction) asimilando peritaje sobre los brazos y ramales (subsystems) asegurando u arrojando veredictos amparando ensambles forzados (modules work properly as a group).",
            "Mecánica orientada de lleno escudriñando orgánicamente operando y asegurándose blindando a las particularidades (features) rindiendo cuentas asimiladas forzando su escrutinio ciego 'business requirements'.",
            "Método enfocado asimilando a operarios forzando y enraizando simulacros (simulate) de operativas externas (user experience) escrutando organicidad ante tempestades o embudos operativos y asimilando rutinas al pie del real-world escenarios.",
            "Protocolo orgánico encauzando su esmero analizando asimilando un microcosmo u operar atomizado (individual units) arrojando certeza y firmeza colindante paliando con futuras transmutaciones (new changes) paliando todo revés sobre el rastro asimilado base operando en lo futuro."
        ],
        "answer": "Funge de andamio o metodología abocándose e interrogando empíricamente vínculos o nudos comunicantes (interaction) asimilando peritaje sobre los brazos y ramales (subsystems) asegurando u arrojando veredictos amparando ensambles forzados (modules work properly as a group).",
        "explanation": (
            "El afamado o regidor denominado como 'Integration Testing' funge de operario supervisor interrogando a título orgánico los lazos u nexos interconectantes entrelazando engranajes 'subsystems'. Sometiendo a juicio orgánico y transaccional corroborando que los retazos o pilares de software operan lógicamente acoplados rindiendo un sumario base (tested as a group)."
        )
    },
    {
        "exam": 3,
        "id": "q51_databricks_bundle_generate",
        "question": (
            "El operario al mando maneja y gestiona a título operativo un Databricks job originario con su organicidad, anhelando a estas alturas abarcar y subsumirse operando de lleno con los comandos y riendas del Databricks Asset Bundles. Alentándose apoyándose ciegamente a lomos de los CLI (Databricks CLI) para recabar transaccionalmente su envoltorio regidor base a YAML definition paliando colindantemente descargas de asimilados 'referenced artifacts'.\n\n"
            "Acudiendo al manual o abanico normativo base operando de lleno, ¿cuál directriz obrará al amparo del operario para subsanar este trámite ciego orgánico?"
        ),
        "options": [
            "databricks bundle generate job --existing-job-id",
            "databricks bundle clone job --existing-job-id",
            "databricks bundle get job --existing-job-id",
            "databricks bundle download job --existing-job-id"
        ],
        "answer": "databricks bundle generate job --existing-job-id",
        "explanation": (
            "El decreto orgánico e infalible asume orgánicamente o se asienta de lleno: 'databricks bundle generate', dado que consiente ciegamente obrando y fabricando la base arquetipo o entramado 'bundle configuration' frente a moldes ya concebidos u obrando de origen en tus fueros Databricks workspace. Esta faena de base u orgánica cincelará forjando el armazón subyacente (YAML definition) amparando y cubriendo de cimientos todo un conjunto abarcando 'job, pipeline, or dashboard' proveyendo de manera simultánea descargas orgánicas colindantes arropadas ciegamente a su vera o como notebooks o similares referenciados (referenced artifacts)."
        )
    },
    {
        "exam": 3,
        "id": "q52_drop_external_table",
        "question": (
            "Operando en su feudo incondicional el escuadrón operativo ampara y cimenta a base empírica forzando originar el Delta Lake table escarbando a título de query:\n\n"
            "CREATE TABLE customers_clone\n"
            "LOCATION 's3://my-bucket/'\n"
            "AS SELECT * FROM customers\n\n"
            "Emergiendo al paso, el perito en data engineer demanda de lleno y subyacentemente fulminar o descabezar el table rindiendo cuentas esgrimiendo la sentencia:\n\n"
            "DROP TABLE customers_clone\n\n"
            "¿Bajo qué potestad orgánica reluce o describe ciegamente el resultado tras escupir y asimilar semejante accionar o 'drop command'?"
        ),
        "options": [
            "Colapsa o fenece orgánicamente arrojando un fallo asimilado al nexo originario del que emana a título de 'shallowly cloned' arrancado o forjado del 'customers table'.",
            "Exclusivamente perecerá orgánicamente fulminada la organicidad o catalogación subyacente 'table's metadata' abocándose a quedar libre el rastro físico a modo de 'data files' indemnes en su cuna 'storage'.",
            "Queda perenne o colgada temporalmente esquivando el aniquilamiento de origen hasta la invocación forzada u orgánica asimilando al verdugo final de lleno u operativo (VACUUM command).",
            "La guillotina asimila y condena ciegamente operando simultáneamente aniquilando de golpe tanto la cúpula base (metadata) como cimientos (data files)."
        ],
        "answer": "Exclusivamente perecerá orgánicamente fulminada la organicidad o catalogación subyacente 'table's metadata' abocándose a quedar libre el rastro físico a modo de 'data files' indemnes en su cuna 'storage'.",
        "explanation": (
            "Bajo un dictamen operativo asimilado operan las tablas forasteras u originarias libres conocidas como 'External (unmanaged) tables' anidando su acervo e inventario en paraísos físicos delimitados (external storage) a base del señalamiento ciego originado (LOCATION clause).\n\n"
            "A golpe y asimilando la orden DROP TABLE arropada de base contra tablas de perfil foráneo u externo, tan solo perecen fulminados registros o apuntes documentales base en la catalogación (table's metadata is deleted), preservando y manteniendo incólumes y a flote los registros e inventarios físicos empíricos salvaguardados en origen ('data files are kept')."
        )
    },
    {
        "exam": 3,
        "id": "q53_inner_join_static_delta_tables",
        "question": (
            "El comando central asume la siguiente dinámica unificadora esgrimiendo el empuje (join logic) trenzando orgánicamente un trío de tablas Delta:\n\n"
            'df_students = spark.table("students")\n'
            'df_courses = spark.table("courses")\n'
            'df_enrollments = spark.table("enrollments")\n\n'
            'df_join_1 = df_students.join(df_enrollments, df_students.student_id == df_enrollments.student_id, "inner") \\\n'
            "    .select(df_students.student_id,\n"
            "            df_students.student_name,\n"
            "            df_enrollments.course_id)\n\n"
            'df_join_2 = df_join_1.join(df_courses, df_join_1.course_id == df_courses.course_id, "inner") \\\n'
            "    .select(df_join_1.student_id,\n"
            "            df_join_1.student_name,\n"
            "            df_courses.course_name)\n\n"
            "df_join_2.write \\\n"
            '    .format("delta") \\\n'
            '    .mode("overwrite") \\\n'
            '    .table("student_courses_details")\n\n'
            "Bajo el escrutinio, ¿cuál directriz orienta y plasma de lleno la reacción o rastro orgánico arrojado de manera consecutiva e inexorable a cada accionar?"
        ),
        "options": [
            "De un barrido ciego, todo registro e inventario amparado en el tiempo presente (current version) de estas cunas y orígenes sucumbe o es asimilado a base colindante de operaciones mixtas (join operations). Depositando como acervo, el grupo o saldo de excedentes o huérfanos infructuosos (unmatched records) aplastando y abocando su carga contra la meta a título de 'students_courses_details table'.",
            "Mermando o recortando ciegamente y aislando a las inyecciones más tiernas y exclusivas de orígenes colindantes (newly added records) encauzándolos o empujándolos a la mezcla (join operations). Depositando en el cuenco a las perlas acopladas a la fuerza e hiladas asimilando su organicidad (matched records) aplastando implacablemente a la receptora final ('students_courses_details table').",
            "Bajo un perfil austero ciegamente y aislando a las inyecciones más tiernas de orígenes colindantes (newly added records) arropándolos orgánicamente a la mezcla. Excretando a su fin a modo asimilable de excedentes de la criba (unmatched records) arrojándolos brutalmente forzando el reemplazo u aplastamiento de la matriz 'students_courses_details table'.",
            "Acudiendo o recabando todo un torrente pleno transaccional escarbando el tiempo real (current version) asimilando de origen sus fuentes y abrigando el cruce (join operations). Desembocando y excretando a posteriori todo emparejamiento sano a base fidedigna (matched records) aniquilando de raíz e imponiéndose arropando de origen al recipiente 'students_courses_details table'."
        ],
        "answer": "Acudiendo o recabando todo un torrente pleno transaccional escarbando el tiempo real (current version) asimilando de origen sus fuentes y abrigando el cruce (join operations). Desembocando y excretando a posteriori todo emparejamiento sano a base fidedigna (matched records) aniquilando de raíz e imponiéndose arropando de origen al recipiente 'students_courses_details table'.",
        "explanation": (
            "El encauce originario de lectura a base ciega asimilando las Delta tables a título estático recurriendo al andamio operativo spark.table() function condena incondicionalmente y fuerza la lectura o criba integral en plenitud (all records) rindiendo cuentas asimiladas operando al dictamen de cada mezcla.\n\n"
            "Careciendo de saltos o brechas transaccionales base entre el mando 'spark.table()' versus un homónimo o afín 'spark.read.table()'. Ya que operando de espaldas o por conductos base el 'spark.read.table()' invoca a nivel oculto a 'spark.table()'.\n\n"
            "Inmiscuyéndose a base de nexos u operativos, la herramienta pyspark.sql.DataFrame.join() forja y aplica su fuerza bruta ciegamente bajo un arquetipo asimilado pre-establecido de unión acoplada 'inner join' escarbando a modo ciego. Destinando o derivando únicamente a las parejas estables orgánicamente asimilables (matched records) canalizándolas a la entidad final u objetivo. Y para asestar la firma final el dictamen asimilado (\"overwrite\") sella todo abocando una aniquilación y supresión fidedigna (overwrites) frente a la receptora final amparando a la 'student_courses_details' en cada ronda y latido base."
        )
    },
    {
        "exam": 3,
        "id": "q54_lakehouse_federation_purpose",
        "question": "¿A qué se atiene como misión absoluta e irrenunciable el empuje subyacente y orgánico de Lakehouse Federation al amparo arquitectónico (data architecture)?",
        "options": [
            "Se forja buscando podar y apaciguar lastres financieros estrujando bultos a modo de merma en peso (compressing data).",
            "Consolida organicidad esculpiendo y amparando blindajes a base de copias espejo o calcos base en el entorno Databricks.",
            "Posibilita e instiga de lleno las travesías o interrogatorios francos sin intermediarios abarcando amalgamas u orígenes exentos de base a forzar u orquestar migraciones físicas perennes.",
            "Abandera o instiga asimilando mudanzas radicales obligando orgánicamente a todos los datos a sucumbir dentro del feudo Databricks bajo un gobierno monopolista."
        ],
        "answer": "Posibilita e instiga de lleno las travesías o interrogatorios francos sin intermediarios abarcando amalgamas u orígenes exentos de base a forzar u orquestar migraciones físicas perennes.",
        "explanation": (
            "Bajo su velo (Lakehouse Federation) rige asimilando o amparando a entes y perfiles u orgánicos peritajes (queries) a extender sus pesquisas arrojando interrogatorios frente o contra colosos o repositorios remotos diversos y dispares—como acervos, bancos (databases), o almacenes—eludiendo a base franca tragar ciegamente obligando al trasiego (physical migration) masivo amparándose inyectándolo a Databricks. Apaciguando y mitigando las copias clónicas redundantes (data duplication), cediendo saltos ciegos a favor de rebajar estrangulamientos y latencias proveyendo la llave orgánica en la meta o cumbre llamada (unified query experience)."
        )
    },
    {
        "exam": 3,
        "id": "q55_delta_clone_modifications",
        "question": "¿De esta terna y abanico de pautas, cuál acota orgánicamente en la directriz a título de verdad asimilada referida u amparando orgánicamente operaciones (cloning tables) albergadas dentro de Databricks?",
        "options": [
            "Incursiones o ediciones efectuadas de bruces sobre las 'shallow clones' se encierran amparando efectos exentos o sin salpicar a la base o molde original. Dejando como contrapartida u asimilando que toda marca sobre los 'deep clones' rigen transmutando ciegamente a la matriz originaria u 'source table'.",
            "Incursiones o ediciones efectuadas de bruces sobre las 'deep clones' se encierran amparando efectos exentos o sin salpicar a la base o molde original. Dejando como contrapartida u asimilando que toda marca sobre los 'shallow clones' rigen transmutando ciegamente a la matriz originaria u 'source table'.",
            "Incursiones o ediciones efectuadas de bruces sobre clones ciegos, tanto sean 'deep' o de rango 'shallow', rigen y se abocan ciñéndose sin excepción o de pleno derecho alterando exclusividad y cerco a su propio ente eximiendo por completo (sin salpicar) a la matriz originaria.",
            "Toda enmienda asimilada ciegamente en clones base ya sean 'deep' o de corte 'shallow' rebota de bruces operando daños u originando enmiendas colindantes a la matriz original (source table)."
        ],
        "answer": "Incursiones o ediciones efectuadas de bruces sobre clones ciegos, tanto sean 'deep' o de rango 'shallow', rigen y se abocan ciñéndose sin excepción o de pleno derecho alterando exclusividad y cerco a su propio ente eximiendo por completo (sin salpicar) a la matriz originaria.",
        "explanation": (
            "Diferenciando exentamente en uno u otro marco regidor (deep or shallow cloning), alteraciones transaccionales amparando de origen una reescritura cimentándose u originándose a expensas de la entidad calcada (cloned version of the table) obrarán rindiendo cuentas asimilándose y preservando a base de apartes estancos sin mezclar rastro con su progenitora (source), paliando o eximiendo todo revés cruzado."
        )
    },
    {
        "exam": 3,
        "id": "q56_delta_auto_optimize",
        "question": (
            "¿Cuál a priori, amparando al escalón estipendiado u organizativo, acata e infiere o ampara de forma franca esta pauta de base?\n\n"
            "\"Una facultad motriz orgánica y subyacente inherente en Delta Lake acorralando y amoldando archivos mínimos (compacts small files) forjándose asimilado orgánicamente durante inyecciones y volcados asimilando trazas solitarias (individual writes) abarcando la matriz esgrimiendo y disparando asimilando de origen una terna o binomio colindante de operaciones (two complementary operations)\""
        ),
        "options": [
            "Auto compaction",
            "OPTIMIZE operation",
            "Optimized writes",
            "Auto Optimize"
        ],
        "answer": "Auto Optimize",
        "explanation": (
            "A título operativo y forjador el comando 'Auto Optimize' acapara la proeza asimilando al Delta Lake a comprimir en lote o acoplar ciegamente menudencias estructurales de tablas matrices de Delta. Forjándose este fin ciegamente en paralelo e incrustándose orgánicamente amparado sobre la marcha y la criba del 'individual writes'.\n\n"
            "Bajo su caparazón se fragua un asimilado binomio y escuadrón complementario:\n"
            "- Optimized writes: arropado bajo esta armadura, Databricks arremete asimilando la salida consolidada y escupiendo volúmenes unificados tasados en rangos de 128 MB atesorando cada recoveco orgánico o partición.\n"
            "- Auto compaction: asumiendo el relevo u escrutinio orgánico transcurrido o escudriñando orgánicamente la traza tras un volcado solitario (individual write), testea orgánicamente posibles amalgamas para encajar o compactar a posteriori. Ante señales propicias se lanza ciegamente a forjar un proceso en segundo plano acatando la directriz y el mandato del 'OPTIMIZE' pero amoldando ciegamente la talla a base de un formato de 128 MB (sorteando o desplazando el techo colosal u oficial del '1 GB file size' abanderado ciegamente a título de estándar)."
        )
    },
    {
        "exam": 3,
        "id": "q57_notebook_scoped_python_wheel",
        "question": (
            "Albergando inquietudes o metas, el perfil técnico transaccional se afana inyectando una base o bulto asimilado (Python wheel) pero ceñido, delimitando su cerco u orgánico perímetro (scoped) exclusivamente a título e inmiscuyéndose en la sesión en vuelo del notebook operante. Salvaguardando a título ciego o amparando a posibles agregados que giren y penden ciegamente atados y subordinados ciegamente a esta matriz.\n\n"
            "Rebuscando operativamente de base asimilada ¿cuál accionar ciego plasma o resuelve este afán de forma directa?"
        ),
        "options": [
            "%fs install my_package.whl",
            "%pip install my_package.whl",
            "%sh install my_package",
            "%python install my_package.whl"
        ],
        "answer": "%pip install my_package.whl",
        "explanation": (
            "Acudir o engarzar bajo este comando con firmeza operando a base '%pip install' otorga la gracia orgánica permitiendo ciegamente el volcado e incrustación de este bulto base (Python wheel) acotando orgánicamente la malla a modo perenne o sesión exclusiva de este notebook. Permaneciendo orgánicamente este núcleo aislado en un feudo disponible pero privativo ciñéndose y amparando a dependientes 'jobs' que beban y subsistan de este entorno o matriz colindante."
        )
    },
    {
        "exam": 3,
        "id": "q58_standard_cluster_access_mode",
        "question": (
            "En medio de un denso u operante escuadrón amparando perfiles de analítica, ansían orgánicamente aunar afanes en proyectos dispares u operantes (analytics project) que demanden orgánicamente escarceos u tanteos a bases ligeras (small datasets), exprimiendo asimilados lenguajes de base colindantes (Python and SQL). A este ruego invocan y reclaman del data engineer un entorno asimilado o plataformas orgánicas interactivas y maleables.\n\n"
            "Con estas acotaciones operacionales ¿a cuál matriz de conexión o esquema base ('cluster access modes') deberá recurrir el arquitecto para nutrir esta demanda sin reparos?"
        ),
        "options": [
            "STANDARD",
            "SINGLE USER",
            "DEDICATED",
            "NO_ISOLATION_SHARED"
        ],
        "answer": "STANDARD",
        "explanation": (
            "Atendiendo el clamor u orgánicos escarceos amparando a múltiples almas o perfiles peritos (data analysts) inmersos amparando de forma concurrente con herramientas (Python and SQL), la elección prudente y matriz acapara asimilando a título 'STANDARD cluster access mode'. Estos frentes operantes (Standard clusters) atesoran orgánicamente el amparo u abrigo para dotar de uso concurrente y comunal repartiendo cargas de base orgánicas (general workloads), forjando a un menor coste a la par de ejercer mallas e insularidades a modo protector (isolating users). Sin mermas asimilando de origen Python y SQL, a la vez evitando desperdicios de base operativa u orgánica sin incurrir a lujos de exclusividad amparando algoritmos complejos (R, MLlib, o RDD-based tasks) carentes en esta encomienda evadiendo a título base un entorno rígido asimilado a Dedicated access mode.\n\n"
            "Cribando el resto u organicidad adyacente:\n"
            "- Dedicated Access: recae de origen a misiones elitistas asimilando orgánicamente perfiles complejos o de grupos ciegos blindados u orgánicamente asimilados.\n"
            "- Single-user clusters: atados operativamente arropando e instaurándose colindantemente en el abanico Dedicated, se rigen asimilando la soledad a modo aislado (isolated operational workloads).\n"
            "- No Isolation Shared clusters: relaja de origen trabas u orgánicos cercos protectores y mermando organicidad de seguridad cediendo al caos sin fronteras a modo multi-usuario no exento de peligros o de brechas ciegas."
        )
    },
    {
        "exam": 3,
        "id": "q59_lakehouse_federation_foreign_catalog",
        "question": (
            "Consolidando e impartiendo base, los arquitectos u operadores asimilaron de lleno de manera exitosa empalmando o enhebrando ciegamente un conducto asimilado a 'mysql_connection' dentro de su base matriz Databricks asimilando organicidad y tendiendo lazos externos (MySQL database). Anhelando a tenor de sus miras u orgánicamente esgrimiendo y exponiendo las matrices (MySQL tables) para el consumo y visor franco a través del cimiento base o matriz general Unity Catalog apoyándose orgánicamente en la muleta del Lakehouse Federation.\n\n"
            "Bajo el escrutinio o amparo del éxito en este eslabón (connection), el siguiente paso o maniobra dictamina asimilar orgánicamente u empujar estas esferas matrices dentro del Unity Catalog para cimentarlas ciegamente al consumo reglado u amparado de pautas normativas (governed and secure).\n\n"
            "¿Cuál directriz orienta y plasma de lleno la reacción o rastro orgánico arrojado de manera inexorable para esta meta en ciernes?"
        ),
        "options": [
            "Gestación asimilada empujando un 'external catalog' definiendo fronteras o amarres asimilando la de origen 'mysql_connection'.",
            "Bautismo o afianzamiento erigiendo un metastore Unity Catalog a la medida forjándolo colindantemente ciegamente con la citada base 'mysql_connection'.",
            "Erigir o empujar asimilando de lleno a título matriz la base (foreign catalog) encajándola colindantemente ciegamente en el seno de Unity Catalog acudiendo o arrastrando a 'mysql_connection'.",
            "Gestación asimilada empujando a la matriz y forjando una base (external table) invocando o colindantemente tirando y referenciando a la base matriz con la de origen 'mysql_connection'."
        ],
        "answer": "Erigir o empujar asimilando de lleno a título matriz la base (foreign catalog) encajándola colindantemente ciegamente en el seno de Unity Catalog acudiendo o arrastrando a 'mysql_connection'.",
        "explanation": (
            "El paso orgánico e ineludible dictamina asimilar a título y moldear de pleno un 'foreign catalog' sumergiéndolo orgánicamente al amparo matriz de Unity Catalog arrastrando ciegamente los amarres cimentados del 'mysql_connection'. Semejante figura (foreign catalog) ejerce operando u orgánicamente asimilando de puente, rastreando u mapeando colindantemente o acaparando fidedignamente cimientos (tables) originarios asimilados a MySQL forjando a base de espejos (foreign catalogs). Cimentando o posibilitando orgánicamente que las peticiones u orgánicos escarceos (queries) desciendan sin fisuras u ahogos empujándolos al sistema origen (pushed down to the source system) amparando seguridad."
        )
    },
    {
        "exam": 4,
        "id": "q01_sql_alert_multiple_columns",
        "question": (
            "Un data engineer de un call center necesita implementar una alerta SQL para monitorizar el volumen de tickets y cambios de estado. "
            "Quieren establecer la alerta basada en múltiples columnas de la tabla de tickets. La alerta debe lanzarse cuando se cumplan a la vez las dos condiciones siguientes:\n\n"
            "1. El número de nuevos tickets supera los 200.\n"
            "2. El número de tickets bajo procesamiento supera los 150.\n\n"
            "¿Cuál de las siguientes SQL queries implementa correctamente la lógica de esta alerta?"
        ),
        "options": [
            "SELECT\n    SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\nFROM tickets\nWHERE new_tickets > 200\n    AND under_processing > 150",
            "SELECT new_tickets + under_processing\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics\nWHERE new_tickets + under_processing > 350",
            "SELECT new_tickets, under_processing\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics\nWHERE new_tickets > 200\n    AND under_processing > 150",
            "SELECT CASE\n    WHEN new_tickets > 200 AND under_processing > 150 THEN 1\n    ELSE 0\nEND\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics"
        ],
        "answer": "SELECT CASE\n    WHEN new_tickets > 200 AND under_processing > 150 THEN 1\n    ELSE 0\nEND\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics",
        "explanation": (
            "La query calcula correctamente las sumas de nuevos tickets e in-progress en una subquery, y luego usa una instrucción CASE para activar la alerta cuando se cumplen las dos condiciones (new_tickets > 200 AND under_processing > 150), lo cual encaja con la lógica pretendida.\n\n"
            "Recuerda, las alertas en Databricks sólo pueden evaluar un campo de forma individual. Es por lo que combinamos en un CASE WHEN expression todas las variables y las aplanamos a una única columna o output evaluable."
        ),
    },
    {
        "exam": 4,
        "id": "q02_print_working_directory",
        "question": "¿Cuál de los siguientes comandos imprime el actual working directory de un notebook?",
        "options": [
            "%sh pwd",
            "os.path.abspath()",
            "print(sys.path)",
            "os.environ['PYTHONPATH']"
        ],
        "answer": "%sh pwd",
        "explanation": (
            "El magic command %sh te permite ejecutar comandos shell nativos directamente en un notebook.\n\n"
            "El comando pwd viene de las siglas y significa print working directory."
        ),
    },
    {
        "exam": 4,
        "id": "q03_pyspark_withcolumn_override",
        "question": (
            "Un equipo de data engineering necesita ceder de forma externa (compartir con un vendor externo) registros que contengan números de la seguridad social (Social Security Numbers) posibilitando ciertas tareas de macheo (matching). Con esto en mente aplican la receta:\n\n"
            'df_masked = df.withColumn("ssn_hash", sha2("ssn", 256))\n'
            'df_masked.write.saveAsTable("masked_analytics")\n\n'
            "Sufriendo una falla, el código peca ciegamente exponiendo los valores en crudo originales.\n\n"
            "Atendiendo a este desliz orgánico, ¿qué postulado disecciona y devela las carencias del código en dicho comportamiento?"
        ),
        "options": [
            "Carece PySpark de orgánicamente asimilar o invocar sha2 function. Quedaría sujeta o condicionada ciegamente forjando la tabla a base relacional vía Spark SQL acudiendo y escudriñando comandos tipo CTAS statement.",
            'Inyecta o escupe orgánicamente un apéndice o columna ajena al df_masked eludiendo reescribir y mermar o pisar a base original. Demanda obrar de lleno cimentándose al pie en withColumn("ssn", sha2("ssn", 256)).',
            'Inyecta o escupe orgánicamente un apéndice o columna ajena al df_masked omitiendo suprimir ciegamente a base original. Exigiendo tras su cola la anexión de una directriz forzosa o .drop("ssn_hash") command.',
            'Cae o carece orgánicamente el sha2 de aplicación o viabilidad en cifras (numerical values). Requerirá forzarse o transmutarse ciegamente empujándolo a dictámenes como withColumn("ssn_hash", md5("ssn")).'
        ],
        "answer": 'Inyecta o escupe orgánicamente un apéndice o columna ajena al df_masked eludiendo reescribir y mermar o pisar a base original. Demanda obrar de lleno cimentándose al pie en withColumn("ssn", sha2("ssn", 256)).',
        "explanation": (
            "Operando de la amalgama de comandos bajo el dominio PySpark, la instrucción withColumn asume de lleno tanto la forja de flamantes apéndices (columnas) o transmutación de entes preexistentes anclándose en reglas expresas (expression). Resulta evidente que en el dictamen u operativa base este arrastra de lleno la forja anidando apéndices de cuajo a la cola (ssn_hash) cediendo u omitiendo borrar de plano a la original y madre 'ssn column', derivando orgánicamente que las cifras primigenias subsistan ciegamente albergadas dentro de la matriz final.\n\n"
            'De cara a sanear orgánicamente las vulnerabilidades asimilando privacidad (masking) precisarán bien mermar o sobreescribir la columna matriz con dictámenes tipo (withColumn("ssn", sha2("ssn", 256))) u optando de forma radical podándola extirpando al amparo del .drop command.'
        ),
    },
    {
        "exam": 4,
        "id": "q04_dynamic_view_permissions",
        "question": (
            "El equipo de data engineering forja y administra la entidad dinámica (dynamic view) con amparo o codificación tal que:\n\n"
            "CREATE VIEW students_vw AS\n"
            "SELECT * FROM students\n"
            "WHERE\n"
            "    CASE\n"
            "        WHEN is_account_group_member('instructors') THEN TRUE\n"
            "        ELSE is_active IS FALSE\n"
            "    END;\n\n"
            "¿Bajo el escrutinio, cuál directriz orienta y plasma de lleno la reacción o rastro orgánico arrojado a cualquiera operando asimilándolo y sometiendo a query esta view?"
        ),
        "options": [
            "Beneficiarios afines a la cofradía o cúpula 'instructors' sondearán de manera restringida o escueta ciegamente lo concerniente a estudiantes activos. Exponiendo y rindiendo a perfiles excluidos ajenos a la cúpula, una visibilidad estricta escudriñando inactivos.",
            "Acudiendo de lleno únicamente los miembros o afiliados a la cúpula 'instructors', otearán sin impedimentos el mosaico asimilado asimilando activos y mermados. Cediendo a los extraños o parias excluidos una pátina ciega u opaca a base de nulos (null values) referidos al bloque de inactivos.",
            "Afiliados o miembros del reducto 'instructors' otearán u operarán sin trabas a través de todos los rincones abarcando alumnos al margen de estatutos activos o nulos. En tanto asimilados ajenos sin galones a la cúpula, se confinarán rindiendo cuentas observando acotadamente perfiles o registros caídos (inactive students).",
            "Exclusivamente afiliados de pleno o cúpula 'instructors' otearán u operarán sin trabas a través de todos los rincones abarcando alumnos. Los de fuera exentos de rangos chocarán escudriñando acotadamente inactivos."
        ],
        "answer": "Afiliados o miembros del reducto 'instructors' otearán u operarán sin trabas a través de todos los rincones abarcando alumnos al margen de estatutos activos o nulos. En tanto asimilados ajenos sin galones a la cúpula, se confinarán rindiendo cuentas observando acotadamente perfiles o registros caídos (inactive students).",
        "explanation": (
            "Asimilando organicidad y exenciones, los adyacentes pertenecientes u orgánicamente asimilados como 'instructors group' dispondrán por decreto operativo del acceso perenne arropados de origen bajo el 'True' como dictamen condicional u orgánico. Como contrapartida, subyacente de este umbral (instructors), los no afiliados u extraños chocarán o acatarán ciegamente topándose empíricamente restringidos a registros o alumnos portando status (is_active = false)."
        ),
    },
    {
        "exam": 4,
        "id": "q05_autoloader_maxbytespertrigger",
        "question": (
            "Inmersos y lidiando a paso de gigante, un ingeniero opera en pos de orquestar el flujo Auto Loader en modo ciego (incrementally) tragando bultos a base cruda de dimensiones colosales (JSON files) desde la nube. Mermando operativamente agilidad sumiendo al micro-batch en letargo prolongado u ocasionales ahogos (memory issues):\n\n"
            "df = spark.readStream \\\n"
            '    .format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            "    .................................... \\\n"
            '    .load("s3://project/source/")\n\n'
            "Ambicionando en pos a mitigar u orquestar de manera asimilada un fraccionamiento ciego operando consumiendo de tajo en tajo el volcado (micro-batch), forjando robustez a la par que cronometra o atina ciegamente los lapsos de carga.\n\n"
            "¿Cuál opción rellena fielmente la directriz dictaminando u obligando a tragar un cupo ciego o ración no superior a 128 MB en cada sacudida o micro-batch?"
        ),
        "options": [
            '.option("cloudFiles.maxBytesPerTrigger", "128mb")',
            '.option("triggerInterval", "128mb")',
            '.option("batchSize", "128mb")',
            '.option("cloudFiles.maxDataPerTrigger", "128mb")'
        ],
        "answer": '.option("cloudFiles.maxBytesPerTrigger", "128mb")',
        "explanation": (
            "Operando en el seno subyacente y base de Auto Loader, la llave o asidero de nombre 'cloudFiles.maxBytesPerTrigger' pone coto u frena los topes absolutos asimilados al procesador o de bultos pesados por micro-batch. Facilitando la agilidad al stream para devorar de forma seriada o incremental grandes moles asimiladas orgánicamente salvaguardando sin mermas los pronósticos del batch."
        ),
    },
    {
        "exam": 4,
        "id": "q06_cli_jobs_list_completed_only",
        "question": "¿De qué abanico regidor se sirve operando ciegamente a título Databricks CLI un ingeniero, para que emerjan en listado únicamente las sacudidas operacionales u runs referidos de un job culminados con medallas de logro o éxito?",
        "options": [
            "databricks jobs list-runs --job-id <job-id> --success",
            "databricks jobs list-runs --job-id <job-id> --completed-only",
            "databricks jobs list-runs --job-id <job-id> --success-only",
            "databricks jobs list-runs --job-id <job-id> --completed-only --success"
        ],
        "answer": "databricks jobs list-runs --job-id <job-id> --completed-only",
        "explanation": (
            "El estandarte y orden correcta esgrimiendo y manejando el Databricks CLI concediendo el asimilado orgánico ciego que arroje resultados acabados o coronados de éxito:\n\n"
            "databricks jobs list-runs --job-id <job-id> --completed-only.\n\n"
            "La bandera u apéndice --completed-only conforma orgánicamente la guía a incluir tan sólo asimilados remates o runs pasados rindiendo frutos. Evadiendo su uso asimilará a título global mezclando escarceos vivos (active) y moribundos u cerrados."
        ),
    },
    {
        "exam": 4,
        "id": "q07_git_folders_collaboration",
        "question": (
            "Una pareja de operarios juniors transitan en comandita amalgamando un proyecto conjunto empleando como matriz de campo orgánicamente un Databricks notebook. Acudiendo o arrastrando ciegamente hasta ahora las facultades originarias u orgánicas de control o revisión (notebook versioning feature). Topándose orgánicamente en paredón ante el caos colectivo al verse solapados o mermados borrándose entre sí (overwritten or lost). Percibiendo este caos, un supervisor perito amparando de origen recomienda soltar los arcaicos versionados dando cabida franca al amparo ciego y orden de las esferas 'Git folders' como timón u guía del proyecto.\n\n"
            "Rebuscando operativamente de base asimilada ¿cuál rinde pleitesía o desgrana de manera convincente el fundamento pro 'Git folders' dejando en tierra firme a las prebendas rústicas de un mero notebook?"
        ),
        "options": [
            "Acaparan ciegamente y extirpan operativamente los nudos ciegos de modo robotizado o automático (resolving merge conflicts), propulsando vertiginosamente de lleno fundiciones o injertos de aportes colindantes esquivando mermas o roces manuales perpetuos.",
            "Ceden y escupen de lleno escarceos artificiales (AI-generated code suggestions) sondeando huellas u orgánicas trazas operativas a expensas ajenas, facilitando y dictando orgánicamente lazos en sincronía.",
            "Prendan y resguardan de modo ciego operando de garante sin fallas que se blinda el acceso permanente frente al último eslabón de modo auto-sincronizado eludiendo en absoluto peajes engorrosos forjando empujones a pulso (commits or pushes).",
            "Conceden la gracia operativa gestando y moldeando eslabones paralelos u organigramas separados (branches), apaciguando roces mortales u originarios o pisoteos colindantes habilitando de facto operativas paralelas y simultáneas a múltiples mandos."
        ],
        "answer": "Conceden la gracia operativa gestando y moldeando eslabones paralelos u organigramas separados (branches), apaciguando roces mortales u originarios o pisoteos colindantes habilitando de facto operativas paralelas y simultáneas a múltiples mandos.",
        "explanation": (
            "La pauta suprema de por qué rigen y arrollan Git folders arrinconando arcaicos modelos asimilando tareas cruzadas (collaborative teamwork) es su naturaleza forjadora moldeando y tutelando ramales u operaciones separadas (branches). En las trincheras de Databricks, las partes clonan orgánicamente la carpeta vinculada con los amarres lejanos (remote Git repository) esculpiendo en ramas exentas sin emborronar líneas vecinales. Semejante táctica blinda e inmuniza de las zancadillas y sobreescrituras acatando trabajos síncronos, abanderando de fondo su verdadero estatus asimilado a puro (version control)."
        ),
    },
    {
        "exam": 4,
        "id": "q08_lakehouse_federation_connection",
        "question": (
            "Dotado y empoderado, un operario data engineer emprende labores forjando pasarelas blindando orgánicamente a entes (analysts) sondear en origen matrices lejanas (PostgreSQL database) acudiendo a Databricks, evadiendo duplicidades y trasiegos de masa. Con miras a encauzar y asentar bajo las mecánicas del Lakehouse Federation o Unity Catalog acaparando un puente franco u (foreign catalog).\n\n"
            "Acudiendo o arrastrando ciegamente los dogmas ¿cuál rige o plasma a fuego el punto o escalafón cero inicial de partida para enarbolar el proyecto?"
        ),
        "options": [
            "Condecorar de inicio y conceder mandatos forzosos u operativos tipo 'CREATE SHARE y CREATE RECIPIENT' ciegamente abrigando orgánicamente a perfiles o tutores administradores.",
            "Moldear a fuego y forjar o configurar una asimilada y sólida vía (connection) dentro de Unity Catalog garantizando amarre de base y envoltura segura acatando al PostgreSQL, despachando operativas credenciales de paso.",
            "Acudir o deambular de origen y sin dudar enraizándose en paneles matrices o consolas rindiendo cuentas operativas bajo el caparazón u opción maestra 'Allow Delta Sharing with parties outside your organization'.",
            "Sellar y fijar o acotar y parametrizar lindes subyacentes operativas tipo 'external location' y blindajes financieros o avales frente a la guarida y fosa de storage subyacente del PostgreSQL."
        ],
        "answer": "Moldear a fuego y forjar o configurar una asimilada y sólida vía (connection) dentro de Unity Catalog garantizando amarre de base y envoltura segura acatando al PostgreSQL, despachando operativas credenciales de paso.",
        "explanation": (
            "La meta originaria de salida o pistoletazo dictamina u obliga de facto a configurar orgánicamente y con garantías una conexión (connection) regida operando ciegamente bajo amparo Unity Catalog forjando asilo frente a los embudos externos o matriz de datos PostgreSQL, atando orgánicamente requerimientos u accesos normativos. Paso incuestionable originariamente previo a fraguar y dar luz o vida a catálogos exentos, Databricks ruega y solicita operativamente u demanda eslabón sólido e inmunizado para asimilar y procesar mermando fugas abarcando consultas forasteras fluidamente."
        ),
    },
    {
        "exam": 4,
        "id": "q09_foreachbatch_merge_deduplication",
        "question": (
            "El operario al mando maneja y gestiona a título operativo la maquinaria encauzando a la criba los gemelos o colindantes Spark Structured Streaming:\n\n"
            "spark.readStream \\\n"
            '    .table("orders") \\\n'
            '    .selectExpr("from_json(CAST(value AS STRING), \'...\') as value") \\\n'
            '    .select("value.*") \\\n'
            '    .withWatermark("order_timestamp", "30 seconds") \\\n'
            '    .dropDuplicates("order_id", "order_timestamp") \\\n'
            "    ...\n\n"
            "Mermando su fiabilidad nota empíricamente orgánico este cimiento escaso abarcando a título franco los gemelos perezosos y colindantes rebasando e interponiéndose por fuera del (watermark threshold).\n\n"
            "Operando de la amalgama de retales subyacentes ¿cuál extracto o remiendo de base de código embutido ciegamente o subsumido orgánicamente a título 'foreachBatch function' suple y sella en grado superlativo esta merma asimilando trazos en streaming?"
        ),
        "options": [
            "APPLY CHANGES INTO orders_silver c\nFROM microBatch\nON order_id, order_timestamp\nCOLUMNS *",
            "COPY INTO orders_silver\nFROM microBatch\nDISTINCT ALL\nCOPY_OPTIONS ('mergeSchema' = 'true')",
            "MERGE INTO orders_silver c\nUSING microBatch s\nON s.order_id=c.order_id AND s.order_timestamp=c.order_timestamp\nWHEN NOT MATCHED THEN INSERT *",
            'spark.readStream\n  .table("microBatch")\n  .withWatermark("order_timestamp", "7 days")\n  .dropDuplicates("order_id", "order_timestamp")'
        ],
        "answer": "MERGE INTO orders_silver c\nUSING microBatch s\nON s.order_id=c.order_id AND s.order_timestamp=c.order_timestamp\nWHEN NOT MATCHED THEN INSERT *",
        "explanation": (
            "Buceando en operativas Structured Streaming, la tijera u poda dictada por el 'dropDuplicates' uncida u afianzada colindantemente con 'watermark' ejerce mutilación extirpando clones venideros orgánicamente dentro de una ventana viva o umbrales (event-time threshold), ejemplarizándolo con el rastro o margen asimilado a los 30 sec ('order_timestamp'). Pero cediendo al fracaso ante perfiles letárgicos o lerdos catalogados ciegamente pasados en fecha ('too late'). Reafianzando el blindaje global o aniquilamiento (end-to-end deduplication) el sumidero y pozo final (foreachBatch sink) apela de lleno esgrimiendo y fundiéndose forjando la vía idónea blindada e incorruptible (idempotent write pattern) al amparo del MERGE transaccional.\n\n"
            "Dicha traza 'MERGE INTO' evalúa orgánicamente sopesando de cara y a cara al remolque asimilado crudo ('microBatch') escrutándolo frente a los cimientos o matriz receptora Delta ('orders_silver') fijándose ciegamente u escarbando por sus llaves referenciales o de base ('order_id' and 'order_timestamp'). Cedido el avance sólo escupe u transfiere las perlas inéditas evadiendo pisoteos e ignorando asimilados o calcados, ahuyentando solapes intermitentes entre lotes (micro-batches) a la vez que caza o aparta al rezagado tardío. Fusionando a base este tándem y engranaje o tijera volátil en flujo real-time y el colador estático final MERGE aportando solera y pureza."
        ),
    },
    {
        "exam": 4,
        "id": "q10_spark_ui_sql_dataframe_tab",
        "question": "¿De esta terna y abanico de pautas, cuál acota orgánicamente en la directriz a título fidedigno de la lengüeta u operativo 'SQL/DataFrame tab' alojado en el visor Spark UI?",
        "options": [
            "Provee asimilando en base y aglutinando los historiales colindantes (Spark jobs) arrojados o sumidos con marcas y estampas cronometradas (start and end times), estado orgánico o trazas atadas de escalones ('stages'), y sumarios fraccionados evaluando métricas unitarias a título micro de 'task performance'.",
            "Muestra operando o revelando en pleno fragor las transacciones asimiladas, desmenuzando o revelando las estrategias orgánicas o planes ('query plans'), pautas o ritmos asimilados ('execution metrics'), esquemas crudos lógicos o esqueleto físico ('physical and logical plans'), mapas operativos u esquemáticos ('DAG visualizations'), y resúmenes fragmentados de estadísticos depurando las dolencias u escollos.",
            "Desmenuza en abanico abarcando operativamente todas las matrices e inventarios orgánicos tipo (RDDs and DataFrames) colindantemente asimilados u encubiertos anclados a memoria o enraizados al SSD (on disk), perfilando la tara (size) u almacenamiento orgánico asimilando coordenadas base ayudando al peritaje y poda o optimización del 'caching'.",
            "Acapara en visión colosal o en perspectiva escarbando los escalafones colindantes ('stages'), apuntalando en lazos jerárquicos o de base, escarbando tiempos asimilados a rutinas microscópicas ('task execution times'), barajando u operando y contabilizando los cruces e influjos base a título de lectura u escritura (shuffle read/write metrics)."
        ],
        "answer": "Muestra operando o revelando en pleno fragor las transacciones asimiladas, desmenuzando o revelando las estrategias orgánicas o planes ('query plans'), pautas o ritmos asimilados ('execution metrics'), esquemas crudos lógicos o esqueleto físico ('physical and logical plans'), mapas operativos u esquemáticos ('DAG visualizations'), y resúmenes fragmentados de estadísticos depurando las dolencias u escollos.",
        "explanation": (
            "El panel operativo u solapa 'SQL/DataFrame tab' inmerso orgánicamente ciegamente en Spark UI concentra su potencia acatando sin miramientos a las directrices amparadas a base Spark SQL y DataFrames erigiendo o ayudando a diseccionar, amparar u orientar o rastrear (debugging, monitoring) tramas operacionales farragosas. Su despliegue abarca detalladamente las pericias u orgánicas consultas y eslabones englobando: Planes a nivel esquemático lógicos (Logical query plans), trazos crudos esquemáticos (DAG visualizations), marcas u operativos base estadísticos ('Metrics for execution stages and tasks') o resúmenes peritos ('Performance statistics') puliendo las aristas operacionales.\n\n"
            "El descarte colindante u orgánico o del resto:\n"
            "- Spark Jobs tab: Ofrece historial o lista amparando Spark jobs asimilados en la base operativa...\n"
            "- Stages tab: Indaga a base cimentando un escrutinio asimilado y sesudo ('in-depth view') operando sobre los eslabones u 'stages'...\n"
            "- Storage tab: Desvela en panel asimilando y mostrando todo remanente anclado orgánicamente ('RDDs and DataFrames that are cached or persisted')..."
        ),
    },
    {
        "exam": 4,
        "id": "q11_python_script_as_notebook",
        "question": "¿Cuál rige o instiga unívocamente empujando la transformación orgánica de cara a que un crudo (Python file) torne asimilado u se encarne o bautice con título pleno de 'notebook' operando bajo Databricks?",
        "options": [
            "Invocación asimilada empujando de lleno importando el módulo orgánico 'dbutils.notebook' enraizado en el entramado base (source code).",
            "Gestación asimilada de modo orgánico originando una chispa (spark session) escudriñando u operando a base de 'SparkSession.builder.getOrCreate()' engarzada en su código base.",
            "Empujar un apunte o anotación forzada dictada textualmente '# Databricks notebook source' incrustado a fuego y de inicio asimilando cabeceras orgánicas (first line) del bloque.",
            "Acudir o deambular de origen y sin dudar enraizándose en paneles mágicos (magic command) arropando '%databricks' asimilando organicidad al encabezar las filas originarias (first line)."
        ],
        "answer": "Empujar un apunte o anotación forzada dictada textualmente '# Databricks notebook source' incrustado a fuego y de inicio asimilando cabeceras orgánicas (first line) del bloque.",
        "explanation": (
            "Albergando y arropando sin dilación una enmienda orgánica colindante abarcando 'Python, SQL, Scala, and R scripts' se moldean metamorfoseándolos hacia un ente unitario u orgánico de celda libre (single-cell notebooks) anexionando un apunte o traza forzosa incrustada al cénit operativo (first cell) encuadrada como: # Databricks notebook source"
        ),
    },
    {
        "exam": 4,
        "id": "q12_shallow_clone_vacuum",
        "question": (
            "El destacamento perito transaccional atesora o acapara una matriz asimilada a título 'orders_backup' germinada y moldeada orgánicamente invocando la herramienta subyacente y nativa Delta Lake 'SHALLOW CLONE' arrancada desde una colindante mayor u original 'orders'. De súbito operando a lomos o en las postrimerías recabando queries o cruzando la de 'orders_backup' colapsa y escupe de pleno error delatando vacíos de base física u orfandad colindante (data files are no longer present).\n\n"
            "¿Cuál rige o instiga unívocamente empujando la justificación certera a tan desatinado y crudo revés?"
        ),
        "options": [
            "Ejecución forzada o podado operativo al amparo del OPTIMIZE operó y arrampló en la base madre 'orders'.",
            "Barrido orgánico asimilando la escoba VACUUM operando sin trabas asimilando de lleno mermando la hija 'orders_backup'.",
            "Barrido orgánico asimilando la escoba VACUUM dictaminó y aniquiló operativamente en la matriz u 'orders table'.",
            "Ejecución forzada u orgánica al amparo del OPTIMIZE obró de lleno mermando y arrastrando a la matriz derivada u 'orders_backup'."
        ],
        "answer": "Barrido orgánico asimilando la escoba VACUUM dictaminó y aniquiló operativamente en la matriz u 'orders table'.",
        "explanation": (
            "A la cabeza dispone de las siguientes operativas base: el Shallow Clone forja a base empírica e impone calcando o extrapolando el índice (Delta transaction logs). Concluyendo por tanto la ausencia e inactividad en migraciones forzadas o duplicidad colindante referida al peso base ('data moving').\n\n"
            "Empujar un apunte o mandato ciego operando la tijera ('VACUUM') empujándolo hacia matrices originales ('source table') arramplará podando y cercenando orgánicamente los cimientos anidados y referenciados colindantemente en la traza índice del clónico. A posteriori y tras el estrago te acorralará la falla operativa o desastre en etapa de interrogatorio ('querying the clone') arguyendo huérfanas y ausencias de archivos raíz originarios."
        ),
    },
    {
        "exam": 4,
        "id": "q13_lakeflow_declarative_pipelines",
        "question": (
            "¿Cuál a priori, en relación al suceso subyacente y los dictámenes, o cuál artilugio o engranaje técnico es referenciado u acotado bajo este pretexto?\n\n"
            '\"Andamio transaccional a base u orgánico o marco declarativo (ETL framework) empujando a operar paulatinamente en dosis incrementales mitigando asfixias de mantenimiento (operational overhead) a la par que acapara sin titubeos riendas de la interconectividad entre entes (table dependencies) salvaguardando sin fisuras o mácula la pureza originaria o puridad de cimientos (data quality).\"'
        ),
        "options": [
            "ETL",
            "DAB",
            "DBU",
            "LDP"
        ],
        "answer": "LDP",
        "explanation": (
            "El andamiaje o marco referido responde ciegamente a LDP (Lakeflow Declarative Pipelines). Semejante bastión (LDP) funge en base a moldes pre-establecidos (declarative ETL framework) arropado bajo Databricks erigido ciegamente a engullir operaciones en dosis fraccionadas (incremental data processing) de un modo impoluto evadiendo asfixias por roces manuales (operational overhead). Acaparando organicidad y dotando automatismo a nivel regidor (automatic orchestration) afianzando mallas y nudos jerárquicos y salvaguardando cotas impecables orgánicas y de sanidad de la base 'data quality' de comienzo al colofón."
        ),
    },
    {
        "exam": 4,
        "id": "q14_rest_api_runs_get_structure",
        "question": (
            "El operario al mando o data engineer acude y pulsa orgánicamente las mecánicas del Databricks REST API empujando o escupiendo transaccionalmente y de lleno a base ('GET request') invocando a título franco al umbral originario u endpoint '/api/2.1/jobs/runs/get' rindiendo e instigando un volcado referenciado de apuntes o metadatos acatando y amparando a la matriz polifacética ('multi-task job') cobrándose el botín vía cebo o 'run_id'.\n\n"
            "¿Cuál a tenor empírico, se vislumbra como retrato y esqueleto colindantemente franco y transaccional asimilado a la anatomía o rebote ('response structure') de dicha pulsación o llamada?"
        ),
        "options": [
            "Todo órgano escarbado, u operativo 'task' operando inmiscuyéndose en el lote o 'job run' rinde y adquiere organicidad singular a título de 'orchestration_id'.",
            "Todo órgano escarbado, u operativo 'task' operando inmiscuyéndose en el lote o 'job run' acapara organicidad y placa de rango singular a título de 'run_id'.",
            "Todo órgano escarbado, u operativo 'task' operando inmiscuyéndose en el lote o 'job run' acata y se inviste singular u asimilado a título orgánico 'task_id'.",
            "Todo órgano escarbado, u operativo 'task' operando inmiscuyéndose en el lote o 'job run' ostenta o rinde organicidad amparada singular al 'job_id'."
        ],
        "answer": "Todo órgano escarbado, u operativo 'task' operando inmiscuyéndose en el lote o 'job run' acapara organicidad y placa de rango singular a título de 'run_id'.",
        "explanation": (
            "Todo escalón colindante ('task') que pernocta y rinde labores bajo este abanico ('job run') adquirirá por defecto a título orgánico o base singular un rango operativo 'run_id', llave incuestionable de cara a cazar y capturar su botín u output escudriñando ciegamente a base del '/api/2.1/jobs/runs/get-output'."
        ),
    },
    {
        "exam": 4,
        "id": "q15_structured_streaming_processingtime",
        "question": (
            "Dada la siguiente query de Structured Streaming:\n\n"
            'spark.table("orders") \\\n'
            '    .withColumn("total_after_tax", col("total")*col("tax")) \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", checkpointPath) \\\n'
            '    .outputMode("append") \\\n'
            "    ._________________ \\\n"
            '    .table("new_orders")\n\n'
            "Rellena el espacio en blanco para hacer que la query ejecute un micro-batch para procesar datos cada 2 minutos."
        ),
        "options": [
            'trigger(once="2 minutes")',
            'trigger("2 minutes")',
            'processingTime("2 minutes")',
            'trigger(processingTime="2 minutes")'
        ],
        "answer": 'trigger(processingTime="2 minutes")',
        "explanation": (
            "En Spark Structured Streaming, con el fin de procesar datos en micro-batches a intervalos especificados por el usuario, puedes usar el trigger method processingTime. Esto te permite especificar una duración de tiempo como un string, por defecto, es de 500ms."
        ),
    },
    {
        "exam": 4,
        "id": "q16_query_profiler_top_operators",
        "question": (
            "Un data engineer está usando el Query Profiler en Databricks SQL para investigar una SQL query con un rendimiento lento (slow-performing). Quieren descubrir qué operaciones dentro de la query están tomando más tiempo.\n\n"
            "¿Qué sección del Query Profile resalta las operaciones más costosas (expensive) en la query, ayudando a identificar posibles oportunidades de optimización?"
        ),
        "options": [
            "Query wall-clock duration",
            "Query status",
            "Aggregated task time",
            "Top operators"
        ],
        "answer": "Top operators",
        "explanation": (
            "La respuesta correcta es Top operators. En Databricks SQL, la sección Top operators del Query Profile destaca las operaciones más costosas dentro de una query mostrando qué operaciones específicas (como scans, joins, o aggregations) están consumiendo la mayor parte del tiempo. Esto permite al data engineer señalar con precisión los cuellos de botella de rendimiento (performance bottlenecks) y enfocarse en optimizar las partes de la query que tienen el mayor impacto en el tiempo de ejecución general (execution time)."
        ),
    },
    {
        "exam": 4,
        "id": "q17_delta_lake_file_statistics_average",
        "question": "¿Cuál de las siguientes NO es una Delta Lake File Statistics válida?",
        "options": [
            "El número de null values para cada una de las primeras 32 columnas",
            "El valor mínimo y máximo en cada una de las primeras 32 columnas",
            "El número total de registros en el data file añadido.",
            "El valor promedio (average value) para cada una de las primeras 32 columnas"
        ],
        "answer": "El valor promedio (average value) para cada una de las primeras 32 columnas",
        "explanation": (
            "Delta Lake captura automáticamente estadísticas en el transaction log por cada data file añadido a la tabla. Estas estadísticas indican por archivo: El número total de registros, el valor mínimo en cada columna de las primeras 32 columnas de la tabla, el valor máximo en cada columna de las primeras 32 columnas de la tabla, y el recuento de null values para cada columna de las primeras 32 columnas de la tabla.\n\n"
            "El valor promedio (average) en las columnas no forma parte de las Delta Lake File Statistics."
        ),
    },
    {
        "exam": 4,
        "id": "q18_delta_sharing_auth_difference",
        "question": (
            "Una empresa multinacional quiere compartir datos analíticos de ventas tanto con sus equipos internos de Databricks ubicados en diferentes regiones como con consulting partners externos. Los equipos internos acceden a los datos a través de Databricks-to-Databricks sharing (D2D), mientras que los partners externos utilizan el protocolo open Delta Sharing (ODS).\n\n"
            "En este escenario, ¿cómo difiere la autenticación entre el D2D sharing y el protocolo ODS?"
        ),
        "options": [
            "Databricks-to-Databricks sharing (D2D) usa built-in authentication sin intercambio de tokens, mientras que open Delta Sharing (ODS) requiere autenticación externa vía bearer tokens o OIDC federation.",
            "Databricks-to-Databricks sharing (D2D) y open Delta Sharing (ODS) ambos usan el mismo método de autenticación, así que no hay diferencia.",
            "Databricks-to-Databricks sharing (D2D) se apoya en OIDC federation, mientras que open Delta Sharing (ODS) requiere autenticación vía bearer tokens.",
            "Databricks-to-Databricks sharing (D2D) se apoya en un login unificado con single sign-on (SSO), mientras que open Delta Sharing (ODS) usa login externo con OIDC federation."
        ],
        "answer": "Databricks-to-Databricks sharing (D2D) usa built-in authentication sin intercambio de tokens, mientras que open Delta Sharing (ODS) requiere autenticación externa vía bearer tokens o OIDC federation.",
        "explanation": (
            "Databricks-to-Databricks sharing (D2D) usa autenticación integrada (built-in authentication) sin intercambio de tokens, lo que permite a los equipos internos acceder a los datos compartidos sin problemas dentro del entorno de Databricks, mientras que el open Delta Sharing (ODS) requiere autenticación externa, típicamente vía bearer tokens o OIDC federation, para otorgar a los partners externos un acceso seguro a los datos."
        ),
    },
    {
        "exam": 4,
        "id": "q19_liquid_clustering_optimize",
        "question": (
            "Un data engineer gestiona una tabla Delta Lake con el liquid clustering habilitado. Entienden que el liquid clustering opera de forma incremental, pero no están seguros de cómo hacer un trigger de la operación de clustering cuando nuevos datos son ingestados (ingested) en la tabla.\n\n"
            "¿Cuál de los siguientes comandos se debe ejecutar para clusterizar los datos recién añadidos?"
        ),
        "options": [
            "ANALYZE",
            "ZORDER",
            "VACUUM",
            "OPTIMIZE"
        ],
        "answer": "OPTIMIZE",
        "explanation": (
            "Para clusterizar los datos recién añadidos en una tabla Delta Lake con liquid clustering habilitado, el data engineer debe ejecutar el comando OPTIMIZE. OPTIMIZE hace trigger de la operación de clustering reorganizando físicamente los data files para mejorar el rendimiento de las queries."
        ),
    },
    {
        "exam": 4,
        "id": "q20_all_privileges_excludes_manage",
        "question": "¿Cuál de los siguientes privilegios no está incluido en el permiso ALL PRIVILEGES?",
        "options": [
            "MANAGE",
            "EXECUTE",
            "MODIFY",
            "BROWSE"
        ],
        "answer": "MANAGE",
        "explanation": (
            "El privilegio MANAGE no está incluido en el permiso ALL PRIVILEGES. Mientras que ALL PRIVILEGES concede un conjunto exhaustivo de permisos como EXECUTE, BROWSE y MODIFY, excluye explícitamente MANAGE para prevenir la exfiltración accidental de datos o la escalada de privilegios (privilege escalation).\n\n"
            "Recuerda que MANAGE permite a un usuario ver y administrar privilegios, transferir el ownership, eliminar (drop), y renombrar un objeto. Es similar al object ownership, pero tener el privilegio MANAGE no concede automáticamente todos los demás privilegios sobre el objeto, aunque el usuario puede concederse a sí mismo privilegios adicionales si es necesario."
        ),
    },
    {
        "exam": 4,
        "id": "q21_job_ownership_groups",
        "question": (
            'El equipo de data engineering creó un nuevo Databricks job para procesar datos financieros sensibles. Un analista financiero pidió al equipo que transfiriera el privilegio "Owner" de este job al grupo "finance".\n\n'
            'Un junior data engineer que tiene el permiso "CAN MANAGE" sobre el job está intentando realizar esta transferencia de privilegios a través de la Databricks Job UI, pero sigue fallando.\n\n'
            "¿Cuál de las siguientes opciones explica la causa de este fallo?"
        ),
        "options": [
            'Tener el permiso "CAN MANAGE" no es suficiente para conceder los privilegios de "Owner" a un grupo. El data engineer debe ser el owner actual del job.',
            'El privilegio "Owner" se asigna en la creación del job al creador y no se puede cambiar. El job debe ser re-creado usando las credenciales del grupo "finance".',
            "Los grupos no pueden ser owners de los Databricks jobs. El owner debe ser un usuario individual.",
            'Tener el permiso "CAN MANAGE" no es suficiente para conceder los privilegios de "Owner" a un grupo. El data engineer debe ser un workspace administrator.'
        ],
        "answer": "Los grupos no pueden ser owners de los Databricks jobs. El owner debe ser un usuario individual.",
        "explanation": (
            "Un job no puede tener un grupo como owner. Si intentas establecer a un grupo como el owner de un job, obtendrás el error 'Groups can not be owners'."
        ),
    },
    {
        "exam": 4,
        "id": "q22_databricks_notebook_source_comment",
        "question": (
            "Un data engineer ha notado el comentario '# Databricks notebook source' en la primera línea del source code de cada archivo Python de Databricks pusheado (pushed) a GitHub.\n\n"
            "¿Cuál de las siguientes explica el propósito de este comentario?"
        ),
        "options": [
            "Este comentario hace que sea más fácil para los humanos entender el origen del código generado desde Databricks.",
            "Este comentario añade el archivo Python al search index en el Databricks workspace.",
            "Este comentario es usado por la documentación autogenerada de Python.",
            "Este comentario establece que los archivos Python son Databricks notebooks."
        ],
        "answer": "Este comentario establece que los archivos Python son Databricks notebooks.",
        "explanation": (
            "Puedes convertir scripts de Python, SQL, Scala, y R en single-cell notebooks agregando un comentario en la primera celda del archivo: # Databricks notebook source"
        ),
    },
    {
        "exam": 4,
        "id": "q23_deletion_vectors_update",
        "question": (
            "Un data engineer está trabajando con una tabla Delta Lake grande que tiene deletion vectors habilitados. Teniendo en cuenta la mecánica subyacente de Delta Lake y su manejo de los updates, ¿cuál de las siguientes afirmaciones describe de forma más precisa cómo se comportan las operaciones update dentro del directorio de esta tabla?"
        ),
        "options": [
            "La operación update modifica directamente los Parquet files existentes in place sin crear archivos nuevos.",
            "Cada update desencadena una reescritura completa (complete rewrite) de todos los Parquet files que contienen los datos afectados.",
            "Las filas afectadas son marcadas como eliminadas en los deletion vectors, y las filas actualizadas se escriben como nuevos Parquet files.",
            "Las operaciones de update son ignoradas completamente cuando los deletion vectors están habilitados."
        ],
        "answer": "Las filas afectadas son marcadas como eliminadas en los deletion vectors, y las filas actualizadas se escriben como nuevos Parquet files.",
        "explanation": (
            "Cuando los deletion vectors están habilitados en una tabla Delta Lake, las operaciones de update no reescriben los Parquet files enteros ni los modifican in place. En su lugar, Delta Lake aprovecha los deletion vectors para rastrear eficientemente qué filas han sido eliminadas lógicamente (soft deleted) sin eliminarlas físicamente de los data files. Durante un update, las filas originales que requieren modificación se marcan como eliminadas dentro de los deletion vectors, mientras que las versiones actualizadas de esas filas se escriben como filas nuevas dentro de Parquet files. Este enfoque permite a Delta Lake ejecutar updates y deletes de una manera más eficiente evitando costosas reescrituras de archivos, mejorando el performance, especialmente para grandes datasets, sin dejar de mantener las garantías de transacciones ACID y data consistency."
        ),
    },
    {
        "exam": 4,
        "id": "q24_spark_ui_sql_spill_size",
        "question": "En Spark UI, ¿cuál de las siguientes métricas SQL se muestra en la details page de la query?",
        "options": [
            "Query duration",
            "Spill size",
            "Succeeded jobs",
            "Query execution time"
        ],
        "answer": "Spill size",
        "explanation": (
            "En la Spark UI, la página de detalles de la query muestra información general sobre el query execution time, su duration, la lista de jobs asociados y el query execution DAG.\n\n"
            "Además, muestra métricas SQL en el bloque de los operadores físicos. Las métricas SQL pueden ser útiles cuando queremos profundizar en los detalles de ejecución de cada operador. Por ejemplo, 'number of output rows' es una métrica SQL que actualiza el output después de un operador Filter. 'Spill size', que es el número de bytes volcados (spilled) al disco desde la memoria en el operador."
        ),
    },
    {
        "exam": 4,
        "id": "q25_autoloader_schema_location",
        "question": (
            "Un data engineer ha implementado el siguiente stream de Auto Loader para ingestar (ingest) de manera incremental un gran volumen de archivos JSON desde un cloud storage:\n\n"
            'spark.readStream.format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            "    ._____________________ \\\n"
            '    .load("/mnt/incoming/")\n\n'
            "Por defecto, Auto Loader infiere el schema tomando una muestra (sampling) de los primeros 50 GB o 1000 archivos que descubre. Sin embargo, el data engineer quiere evitar re-sampling y reducir el costo de la inferencia de schema en ejecuciones posteriores (subsequent runs), mientras aún rastrea los cambios del schema a lo largo del tiempo.\n\n"
            "¿Qué opción rellena correctamente el espacio en blanco para cumplir con el requerimiento especificado?"
        ),
        "options": [
            '.option("cloudFiles.schemaLocation", "/path/to/checkpoint")',
            '.option("checkpointLocation", "/path/to/checkpoint")',
            '.option("cloudFiles.schemaEvolutionMode", "addNewColumns")',
            '.option("mergeSchema", true)'
        ],
        "answer": '.option("cloudFiles.schemaLocation", "/path/to/checkpoint")',
        "explanation": (
            'La opción correcta para rellenar el espacio en blanco es .option("cloudFiles.schemaLocation", "/path/to/checkpoint"). Esto le indica a Auto Loader que almacene el schema inferido en la ubicación especificada de modo que las ejecuciones (runs) posteriores no necesiten volver a muestrear los archivos, reduciendo el costo de la schema inference mientras permite seguir rastreando los cambios del schema a lo largo del tiempo.'
        ),
    },
    {
        "exam": 4,
        "id": "q26_rest_api_duplicate_jobs",
        "question": (
            "Un data engineer quería crear el job 'process-sales' usando la Databricks REST API.\n\n"
            "Sin embargo, envió por error 2 peticiones POST al endpoint '/api/2.1/jobs/create'\n\n"
            "¿Qué afirmación describe el resultado de estas peticiones?"
        ),
        "options": [
            'Solo se creará el primer job en el workspace. La segunda petición fallará con un error indicando que un job llamado "process-sales" ya está creado.',
            'Se crearán 2 jobs en el workspace, pero el segundo será renombrado a "process-sales (1)".',
            'Se crearán 2 jobs llamados "process-sales" en el workspace, pero con diferente job_id.',
            "El segundo job sobrescribirá al anterior creado usando la primera petición."
        ],
        "answer": 'Se crearán 2 jobs llamados "process-sales" en el workspace, pero con diferente job_id.',
        "explanation": (
            "Enviar la misma definición de job en múltiples peticiones POST al endpoint '/api/2.1/jobs/create' creará un nuevo job por cada petición, pero cada job tendrá su propio job_id único."
        ),
    },
    {
        "exam": 4,
        "id": "q27_dab_resources_grants",
        "question": (
            "Un data engineer tiene el siguiente proyecto en Databricks Asset Bundle (DAB):\n\n"
            "resources:\n"
            "  jobs:\n"
            "    bookstore_job:\n"
            '      name: "bookstore_job"\n'
            "      # ...\n"
            "  volumes:\n"
            "    bookstore_volume:\n"
            '      name: "bookstore_volume"\n'
            '      catalog: "demo_schema"\n'
            '      schema: "demo_schema"\n'
            '      volume_type: "EXTERNAL"\n'
            '      storage_location: "s3://my-bucket/bookstore/"\n'
            "      grants:\n"
            "        - principal: ${resources.apps.bookstore_app.id}\n"
            "          privileges:\n"
            "            - READ_VOLUME\n"
            "            - WRITE_VOLUME\n\n"
            "¿Cuál de las siguientes describe correctamente el resultado de hacer un deployment de este proyecto DAB?"
        ),
        "options": [
            "Despliega una Databricks App bookstore_app y un Volume bookstore_volume, y concede al Service Principal asociado con la Databricks App permisos de lectura y escritura (read and write access) sobre el Volume.",
            "Genera un error porque la referencia ${resources.apps.bookstore_app.id} es incorrecta y debería ser en su lugar ${resources.jobs.bookstore_job.id}.",
            "Despliega un Catalog demo_catalog, un Schema demo_schema, un Volume bookstore_volume, y una Databricks App bookstore_app con acceso al volume usando un 3-level namespace.",
            "Despliega un Volume bookstore_volume y un Service Principal bookstore_app con permisos de lectura y escritura sobre el Volume."
        ],
        "answer": "Despliega una Databricks App bookstore_app y un Volume bookstore_volume, y concede al Service Principal asociado con la Databricks App permisos de lectura y escritura (read and write access) sobre el Volume.",
        "explanation": (
            "La configuración incluye un bloque de grant que referencia correctamente el identificador de la app usando ${resources.apps.bookstore_app.id}, lo que asegura que al Service Principal asociado a la Databricks App desplegada se le otorguen automáticamente tanto los privilegios READ_VOLUME como WRITE_VOLUME sobre este volume. Esto significa que la service identity de la app puede leer y escribir datos en el bookstore volume como parte de su flujo de trabajo operativo."
        ),
    },
    {
        "exam": 4,
        "id": "q28_cluster_permissions_manage",
        "question": "¿Cuál de las siguientes describe los permisos mínimos que un data engineer necesita para modificar los permisos de un cluster existente?",
        "options": [
            'Cluster creation allowed + privilegios "Can Restart" sobre el cluster',
            'Cluster creation allowed + privilegios "Can Manage" sobre el cluster',
            'Privilegio "Can Manage" sobre el cluster',
            'Privilegio "Can Restart" sobre el cluster'
        ],
        "answer": 'Privilegio "Can Manage" sobre el cluster',
        "explanation": (
            "Puedes configurar dos tipos de permisos para un cluster:\n"
            "1- El permiso general 'Allow cluster creation' controla tu capacidad para crear clusters.\n"
            "2- Los cluster-level permissions controlan tu capacidad para usar y modificar un cluster en específico. Hay cuatro niveles de permisos para un cluster: No Permissions, Can Attach To, Can Restart y Can Manage. Modificar permisos requiere el nivel Can Manage."
        ),
    },
    {
        "exam": 4,
        "id": "q29_ldp_constraint_fail_update",
        "question": (
            "Un data engineer ha definido el siguiente data quality constraint en un pipeline LDP:\n\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ________________\n\n"
            "¿Qué cláusula rellena correctamente el espacio en blanco para detener inmediatamente la ejecución cuando un registro viola este constraint?"
        ),
        "options": [
            "ON VIOLATION FAIL UPDATE",
            "ON VIOLATION FAIL PIPELINE",
            "ON VIOLATION DROP ROW",
            "ON VIOLATION STOP"
        ],
        "answer": "ON VIOLATION FAIL UPDATE",
        "explanation": (
            "La cláusula correcta para rellenar el espacio en blanco es ON VIOLATION FAIL UPDATE, ya que esto asegura que cualquier registro que viole el constraint valid_id evite que la actualización continúe. Esto refuerza un strict data quality y previene el procesamiento en las capas posteriores (downstream) de registros inválidos.\n\n"
            "En este caso, se requiere intervención manual antes de volver a procesar. Cuando un pipeline falla debido a la violación de una expectativa, debes decidir cómo manejar los datos inválidos correctamente antes de hacer un re-run del pipeline."
        ),
    },
    {
        "exam": 4,
        "id": "q30_streaming_ignore_deletes",
        "question": (
            "El equipo de data engineering tiene una gran tabla Delta llamada 'user_posts' que está particionada por la columna 'year'. Esta tabla se usa como input streaming source en un streaming job. La streaming query se muestra a continuación con un espacio en blanco:\n\n"
            "spark.readStream \\\n"
            '    .table("user_posts") \\\n'
            '    .groupBy("post_category", "post_date") \\\n'
            "    .agg( \\\n"
            '        count("*").alias("total_posts_count"), \\\n'
            '        sum("likes").alias("total_likes")) \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", "/path/checkpoint") \\\n'
            '    .table("users_stats")\n\n'
            "Quieren eliminar los datos de los 2 años anteriores de la tabla sin romper el requisito append-only de los streaming sources.\n\n"
            "¿Qué opción rellena correctamente el espacio en blanco para habilitar el procesamiento en stream desde la tabla después de borrar las particiones?"
        ),
        "options": [
            '.withWatermark("year", "INTERVAL 2 YEARS")',
            '.option("ignoreDeletes", True)',
            '.option("ignoreDeletes", "year")',
            '.withWatermark("year", "INTERVAL 2 YEARS")'
        ],
        "answer": '.option("ignoreDeletes", True)',
        "explanation": (
            "El particionamiento (partitioning) sobre columnas datetime se puede aprovechar al remover datos de la tabla que sobrepasan cierta edad. Por ejemplo, puedes decidir borrar los datos de los años previos. En este caso, el borrado se realizará de forma efectiva a nivel de partición (partition-level delete).\n\n"
            "Sin embargo, si estás usando esta tabla como un streaming source, borrar datos rompe el requisito append-only para streaming sources, lo cual hace que la tabla ya no se pueda streamear. Para evitar esto, puedes usar la opción ignoreDeletes cuando haces el stream desde esta tabla. Esta opción habilita el procesamiento streaming desde una tabla ignorando el hecho de que se hagan borrados a nivel de partición.\n\n"
            'option("ignoreDeletes", True)'
        ),
    },
    {
        "exam": 4,
        "id": "q31_cdc_performance_optimization",
        "question": (
            "Un data engineer nota que una tabla Delta gestionada por UC (UC-managed Delta table) de gran tamaño (~750GB) se ha vuelto lenta al aplicarle streams intensivos de CDC (intensive CDC feeds).\n\n"
            "¿Cuál de las siguientes acciones debería tomar el data engineer para mejorar el performance?"
        ),
        "options": [
            "Particionar la tabla y aplicar indexación Z-order sobre las primary keys.",
            "Habilitar deletion vectors en la tabla y aplicar liquid clustering usando las primary keys.",
            "Particionar la tabla y aplicar liquid clustering usando las primary keys.",
            "Habilitar deletion vectors en la tabla y aplicar indexación Z-order sobre las primary keys."
        ],
        "answer": "Habilitar deletion vectors en la tabla y aplicar liquid clustering usando las primary keys.",
        "explanation": (
            "Puesto que un Change Data Capture (CDC) involucra el procesamiento de updates y deletions, para mejorar el performance de una tabla Delta tan grande sufriendo latencias por el CDC, el data engineer debería habilitar deletion vectors en la tabla y aplicarle liquid clustering usando las primary keys.\n\n"
            "Habilitar deletion vectors permite a Delta rastrear y manejar de manera muy eficiente las filas que se eliminan o actualizan sin requerir reescrituras completas (full rewrites) de los archivos subyacentes, reduciendo en gran medida la sobrecarga (overhead) para las operaciones de CDC. Aplicar liquid clustering sobre las CDC merging keys organiza físicamente los datos basándose en esas llaves, garantizando que registros relacionados se coubiquen y minimizando el escaneo de datos durante updates o deletes. Combinadas, estas optimizaciones mantienen una alta tasa de ingesta y un alto rendimiento en las queries, reducen la latencia para los workloads de CDC y hacen la tabla mucho más gobernable a escala."
        ),
    },
    {
        "exam": 4,
        "id": "q32_foreachbatch_spark_session_legacy",
        "question": (
            "Un data engineer está usando una lógica foreachBatch para hacer upsert de datos en una tabla target Delta.\n\n"
            "La función que será llamada en cada procesamiento del nuevo microbatch se muestra a continuación con un espacio en blanco:\n\n"
            "def upsert_data(microBatchDF, batch_id):\n"
            '    microBatchDF.createOrReplaceTempView("updates_microbatch")\n\n'
            '    sql_query = """\n'
            "        MERGE INTO stats_order t\n"
            "        USING updates_microbatch s\n"
            "        ON t.item_id = s.item_id\n"
            "            AND t.item_timestamp=s.item_timestamp\n"
            "        WHEN NOT MATCHED THEN INSERT *\n"
            '    """\n\n'
            "    ________________\n\n"
            "¿Qué opción rellena correctamente el espacio en blanco para ejecutar la sql query dentro de la función en un cluster con un Databricks Runtime por debajo de 10.5?"
        ),
        "options": [
            "microBatchDF._jdf.sparkSession().sql(sql_query)",
            "microBatchDF.sql(sql_query)",
            "microBatchDF.sparkSession.sql(sql_query)",
            "spark.sql(sql_query)"
        ],
        "answer": "microBatchDF._jdf.sparkSession().sql(sql_query)",
        "explanation": (
            "Usualmente, usamos la función spark.sql() para ejecutar queries SQL. Sin embargo, en este caso en particular, no se puede acceder a la sesión de spark de forma global desde el interior del proceso microbatch. En su lugar, podemos acceder a la spark session local desde el dataframe propio del microbatch.\n\n"
            "Para los clusters con versión de Databricks Runtime anterior a la 10.5, la sintaxis correcta para invocar la spark session local es:\n"
            "microBatchDF._jdf.sparkSession().sql(sql_query)"
        ),
    },
    {
        "exam": 4,
        "id": "q33_predictive_optimization_unsupported_ops",
        "question": (
            "Un equipo de data engineering administra tablas de Unity Catalog que tienen la predictive optimization habilitada. No están seguros de qué operaciones son realizadas automáticamente sobre estas tablas como parte del mantenimiento automático (automatic maintenance) de la predictive optimization.\n\n"
            "¿Cuál de las siguientes operaciones NO es manejada automáticamente por la predictive optimization para las tablas que la tienen habilitada?"
        ),
        "options": [
            "ZORDER",
            "ANALYZE",
            "VACUUM",
            "OPTIMIZE"
        ],
        "answer": "ZORDER",
        "explanation": (
            "La indexación de Z-order NO es manejada de forma automática por la predictive optimization para tablas de Unity Catalog. Si bien la predictive optimization se encargará del OPTIMIZE, ANALYZE y VACUUM para mantener el performance de la tabla, no ejecutará el comando ZORDER. Adicionalmente, todo archivo que haya sido ordenado con Z-order es ignorado cuando la predictive optimization se ejecuta."
        ),
    },
    {
        "exam": 4,
        "id": "q34_sh_magic_command_drawbacks",
        "question": (
            "Un junior data engineer está usando el magic command %sh para ejecutar algo de código heredado (legacy code). Un senior data engineer le ha recomendado refactorizar el código.\n\n"
            "¿Cuál de las siguientes podría explicar el motivo por el cual un data engineer debería evitar el uso del magic command %sh?"
        ),
        "options": [
            "%sh reinicia el intérprete de Python. Esto borra todas las variables declaradas en el notebook.",
            "Todas las razones listadas explican por qué %sh debería evitarse.",
            "%sh no puede acceder al storage para persistir (persist) el output.",
            "%sh ejecuta el código shell solo en la máquina local del driver, lo cual conduce a una sobrecarga de rendimiento significativa (significant performance overhead)."
        ],
        "answer": "%sh ejecuta el código shell solo en la máquina local del driver, lo cual conduce a una sobrecarga de rendimiento significativa (significant performance overhead).",
        "explanation": (
            "Databricks soporta el comando auxiliar (magic command) %sh para correr sentencias bash/shell en notebooks. Este comando se ejecuta únicamente en el Apache Spark driver, y no se paraleliza en los worker nodes, lo que puede implicar grandes bloqueos y latencias."
        ),
    },
    {
        "exam": 4,
        "id": "q35_delta_share_creation_permissions",
        "question": (
            "¿Cuáles de los siguientes usuarios poseen la capacidad de crear y gestionar Delta Shares en Unity Catalog?\n\n"
            "Elige 2 respuestas:"
        ),
        "options": [
            "Users con el privilegio MANAGE sobre el metastore",
            "Account admins",
            "Metastore admins",
            "Workspace admins",
            "Users con el privilegio CREATE SHARE sobre el metastore"
        ],
        "answer": [
            "Metastore admins",
            "Users con el privilegio CREATE SHARE sobre el metastore"
        ],
        "explanation": (
            "Los usuarios que cuentan con los derechos o habilidades para crear y gestionar Delta Shares dentro del Unity Catalog son los Metastore admins y los usuarios con el privilegio de CREATE SHARE sobre el metastore, puesto que dichos perfiles tienen de forma explícita los permisos necesarios."
        ),
    },
    {
        "exam": 4,
        "id": "q36_unity_catalog_default_privileges",
        "question": (
            "Un equipo de data engineering creó un nuevo workspace, que se habilita automáticamente para el Unity Catalog. Quisieron crear un workspace catalog predeterminado y un default schema.\n\n"
            "¿Cuál de las siguientes afirmaciones describe de forma correcta los privilegios predeterminados que los workspace users tienen sobre este catalog y schema?"
        ),
        "options": [
            "Los workspace users tienen principalmente los privilegios CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, y USE SCHEMA sobre el default schema, junto con el privilegio USE CATALOG en el workspace catalog.",
            "Los workspace users tienen el privilegio ALL PRIVILEGES sobre el default schema, junto al privilegio USE CATALOG sobre el workspace catalog.",
            "Los workspace users tienen el privilegio ALL PRIVILEGES sobre el workspace catalog.",
            "Los workspace users no poseen ningún privilegio por defecto sobre el default schema, a menos que el workspace administrator les conceda explícitamente los permisos."
        ],
        "answer": "Los workspace users tienen principalmente los privilegios CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, y USE SCHEMA sobre el default schema, junto con el privilegio USE CATALOG en el workspace catalog.",
        "explanation": (
            "Cuando se levanta un workspace nuevo con Unity Catalog habilitado, Databricks provee de forma automática un default catalog, nombrado 'workspace', y un default schema, asignando a los usuarios un grupo de privilegios bases que les permitirán acometer las tareas habituales de data engineering dentro de ese esquema.\n\n"
            "Los workspace users poseen el privilegio USE CATALOG en el workspace catalog, y privilegios específicos en el default schema que incluyen: CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, CREATE MATERIALIZED VIEW, CREATE MODEL, y USE SCHEMA."
        ),
    },
    {
        "exam": 4,
        "id": "q37_production_job_clusters",
        "question": "Para los Databricks jobs de producción, ¿cuál de los siguientes tipos de cluster está recomendado?",
        "options": [
            "Production clusters",
            "Job clusters",
            "All-purpose clusters",
            "On-premises clusters"
        ],
        "answer": "Job clusters",
        "explanation": (
            "Los Job Clusters son clusters dedicados para el run de un job o task de forma aislada. Un job cluster terminará automáticamente (auto-terminates) una vez que el job finaliza, logrando grandes ahorros económicos en coste comparado contra los all-purpose clusters.\n\n"
            "Adicionalmente, Databricks recomienda tajantemente emplear job clusters para la producción para que así cada job se mueva en su propio entorno y ambiente plenamente aislado."
        ),
    },
    {
        "exam": 4,
        "id": "q38_cluster_permissions_view_ui",
        "question": "¿Cuál de las siguientes opciones describe los permisos mínimos que necesita un data engineer para ver las métricas y el panel de Spark UI de un cluster ya existente?",
        "options": [
            'Privilegio "Can Restart" sobre el cluster',
            'Privilegio "Can Manage" sobre el cluster',
            'Cluster creation allowed + privilegios "Can Attach To" sobre el cluster',
            'Privilegio "Can Attach To" sobre el cluster'
        ],
        "answer": 'Privilegio "Can Attach To" sobre el cluster',
        "explanation": (
            "Existen dos formas de configurar los permisos en base a un cluster:\n"
            "1- El permiso global 'Allow cluster creation', rigiendo tu potestad principal para lanzar o instanciar nuevos clusters.\n"
            "2- Los cluster-level permissions, que rigen la potestad para invocar y modificar a una instancia particularizada. Existiendo así cuatro jerarquías: No Permissions, Can Attach To, Can Restart, y Can Manage.\n\n"
            "Para poder entrar y visualizar el panel de Spark UI bastará con el permiso de nivel base \"Can Attach To\"."
        ),
    },
    {
        "exam": 4,
        "id": "q39_spark_functions_extract_date",
        "question": "¿Cuál de las siguientes funciones en Spark NO es válida si el objetivo es extraer la fecha de una columna de tipo timestamp?",
        "options": [
            "CAST(ts AS DATE)",
            "date_part('day', ts)",
            "date_trunc('day', ts)",
            "TO_DATE(ts)"
        ],
        "answer": "date_trunc('day', ts)",
        "explanation": (
            "Las funciones que son válidas en este cometido de extraer el date base a partir de un timestamp son:\n"
            "- CAST(ts AS DATE)\n"
            "- TO_DATE(ts)\n"
            "- date_trunc('day', ts) sin embargo realiza un truncado de un timestamp al inicio del nivel unitario (day), en lugar de extraer por completo únicamente la porción en tipo estructurado DATE.\n\n"
            "Cabe sumar que date_part típicamente puede retornarnos o vomitar un solo componente puramente numérico antes que un casting propiamente a date."
        ),
    },
    {
        "exam": 4,
        "id": "q40_stream_static_exceptall",
        "question": (
            "Dada la siguiente query:\n\n"
            'spark.table("stream_sink") \\\n'
            "    .exceptAll( \\\n"
            '        spark.table("stream_data_stage") \\\n'
            '            .dropDuplicates(["id", "row_timestamp"]) \\\n'
            "    ) \\\n"
            "    .write \\\n"
            '    .mode("overwrite") \\\n'
            '    .table("stream_data_stage")\n\n'
            "¿Qué enunciado ilustra el resultado tras ejecutar esta query?"
        ),
        "options": [
            'Un batch job sobrescribirá (overwrite) la tabla stream_data_stage con los registros deduplicados que sean calculados usando todos los registros sobre la actual versión de la tabla stream_sink.',
            "Un batch job sobrescribirá la tabla stream_data_stage con aquellos registros deduplicados provenientes de stream_sink añadidos exclusivamente desde la última ejecución del job.",
            "Un incremental job sobrescribirá la tabla stream_data_stage valiéndose de los registros deduplicados de stream_sink asimilados exclusivamente a raíz del último run.",
            "Un incremental job sobrescribirá la tabla stream_sink usando los registros deduplicados provenientes de stream_data_stage que llegaron desde el momento del último run del job."
        ],
        "answer": "Un batch job sobrescribirá (overwrite) la tabla stream_data_stage con los registros deduplicados que sean calculados usando todos los registros sobre la actual versión de la tabla stream_sink.",
        "explanation": (
            "Consumir una tabla Delta tirando de la función spark.table() asume y rige de inmediato leer el contenedor como una static source. De modo que, ante cada lanzamiento de tu query, se asimilarán en su plenitud (all records) todas las filas disponibles en ese momento referidas a 'stream_sink'.\n\n"
            "No hay distancia o diferencia operativa subyacente entre spark.table() y spark.read.table(). De hecho, spark.read.table() llamará en su interior a spark.table().\n\n"
            "Toda la query desembocará escupiendo en modo \"overwrite\" de cara a la tabla destino 'stream_data_stage', resultando en un reemplazo y sobreescritura absoluta en cada ciclo u ejecución."
        ),
    },
    {
        "exam": 4,
        "id": "q41_deprecated_init_scripts_location",
        "question": "¿Cuál de las siguientes ubicaciones (source locations) ya no se puede utilizar para depositar o almacenar los init scripts?",
        "options": [
            "DBFS",
            "Cloud storage",
            "Workspace files",
            "Volumes"
        ],
        "answer": "DBFS",
        "explanation": (
            "A tenor de los updates y políticas de seguridad y sanidad recientes por parte de Databricks, el DBFS (Databricks File System) quedó vetado u obsoleto, sin capacidad para asimilar almacenamientos de init scripts. Databricks retiró su potestad (ha quedado deprecated) en relación al uso del path de raíz (dbfs:/) esgrimiendo lógicos escrúpulos orientados hacia una mermada fiabilidad en la gestión de credenciales y base (reliability and security concerns).\n\n"
            "A la fecha los únicos pilares o locations donde está permitida la guarda de un init script son:\n"
            "- Volumes\n"
            "- Cloud storage\n"
            "- Workspace files"
        ),
    },
    {
        "exam": 4,
        "id": "q42_streaming_corrupted_events_handling",
        "question": (
            "Una firma adscrita al sector IoT consume lecturas transaccionales vía streaming pipeline disparadas por innumerables dispositivos o sensores en pleno vivo. Con cierta recurrencia, estos cacharros pueden lanzar eventos carentes de pulso, corrompidos, o mutilados abocando al desastre tras cruzar barreras o validaciones de schema. El escuadrón de la ingeniería ha de certificar innegociablemente que sus cuadros de mando analíticos en producción, apoyados o sustentados en data impoluta, latan continuamente bajo una sincronía del real time. Pese a todo, la chatarra u registros corruptos todavía obligan a un acopio forzoso salvaguardando sus pistas por investigaciones a futuro, escatimando al límite su factura y gasto sobre los computing resources (recursos de computación).\n\n"
            "¿Bajo qué directriz u operativa deberán atajarse y cubrir tales demandas operacionales por los engineers?"
        ),
        "options": [
            "Anexar capas de retry logic de lleno incrustados al main stream para abocarse o reintentar reprocesar a destajo hasta alcanzar el eventual éxito del mensaje corrupto.",
            "Filtrar o separar (Filter out) drásticamente toda lectura envenenada o evento corrupto apartándolo de lleno del main real-time stream. Trasvasando ciegamente a título de tabla productiva las filas sanas y puras. Mientras, se cimenta a la vera un escuadrón secundario en modo de proceso ligero que por goteos lea e infiera guardando el mensaje nocivo de cara al posterior análisis.",
            "Asimilar y amalgamar ciegamente el buen fruto al podrido (valid and invalid) hacia la panza común de una tabla Delta asumiendo posteriormente a futuro (queries downstreams) el laborioso filtro imponiendo las data quality rules mermando las corrupciones en ruta hacia el dashboard.",
            "Acaparar el lote completo orgánicamente arrastrando sanos o fallidos fundiéndolos al main stream, marcando con una bandera o flag los avatares e impurezas a ser apartadas."
        ],
        "answer": "Filtrar o separar (Filter out) drásticamente toda lectura envenenada o evento corrupto apartándolo de lleno del main real-time stream. Trasvasando ciegamente a título de tabla productiva las filas sanas y puras. Mientras, se cimenta a la vera un escuadrón secundario en modo de proceso ligero que por goteos lea e infiera guardando el mensaje nocivo de cara al posterior análisis.",
        "explanation": (
            "Los engineers adoptarán de forma lógica la criba (filter out) desechando del tronco matriz principal del streaming pipeline a los flujos nocivos, registrando e instigando un pase directo única y exclusivamente a los datos verídicos o impolutos rumbo a sus cimientos y tablas operativas. Validando una inyección sana blindando los dashboards sin ahogos por lag o dilaciones inútiles. Operando en sincronía paralela, forjarán su operativa liviana o secundaria asimilando periódicamente el rescate o guarda de aquel vertido nocivo (corrupted messages) prestando el auxilio necesario asimilando auditorías de rigor o depuraciones posteriores (debugging or auditing).\n\n"
            "Bajo semejante encuadre o diseño preservas tu agilidad manteniendo picos soberbios dictados bajo normativas strict real-time analíticas y evadiendo a título general la infección asimilada sobre tu target orgánico o panel. Apilando o resguardando a coste mermado la trazabilidad del resto (invalid records) impidiendo estrangular a tu vía central ni ahogar por saturación tu facturación en (computing resources)."
        ),
    },
    {
        "exam": 4,
        "id": "q43_apply_column_mask",
        "question": (
            "Un data engineer posicionado en una central bancaria orquesta y tutela el mandato rindiendo sobre una matriz Delta Lake 'customer_accounts' atada de lleno a sus ramales de columnas: 'customer_id', 'name', 'account_number', 'credit_card'. Sometido a políticas blindan operar infiriendo un 'mask' asimilado ciñendo férreamente su columna de la 'credit_card' de cara a posibilitar accesos sin velo (visión real) tan sólo asimilados o aprobados para analistas encuadrados a su equipo o filial de Fraud Detection Department. En vistas a amparar y orquestar tan peliaguda traba, modelan de cero incrustando orgánicamente la citada user-defined function:\n\n"
            "CREATE FUNCTION card_mask(credit_card STRING)\n"
            "RETURN CASE WHEN is_account_group_member('fraud_detection') THEN credit_card\n"
            "            ELSE '****-****-****-****' END;\n\n"
            "Escudriñando el código o las sentencias base, ¿bajo qué directriz (command) se le otorga empuje para fijar este bloque función transmutándolo o atándolo como 'column mask' adherido en la matriz (table)?"
        ),
        "options": [
            "ALTER TABLE customer_accounts SET MASK card_mask ON (credit_card);",
            "ALTER TABLE customer_accounts SET MASK card_mask;",
            "ALTER TABLE customer_accounts ALTER COLUMN credit_card SET MASK card_mask;",
            "SET MASK card_mask ON TABLE customer_accounts TO COLUMN credit_card;"
        ],
        "answer": "ALTER TABLE customer_accounts ALTER COLUMN credit_card SET MASK card_mask;",
        "explanation": (
            "A la hora de proveer garantías en pro y resguardo de la confidencialidad, propiciando ciegamente a los adscritos al Fraud Detection Department su escrutinio visual exento de tapujos hacia numeraciones de la tarjeta en bruto (actual credit card numbers), se le encarga aplicar tal capa encubridora (masking function) emparejándola e impactando drásticamente ciegamente al nivel de la column en su guarida base Delta. Dicha orden o instrucción cimentadora SQL correcta flanquea a modo:\n\n"
            "ALTER TABLE customer_accounts ALTER COLUMN credit_card SET MASK card_mask;\n\n"
            "Su ejecución muta incrustándose de cuajo en el atributo columna ('credit_card') encuadrándolo e implicando directamente la atadura funcional bajo el nombre de 'card_mask', ejerciendo la metamorfosis entre encubrir u mostrar dictaminando por membresía grupal base del que acude."
        ),
    },
    {
        "exam": 4,
        "id": "q44_autoloader_schema_evolution_addnewcolumns",
        "question": (
            "El perfil a cargo del data engineering rige u orquesta cimentando bajo estatus de andamiaje el empuje de un Databricks Auto Loader stream asimilado y forjado en arrastrar JSON data devorándolos al pie de su base (S3 bucket):\n\n"
            "spark.readStream \\\n"
            '    .format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            '    .option("cloudFiles.schemaLocation", "/path/to/checkpoint/dir") \\\n'
            "    .____________________ \\\n"
            '    .load("s3://bucket/data/") \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", "/path/to/checkpoint/dir") \\\n'
            '    .start("sales_data")\n\n'
            "En el pliego de requerimientos y blindajes se impone o condena a que el pipeline salte arrojando fallo inmediato y perenne en ocasión a toparse contra incipientes e inadvertidos atributos (new columns) brotando originariamente por el torrente (incoming data). Pero con la dispensa dictatorial y prebenda orgánica de tolerar y sumar, absorbiendo o encuadrando estos hallazgos (new columns) encastrándolos de fondo al schema matriz, otorgando así que en renacidos posteriores o ciclos colindantes venideros (subsequent runs) se prosiga fluidamente gozando de la nueva adaptación. Todo atributo precedente acatará salvaguardar sin permutas en su fisionomía base (data types).\n\n"
            "Indagando las vías ¿cuál remiendo al hueco dictado suple y abraza este condicionado pre-establecido en firme?"
        ),
        "options": [
            "failOnNewColumns",
            "rescue",
            "none",
            "addNewColumns"
        ],
        "answer": "addNewColumns",
        "explanation": (
            "Operando en su modalidad por naturaleza o por decreto dictado de base (default schema evolution behavior) bajo la figura Auto Loader recala incuestionablemente el término 'addNewColumns'. Dictaminándose bajo sus riendas y fueros (mode) que un hallazgo inesperado asimilado a una (new column) originará un alto repentino operando un bloqueo del stream (fails). Tras este colapso se asienta el milagro operativo ya que asimilará e incrustará en sus cimientos orgánicamente dicha ramificación al esquema (schema). Devolviendo así paso y encumbramiento, propulsando de que pueda reactivarse o reiniciar operando libre (continue processing) esgrimiendo una estructura y blindaje robusto al día y con el esquema aggiornado. Salvaguardando a ultranza sin corromper ni un ápice los cimientos originales o base (data types)."
        ),
    },
    {
        "exam": 4,
        "id": "q45_pyspark_window_cumulative_average",
        "question": (
            "Adscrito en el núcleo del peritaje o data engineer, rigiendo para la esfera e instancias educativas operativas ('international school'), ha de implantarse o moldear ciegamente este operativo código PySpark de origen:\n\n"
            "from pyspark.sql.window import Window\n"
            "from pyspark.sql.functions import avg, col\n\n"
            'window_spec = Window.partitionBy("student_id").orderBy("exam_date") \\\n'
            "    .rowsBetween(Window.unboundedPreceding, Window.currentRow)\n\n"
            'df_new = df_student_results.withColumn("avg_score",\n'
            '    avg(col("score")).over(window_spec))\n\n'
            "Sopesando en abanico analítico ¿cuál de todas estas sentencias describe o resume en veredicto franco orgánicamente el calado y trazas colindantes dictaminadas y fraguadas en el script?"
        ),
        "options": [
            "Vomita anexionando un apéndice columna delatando y reflejando ciegamente a título y sumario operando como balance el acumulado de rango promediado (cumulative average score) amparando individualmente (each student) su trayectoria o progreso sumando sin fisuras todo intento abarcando del primer examen acatando de cierre su colindante y último asalto ('current exam').",
            "Vomita anexionando un apéndice columna rindiendo de pleno recabando y delatando el acumulado general y rango promediado (cumulative average score) pero rigiendo ciegamente frente a las almas o entes (each student) abrazando desde el pionero o recién estrenado alumno escudriñando todo amparo y tope con el matriculado en vigor.",
            "Vomita anexionando o adhiere al conjunto un apéndice columna rindiendo o vomitando un promedio general estático y cerrado (overall average score) ceñido de modo asimilado al individuo, atados forzosos u ordenados al calor de la fecha de cada evento ('exam date').",
            "Vomita anexionando un apéndice de columna delatando o fijando la media de peso puro ('overall average score') amparando y escudriñando operativamente o asimilando el evento físico en sí o rito u ('each exam'), segregando asimilándolo ciegamente sin acatar diferencias dictadas sobre su propio creador ('student')."
        ],
        "answer": "Vomita anexionando un apéndice columna delatando y reflejando ciegamente a título y sumario operando como balance el acumulado de rango promediado (cumulative average score) amparando individualmente (each student) su trayectoria o progreso sumando sin fisuras todo intento abarcando del primer examen acatando de cierre su colindante y último asalto ('current exam').",
        "explanation": (
            "Operando en su esencia rindiendo cuentas asimiladas en su código de base PySpark y empujando las herramientas a modo ('Window function'), estipula ciegamente o amalgama operando tras los pasos acumulando sobre variables continuas ('cumulative or running average score') a título asimilado u empírico y unitario del ente central ('each student').\n\n"
            "1. Window.partitionBy(\"student_id\"): Divide y escinde el cimiento base amasándolos o fraccionando sus muros separándolos por entes individualizados asimilados en base a ('student_id').\n"
            "2. .orderBy(\"exam_date\"): Pone a su amparo rigor ordenado dictaminando y apilando las filas ceñidas cronológicamente para ese mismo alumno asimilando en base a la línea cronológica desde orígenes viejos al más verde (oldest to newest).\n"
            "3. .rowsBetween(Window.unboundedPreceding, Window.currentRow): Acota ciegamente el encuadre o perímetro base abarcando empíricamente de la fase más remota sin barrera ni ataduras al origen absoluto (unboundedPreceding) y frenando su empuje u cálculo al momento de asimilado escrutinio a modo ventana o renglón base actual (currentRow). Rindiendo su balance al amparo del cumulative.\n"
            "4. avg(col(\"score\")).over(window_spec): Apoyándose de lleno asimilando al promedio 'avg function' dictándolo u operando inmiscuyéndose en el encuadre forjado al 'window_spec', resultando u arrojando a colación el peso y valor progresivo de promedio amasado justo y ceñido sobre ese trance temporal cronológico de examen."
        ),
    },
    {
        "exam": 4,
        "id": "q46_dab_cicd_commands",
        "question": (
            "Amparando y abocándose a título orgánico forjado en las trincheras, una cuadrilla encuadrada en labores de base (data engineering team) bajo el sello u organización corporativa colosal remató con acierto u forjando de pleno el andamio base (setup) arropando un fresco u rutilante proyecto ('Databricks Asset Bundle project'). A posteriori y logrando enlazar a la máquina de forja y despliegue (CI/CD system), rigen o ambicionan dictámenes operando o amparando salvaguardar un clima armónico o pericia asimilada y fluida exenta de colisiones y roces cuando acaben escupiendo iterativamente a título automático a futuro ('automated deployments') inyectándolo ciegamente contra la matriz viva productiva ('production environment').\n\n"
            "En esta traba u marco de base operativo, ¿cuál directriz obrará al amparo del operario para subsanar este trámite ciego orgánico debiendo ser vetada y rehuida operativamente ('avoid rerunning') bajo el mandato del CI/CD pipeline a expensas puras o en las continuas descargas (subsequent deployments) a posteriori?"
        ),
        "options": [
            "databricks bundle run",
            "databricks bundle deploy",
            "databricks bundle validate",
            "databricks bundle init"
        ],
        "answer": "databricks bundle init",
        "explanation": (
            "El pilar rindiendo cuentas operativas u orquestador automático del 'CI/CD pipeline' ha de desterrar u obviar tajantemente y ciegamente el impulso base referido y acotado como (databricks bundle init) dentro del abanico temporal del día a día ('subsequent deployments') a razón innegociable asimilada en que asoma u opera de base exclusivamente o como disparo originario virgen (only used once). Sirve a su amo germinando de la nada orgánicamente asimilando a un neonato o incipiente 'Databricks Asset Bundle project' y dándole su armazón y configuración matriz base. Su constante o negligente llamado y ejecución iterativa corrompería operando u asimilando un pisoteo destructivo a los preexistentes amarres o desfigurando ('reset') desde el núcleo y la simiente el proyecto asimilado (project setup). Actuando como opuesto benigno los demás operativos a título franco (databricks bundle validate, databricks bundle deploy, and databricks bundle run) actúan albergando rutinas impolutas y blindadas propiciando un reuso sereno en su maquinaria de forja asimilando a los automatizados conductos o rutinas ('automated deployment pipelines')."
        ),
    },
    {
        "exam": 4,
        "id": "q47_autoloader_definition",
        "question": "¿Cuál enunciado enarbola fielmente en un Databricks context, el significado intrínseco detrás del mecanismo de Auto Loader?",
        "options": [
            "La herramienta Auto loader ampara ciegamente inmiscuyéndose en agilizar operativas base (insert, update, deletes, and rollback capabilities), cimentando de modo ciego capas subyacentes o de peso matriz ('storage layer') propulsando de un tiro confiabilidad absoluta a nivel cimiento en tu lago u 'data lakes'.",
            "Este motor u artilugio Auto loader obsequia la potestad al amparo del ('CDC feed') o recolección de mutaciones continuas aplicando sus remiendos y empujes base (update tables) escudriñando orgánicamente las mermas o alteraciones latentes en tu fuente original.",
            "De un barrido ciego, el Auto loader vigila incansable y celosamente una madriguera o cuenco base (source location) a espera a que se hacinen acervos e inventarios esparcidos u orgánicamente asimilados ('accumulate files'), husmeando y devorando únicamente de lleno (ingest) al que acabe de cruzar de modo novel ('new arriving files') cediendo y disparando este eslabón en cada tirada o batida del mandato (command run). Arrojando o ignorando del tajo de forma pasiva a todo aquel fichero preexistente triturado de antaño ('skipped').",
            "El componente Auto loader confiere el don orgánico de duplicar al vuelo o generar espejismos puros (cloning) a título y marco orgánico a partir de fuente original (Delta table) asimilando de destino un cerco u 'target destination' afianzando un amarre estático sobre versión anclada al instante ('specific version')."
        ],
        "answer": "De un barrido ciego, el Auto loader vigila incansable y celosamente una madriguera o cuenco base (source location) a espera a que se hacinen acervos e inventarios esparcidos u orgánicamente asimilados ('accumulate files'), husmeando y devorando únicamente de lleno (ingest) al que acabe de cruzar de modo novel ('new arriving files') cediendo y disparando este eslabón en cada tirada o batida del mandato (command run). Arrojando o ignorando del tajo de forma pasiva a todo aquel fichero preexistente triturado de antaño ('skipped').",
        "explanation": (
            "Dicha traza 'Auto Loader' procesa a título franco operando de forma segmentada u orgánica y recurrente o ciegamente a paso ('incrementally') amparado con base inalterable y resistente ('idempotently') fagocitando sin piedad todo vestigio (data files) noveles a la vez en base asimilada ciegamente según abocan el salto e infiltran en tu guarida base tipo 'cloud storage' propiciando volcarlos de origen depositándolos asimilados y acoplados de lleno hacia la meta dictaminada o 'Delta Lake table'."
        ),
    },
    {
        "exam": 4,
        "id": "q48_describe_extended_table_comment",
        "question": (
            "A la estela e indicación un perito data engineer dio vida asimilando la chispa germinando su tabla de origen y amarrando con un nexo un apostillo o traza adjunta u (comment) usando este marco o pauta:\n\n"
            "CREATE TABLE payments\n"
            "COMMENT 'This table contains sensitive information'\n"
            "AS SELECT * FROM bank_transactions\n\n"
            "Dentro de las pautas a esgrimir codificadas, ¿qué abanico obedece o cimienta el dictamen orgánicamente para destapar, rebuscar o dar luz visibilizando de frente al ingeniero el rastro adjunto y comentario en las fauces de tu matriz o tabla?"
        ),
        "options": [
            "DESCRIBE TABLE payments",
            "DESCRIBE EXTENDED payments",
            "SHOW TBLPROPERTIES payments",
            "SHOW COMMENTS payments"
        ],
        "answer": "DESCRIBE EXTENDED payments",
        "explanation": (
            "Empujar un apunte o mandato ciego operando la orden 'DESCRIBE TABLE EXTENDED' amparándose a modo sinónimo o abreviación purista en 'DESCRIBE EXTENDED' obsequia operativamente a tu visual ciegamente arrastrando a pleno sol tanto las muescas o trazas subyacentes referidas y amparando comentarios o ('table's comment'), rindiendo cuentas análogas sobre acotaciones en base y fisonomía amparando 'columns' comments', albergando otras tantas o innumerables excentricidades a modo propiedades base u variables originarias (custom table properties)."
        ),
    },
    {
        "exam": 4,
        "id": "q49_secret_scope_usage_roles",
        "question": (
            "Operando en su feudo incondicional el escuadrón operativo ampara y cimenta a base empírica su bóveda ('secret scope') encuadrada bajo etiqueta u apodo en rango de \"prod-scope\", atesorando a la sombra de su cobijo eslabones y criptogramas de calado (sensitive secrets) incrustado en el vientre productivo (production workspace).\n\n"
            "A la cabeza dispone de un operario o data engineer el cual transita elaborando ciegamente a título franco y corporativo todo un pergamino atado o normativas a base operativa o ('security and compliance documentation'), y precisa orgánicamente enumerar, visibilizar o encuadrar y esgrimir explícitamente a título reglado o dictaminando perfiles a operar libre con tales privilegios en el encuadre o seno ('secret scope').\n\n"
            "Bajo estas lides u operando ¿cuál rol orgánico acata el grado ínfimo exigido imperativo para desenvolverse asimilando u usando ('use') los secretos a base asimilada de la matriz señalada?"
        ),
        "options": [
            "Solo y a ciegas a título general para perfiles de rango y peso asimilando Workspace Administrators.",
            "Tributando orgánicamente operando en su rol primigenio y matriz los denominados orfebres de base u (Secret creators).",
            "Operar o subsumirse asimilando únicamente amparado en rangos referidos con prerrogativa de base READ o la transaccional MANAGE apuntalando la barrera del (secret scope).",
            "Todos y de manera global el abanico mencionado atesorando u orgánicamente asimilando operativas amparan la visibilidad (use the secrets) englobando al matriz 'secret scope'."
        ],
        "answer": "Todos y de manera global el abanico mencionado atesorando u orgánicamente asimilando operativas amparan la visibilidad (use the secrets) englobando al matriz 'secret scope'.",
        "explanation": (
            "La balanza en su conjunto de roles o perfiles, llámense a base ('Administrators*'), forjadores primigenios ('secret creators'), sumado o atesorando orgánicamente y confiriendo acceso reglado a usuarios o subalternos acaparando permiso (users granted access permission), poseen el don franco de acudir operando o manosear a título general u orgánico (use Databricks secrets). Acatando y ciñéndose ciegamente a la escalera piramidal de privilegios sobre la base (secret access permissions):\n\n"
            "- MANAGE - Facultando obrar sin coto subyacente operando bajo directrices asimilando cambios (ACLs), lecturas sumadas a escarceos físicos o sobreescritura (read and write) sobre el cimiento ('secret scope').\n"
            "- WRITE - Dotado orgánicamente del accionar subyacente o estipendiado confinado al esculpido de registros transaccionales (read and write) en este seno del secret scope.\n"
            "- READ - Subsumiéndose orgánicamente con visibilidad transaccional a la lectura operando al escrutinio e inventariado (read) con la de visibilidad del abanico latente asimilando a título (list what secrets are available).\n\n"
            "Esquivando toda laguna, en la cascada normativa de poder ('permission level') se ampara y subyace integrando colindantemente o asimilando el escalón o potestad inmediata inferior o de arrastre (a título que un estamento encumbrado como WRITE absorbe colindantemente ciegamente y rinde los favores del de base READ).\n\n"
            "* Los encumbrados (Workspace administrators) rigen imperiosamente ciegamente detentando mandos o prebendas inamovibles o asimiladas de (MANAGE permissions) enclavadas para acaparar y amparar cualquier o todo ('all secret scopes') enrutamiento enmarcado en el workspace."
        ),
    },
    {
        "exam": 4,
        "id": "q50_unity_catalog_masking_consistency",
        "question": (
            "Una célula operativa de mando amparada en velar pautas (data governance team) detecta asimilando anomalías ciegamente que ramales inconexos o tribus de base (different business units) acudieron esculpiendo asimilados a título ciego o por goteo individual forjando o modelando remiendos singulares u orgánicos o versiones desamparadas asimilando políticas o escudos ('masking policies') amparadas ciegamente a operar incidiendo bajo la misma o idéntica base originaria ('same columns').\n\n"
            "Indagando las vías ¿de qué manera apuntala y enmienda cimentando orgánicamente (Unity Catalog) esta desbarajuste y caos subyacente de escenario?"
        ),
        "options": [
            "Provee de placa operante asimilando a modo escudo blindando de manera transversal y forjando una base referencial inamovible (single source of truth) encauzando operaciones tapujo o (masking functions), extirpando y bloqueando ciegamente vulnerabilidades e interponiéndose arropando de origen brechas caóticas o asimiladas a visibilidad (inconsistent exposure).",
            "Mantiene o cede amparo delegando la organicidad o potestad a entes o (each team) que rijan a base asimilada de su capricho ('manage its version') el mandato sobre escudos (masking rules), apuntalando de este modo al incremento transaccional controlando la de la data privacy.",
            "Tributa asimilando de base flexibilidades mermando colindantemente el escudo cediendo asimilar o apartar temporalmente el blindaje (disable masking) rindiendo cuentas asimilando escrutinios (testing purposes), operando a beneficio de un cimiento de desarrollo desamarrado o sin lastres (flexibility during development).",
            "Atribuye u obsequia o cede amparo dictaminando ciegamente facultando al equipo a ejercer o tirar de poderes colindantes y de base preexistente (data object privileges) esculpiendo a placer tapujos y caretas que asimilen de forma difuminada ('mask data differently') arrastrando para dispares engranajes."
        ],
        "answer": "Provee de placa operante asimilando a modo escudo blindando de manera transversal y forjando una base referencial inamovible (single source of truth) encauzando operaciones tapujo o (masking functions), extirpando y bloqueando ciegamente vulnerabilidades e interponiéndose arropando de origen brechas caóticas o asimiladas a visibilidad (inconsistent exposure).",
        "explanation": (
            "Acudiendo o arrastrando ciegamente los dogmas base, el (Unity Catalog) interpone solución tajante aclamando este embrollo organizativo esgrimiendo y dictaminando a título innegociable a modo estandarte forjando orgánicamente asimilando a (single source of truth) en su afán de operar o regir encuadres operacionales (masking functions), forzando y avalando de cara al público o ramales de base (business units) consuman operando ciegamente normativas inmutables y homogéneas u (consistent) arropadas globalmente de base ('centrally governed masking policies') sin escatimar al esparcir o barrer por la matriz en general del data estate.\n\n"
            "Con los cimientos en Unity Catalog, este encuadre asimilado a escudo y pauta (masking logic) queda anidado y forjado rindiendo a base orgánica o moldeándose asimilado de origen en (user-defined functions - UDFs), logrando asimilar encapsulados o encerrando la doctrina y norma del escudo (masking rules) propulsándolo como un bloque compacto transaccional u ('reusable and standardized code'). Conllevando de pleno que en detrimento a propiciar que tribus u orgánicas células fabriquen caprichosamente (creating its own version of a masking rule) de modo descentralizado su regla, prime asimilar de origen un estandarte amparando la de (single, validated UDF) anidada y referenciada en pleno para disfrute u orgánico a todos (all teams). Cimentando por colofón (As a result) el imperativo asimilado orgánico por el que Unity Catalog impone acatar rígidamente e inmiscuyéndose blindajes y puridades forjando ('consistent data governance'), asimilando barreras ante compliance, amputando de origen peligros letales orgánicos mermando a la par al ciego y descuidado desajuste u asimilado (inadvertent exposure) abarcando registros comprometedores exentos en la base o entorno."
        ),
    },
    {
        "exam": 4,
        "id": "q51_foreach_task_efficiency",
        "question": (
            "Operando en su etapa novel o inmadura (junior data engineer) se apremia amparando y configurando forjando una base de andamiaje operativa (Databricks job) aglutinando la colosal de (15 notebook tasks), operando a cada asimilado latido el mismo mandato orgánico o criba rigiendo idéntica (data validation logic) ciegamente arrojado o vertido frente o de lleno a 15 matrices distintas (different tables). El encadenamiento sufre ya que por organicidad operando asimilado de pleno o a su caparazón dictamina un arraigo estricto en el (completion) arrastrando al éxito forzado y asimilando paso al cimiento anterior, forjando por lo tanto que el río o encauce operativo (workflow) resulte asimilado o lastre gigantesco a modo infinito (long) mermando u operando y asimilado una calamidad en labores referidas al 'maintain' (mantenimiento).\n\n"
            "Buscando el Norte y acudiendo al amparo asimilado ¿cuál dictamen colindante rige sin fisura esta enmienda de forma ineludible asimilando a pautas u organicidad resolviendo operativamente este atolladero elevando rendimiento (efficient) operando de lleno con resiliencia en picos de masa o peso (scalable) amparando este contexto (use case)?"
        ),
        "options": [
            "Fijar en calendario asimilando cronogramas a base colindante u orgánico o separando abocando 15 encuadres u (separate jobs) en contrapartida a amasar y arrastrar asimilando pesada carga amalgamando infinidad de apéndices operando ciegamente albergado dentro un único (one job).",
            "Tributar orgánicamente asimilando directrices sobre un aluvión de 15 bloques u operativos a título (notebook tasks) empujándolos asimilados operando todos (run in parallel), dotados u pertrechados a asimilados y blindajes de recursos apartados orgánicamente operando u asimilando un clúster disonante y exento ('separate cluster configuration').",
            "Aglutinar amparando y apretujando de forma asimilada u orgánica emparejando la criba en todas de las matrices asimilando y comprimiéndolas en un solo mastodonte (large notebook) operando o abocándose ciegamente u escarbando por el bucle y mermando asimilando el acervo de base operando secuencial o consecutivamente.",
            "Acudir o usar a modo orgánico amparando la directriz y el andamio colindante operando ciegamente a base de iterar y reincidir ('foreach task') asimilando la inyección al volcado y disparo iterativo arrastrando idéntico bloque ciego operativo (validation notebook) asimilando su impacto ciegamente para las casillas ('each table') operando con sincronía plena de a la vez ('parallel'), arrojando asimilando el pase operante de variable amparando o asimilado a base de señuelo ('table name as a parameter')."
        ],
        "answer": "Acudir o usar a modo orgánico amparando la directriz y el andamio colindante operando ciegamente a base de iterar y reincidir ('foreach task') asimilando la inyección al volcado y disparo iterativo arrastrando idéntico bloque ciego operativo (validation notebook) asimilando su impacto ciegamente para las casillas ('each table') operando con sincronía plena de a la vez ('parallel'), arrojando asimilando el pase operante de variable amparando o asimilado a base de señuelo ('table name as a parameter').",
        "explanation": (
            "En esta travesía de asimilar orgánica de peritaje cimentando fluidez extrema (efficient) y poderío de adaptación o molde de expansión infinita (scalable) amparando ciegamente la de base en este marco y disyuntiva, incide forzosamente dictaminado u orgánicamente asimilando al uso operativo amparando a la de (For Each task). El asimilado orgánico For Each task propicia o dota ciegamente empujando el permiso para empalmar e iterar incrustando a base una anidada labor ('nested task') escarbando de bucle en bucle, arrojando variables mutables o pasarela dispar ('parameters') asimilando al salto por ciclo iterativo ('iteration'). En la coyuntura del problema, el ingeniero se beneficia infiriendo pasar el rango e identificador a título orgánico ('table name as a parameter'), detonando el encuadre operativo único ('same validation notebook') barriendo orgánicamente y forzando el volcado hacia todas matrices de un tajo. Semejante ardid mengua ciegamente de pleno lastres operacionales y de carga referida al control o 'maintenance overhead', propiciando asimilar el arranque o empuje y que las inspecciones y chequeos operen y convivan en simultaneidad ciegamente mermando y extirpando dictámenes caducos amparando la linealidad (sequential dependencies), acelerando de lleno el cause operando en su totalidad (workflow faster) restando engorros al ampararlo de una tacada o 'manage'."
        ),
    },
    {
        "exam": 4,
        "id": "q52_modify_privilege_abilities",
        "question": (
            "El operario al mando o data engineer acude a esgrimir orgánicamente asimilando a sentencia ('SQL query') dictaminando:\n\n"
            "GRANT MODIFY ON TABLE employees TO hr_team\n\n"
            "Del abanico y prebendas asimiladas mostradas, ¿cuál de todas ilustra empíricamente y ampara ciegamente a la destreza conferida u orgánica o blindada por mediación del título operante ('MODIFY privilege')?"
        ),
        "options": [
            "Confiere el don o arroja de origen la destreza amparando anexar o incluir ('add data') abocando de base al cimiento de matriz",
            "Confiere el don o arroja de origen la destreza amparando segar o liquidar ('delete data') extirpando registros a base de matriz",
            "Aglutina ciegamente o empaqueta globalmente rindiendo todas ('All the listed abilities') y cada una de las capacidades listadas en gracia y dictamen orgánico amparado ciegamente a favor operando de 'MODIFY privilege'",
            "Confiere el don o arroja de origen la destreza amparando reescribir u alterar ('modify data') el eslabón encuadrado orgánico a base de matriz"
        ],
        "answer": "Aglutina ciegamente o empaqueta globalmente rindiendo todas ('All the listed abilities') y cada una de las capacidades listadas en gracia y dictamen orgánico amparado ciegamente a favor operando de 'MODIFY privilege'",
        "explanation": (
            "Bajo su velo operante (MODIFY privilege) cede y ampara asimilando y rindiendo gracia absoluta asimilando de origen el empuje operativo en escarceos físicos o sobreescritura habilitando el don transaccional dictaminado amparando anexar, fulminar y reescribir (add, delete, and modify data) sobre un cimiento, o asimilándolo recabando base 'object'."
        ),
    },
    {
        "exam": 4,
        "id": "q53_delta_time_travel_except",
        "question": (
            "Operando en su feudo incondicional el escuadrón amparando a la de (data engineering team) gozan operando de matriz Delta Lake bajo apodo u originario de 'daily_activities' que a las claras y ciegamente en horario nocturno perece fulminada de pleno o aniquilada de raíz (overwritten) recibiendo un torrente novel u operando a base recabando (new data) inyectados de lleno a base colindante de 'source system'.\n\n"
            "Con fines indagatorios u amparando escrutinios contables ('auditing purposes'), rigen operando de base colindantemente empujar y asentar o dar vida a una mecánica de remate asimilada ('post-processing task') forjando ciegamente a modo transaccional o recabando el jugo u operativo (Delta Lake Time Travel functionality) a modo orgánico recabando o dilucidando ciegamente de modo empírico la criba o resquicio diferenciador ('difference') confrontando el estado crudo vigente novel y la foto sepultada anterior (previous version) acatando al entorno o matriz. Abordan ciegamente en su inicio o preámbulo asimilando al recolectar el empuje u orgánicamente asimilado rastro asimilando la etapa vital viva 'current version' forjada a base del 'transaction log':\n\n"
            'current_version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY daily_activities)").collect()[0][0]\n\n'
            "De este ciego repertorio y amalgama orgánicamente de sentencias ('queries') a exprimir ¿cuál obrará al amparo del equipo o sirviendo operando de lleno con firmeza colindante paliando de forma ineludible y finiquitando o de base ('complete this task')?"
        ),
        "options": [
            "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities AS OF VERSION (current_version-1)",
            "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities AS VERSION = (current_version-1)",
            "SELECT * FROM daily_activities\nMINUS\nSELECT * FROM daily_activities AS VERSION = (current_version-1)",
            "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities VERSION AS OF (current_version-1)"
        ],
        "answer": "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities VERSION AS OF (current_version-1)",
        "explanation": (
            "Es de notar que frente a los padecimientos o toda alteración rindiendo cuentas asimiladas amparando de origen sobre un ente u base Delta Lake obsequia y engendra inexorablemente en matriz paralela un apunte orgánico o foto nueva (new table version). Quedando a potestad ciegamente acudiendo a escudriñar eslabones o archivo a base e (history information) en aras u operando la auditoría e inspeccionando rastros de modo ('audit operations') o sondear de pleno un cerco u matriz acotada a una coordenada temporal esgrimiendo y dictaminando a su orden matriz de 'VERSION AS OF'.\n\n"
            "Abrazando el código base y de dictamen o herramienta lógica referida o amparada al 'EXCEPT set operator', el sistema o el perito ciegamente a título de obsequio asimila o adquiere la destreza recabando el sesgo empírico o diferenciador asimilado de origen en su estado de cruce ('difference') enfrentando y destapando lagunas entre el actual estado naciente a de base asimilada de su 'previous version' de tu matriz o de la susodicha."
        ),
    },
    {
        "exam": 4,
        "id": "q54_assertions_definition",
        "question": "¿De esta terna y abanico de pautas, cuál acota orgánicamente en la directriz a título de verdad asimilada referida u amparando a la de (assertions) subsumiéndose u operando a base u organicidad del 'unit testing'?",
        "options": [
            "Cobra vigor de amparo a título operante como asimilado booleano ('boolean expression') rigiendo u operando e interrogando empíricamente de frente sopesando vínculos de armazón base asegurando ensambles lógicos u de amalgama forjados a título (interacted as a group).",
            "Mecánica orientada u amparada a modo dictamen orgánico o de directriz transaccional arrojando a ciego las asimiladas aristas e irregularidades o (differences) asimilando de origen contrastando o empujando matrices entre estado actual crudo o 'code unit' contra el rastro anidado previo amparando ('most recently edited version').",
            "Cobra vigor de amparo a título operante como asimilado booleano ('boolean expression') dictaminando o cerciorándose a título que los presagios asimilados ('assumptions made') o bases cimentadas en lo empírico orgánico del código asimilando de origen reinen imperturbables ('remain true') a lo largo u amparo operativo amarrado al fragor u ('development').",
            "Mecánica orientada de pleno u amparada a modo dictamen transaccional arrojando y rindiendo a título base engrosar la bitácora o asimilando acervos ciegos (logs failed units) operando fallos en tramos o 'code in production' facilitando escrutinio a modo asimilable de autopsias u ('later debugging and analysis')."
        ],
        "answer": "Cobra vigor de amparo a título operante como asimilado booleano ('boolean expression') dictaminando o cerciorándose a título que los presagios asimilados ('assumptions made') o bases cimentadas en lo empírico orgánico del código asimilando de origen reinen imperturbables ('remain true') a lo largo u amparo operativo amarrado al fragor u ('development').",
        "explanation": (
            "Dichas pautas o amparos a título asimilable a 'Assertions' conforman de base un operante booleano ('boolean expressions') dotándote u obsequiando potestad de testear (test) y poner a prueba los estigmas o prejuicios y lógicas matrices asimilados de frente u operando de origen o ('assumptions you have made in your code'). Sirven a su amo o se invocan orgánicamente a modo asimilable en las pruebas o en la de 'unit tests' cerciorándose orgánicamente de confirmar ciegamente a modo base que presagios u operativas enclavadas subyacentes logren de modo orgánico pervivir sin mácula o fidedignos ('remain true') en tanto y en cuanto deambulas o amoldas forjando orgánicamente (developing your code).\n\n"
            "assert func() == expected_value"
        ),
    },
    {
        "exam": 4,
        "id": "q55_location_keyword_external_tables",
        "question": (
            "Consolidando e impartiendo forzosamente la traba restrictiva acotando de la escuadra base del data engineering team exprimen u operan y manosean de forma recurrente orgánicamente a la referida etiqueta o acotada de palabra a modo de base 'LOCATION keyword' asimilando de origen para todo nacimiento asimilable Delta Lake table de cuajo o matriz naciente dentro del 'Lakehouse'.\n\n"
            "En esta traba u marco de base operativo, ¿cuál dictamen o rastro describe fielmente y sopesando los asimilados a título ciego o amparando a justificación operativa del uso o 'purpose of using' recabando el apelativo asimilado 'LOCATION keyword' amparando a este uso asimilado o caso ('in this case')?"
        ),
        "options": [
            "Dicha palabra referencial de apelativo ciego u 'LOCATION keyword' cimenta o se arroga el fin forjando una criba asimilando orgánicamente de la base albergando o estructurando asimilado de origen en matrices a título de Delta Lake tables acatando rango de o apuntando de lleno a un estatus o external database.",
            "Dicha palabra referencial de apelativo ciego u 'LOCATION keyword' es arrastrada o rinde cuentas amparando o instigando un formato o 'configure' asimilando de origen operando a base u Delta Lake tables arropadas colindantemente u orgánicamente asimilando a estatus y cimientos o 'external tables'.",
            "Dicha palabra referencial de apelativo ciego u 'LOCATION keyword' es empleada e instigada en pos de apuntalar un molde base o ('default schema') aparejado u en sintonía amparando ciegamente a rastros colindantes e identificativos o checkpoint location asimilando de origen o rigiendo las Delta Lake tables.",
            "Dicha palabra referencial de apelativo ciego u 'LOCATION keyword' rinde favores orgánicamente u se arroga amparando el encuadre operativo asimilando u originario (configure) esculpiendo a base colindante y regida a las Delta Lake tables a título matriz orgánico de o asimilado a las 'managed tables'."
        ],
        "answer": "Dicha palabra referencial de apelativo ciego u 'LOCATION keyword' es arrastrada o rinde cuentas amparando o instigando un formato o 'configure' asimilando de origen operando a base u Delta Lake tables arropadas colindantemente u orgánicamente asimilando a estatus y cimientos o 'external tables'.",
        "explanation": (
            "Por dogma y encuadre asimilado y subyacente, de origen rindiendo cuentas operativas referidas, las conocidas como tablas de extramuros y o asimilando u de corte 'External (unmanaged) tables' acaparan la potestad y título amparando a entes (tables) cuyos recovecos e inventario matriz físico ('whose data is stored') moran de espaldas ciegamente asimilados de base a cobijos y silos o almacenamientos remotos u asimilados a un ('external storage path') amparando y recurriendo a ciegamente asimilando a base de ('using a LOCATION clause')."
        ),
    },
    {
        "exam": 4,
        "id": "q56_delta_append_only_property",
        "question": (
            "Bajo un perfil austero ciegamente el data engineer asume el dictamen de amparar el cuidado u operar al cargo resguardando un cimiento o base de rango u matriz o (bronze Delta Lake table) anclada al Unity Catalog. Empujándolo amparando un uso colindante y en afán u operando de acotar los rigores y atar o (maintaining data integrity) encauzando salvaguardas matrices de seguridad y el rigor base o ('enforcing governance policies'), el operario anhela y aspira orgánicamente interponer vetos y trabas ('restrict modifications') blindando y aislando al cimiento o tabla procediendo u operando a bloquear o dejar sordas ('disabling') asimilando operativas matrices como las 'UPDATE and DELETE operations'.\n\n"
            "De este ciego repertorio de sentencias ('commands') a exprimir ¿cuál obrará al amparo del perito u asimilando la orden de ('data engineer') acudiendo orgánicamente en pro y en el marco de aplicar u ejecutar sin medias tintas la ley asimilada o forzar tal limitación u 'enforce this restriction'?"
        ),
        "options": [
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.appendOnly' = 'true');",
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.disableUpdate' = 'true');",
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.preventModification' = 'true');",
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.disableUpdate' = 'true', 'delta.disableDelete' = 'true');"
        ],
        "answer": "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.appendOnly' = 'true');",
        "explanation": (
            "Acudiendo o arrastrando ciegamente los dogmas base, el (data engineer) ostenta de lleno el poder de estrangular o vetar las injerencias base y las ('UPDATE and DELETE operations') escudriñando a la entidad (bronze Delta Lake table) sometiendo y encuadrando orgánicamente a la matriz o (table) inyectándola a operar de pleno y en exclusividad colindante a un ('append-only mode'). Semejante ardid mengua ciegamente de pleno trabas u amputa intrusiones o alteraciones puras de código (modifications) asimilando en base y a la par de dar luz u obsequiando potestad tolerada a la incursión ciegamente a nuevos frentes u (inserts). La doctrina y norma base u SQL amparada o asimilada a originar de lleno esta acción es:\n\n"
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.appendOnly' = 'true');\n\n"
            "El apéndice y llave asimilada 'delta.appendOnly' se encumbra ostentando su título normativo asimilado en Delta Lake, rigiendo el dictamen y asimilando el 'property' fidedigno extirpando mutaciones o bloqueando los temidos (updates and deletes), contrapuesto al asimilado elenco de sobrantes u ('other options') desamparados de rigor, vacíos asimilados de origen amparando el rango 'valid Delta table properties'."
        ),
    },
    {
        "exam": 4,
        "id": "q57_checkpointing_not_shared",
        "question": "¿De esta terna y abanico de pautas, cuál acota orgánicamente en la directriz a título de verdad asimilada referida u amparando orgánicamente y colindantemente con el uso de ('checkpointing') alojado en los marcos amparando el Spark Structured Streaming carece de exactitud o es tachado asimilando una base NO ('Not correct')?",
        "options": [
            "Los susodichos e inventarios o 'Checkpoints' atesoran y resguardan a título fidedigno operando y fijando la foto base alzada y matriz temporal ('current state') de forma y amparo en un canal asimilado a ('streaming job') arrojando y amparando en los rincones de almacenamiento u ('cloud storage')",
            "La mecánica u operatividad cimentándose de lleno y apoyada al uso de 'Checkpointing' coligando y entrelazada con el rastro amparado al (write-ahead logs mechanism) avala o instiga orgánicamente el salvoconducto forzando el cauce robusto y fiable asimilando peritaje y mermas a ('fault-tolerant stream processing')",
            "El susodicho (Checkpointing) cede asimilando a título orgánico o permite inmiscuyéndose en el lote y motor base o ('streaming engine') rastrear de manera empírica, seguir huellas y afianzar u operando ciegamente a rastrear avance operativo asimilando a ('progress') del torrente vivo ('stream processing')",
            "Dichas pautas o amparos a título asimilable ('Checkpoints') ceden y operan asimilando la facilidad y puente para fusionarse orgánicamente compartiendo de lleno asimilados u operando ('shared') de modo transversal forzando un puente entre torrentes y cauces asimilando una ajenidad o separados u ('separate streams')"
        ],
        "answer": "Dichas pautas o amparos a título asimilable ('Checkpoints') ceden y operan asimilando la facilidad y puente para fusionarse orgánicamente compartiendo de lleno asimilados u operando ('shared') de modo transversal forzando un puente entre torrentes y cauces asimilando una ajenidad o separados u ('separate streams')",
        "explanation": (
            "Dichos acervos y resguardos cimentados (Checkpoints) no obran ni soportan a título de asimilado o transaccional la dualidad o promiscuidad transaccional amparada o compartida de origen ('shared') escudriñando orgánicamente entre mallas y arroyos ajenos ('separate streams'). Indefectiblemente todo cauce u originario ('Each stream') ampara, rige y estipula forzoso acatar disponer colindantemente asimilado su celoso o purista cimiento o guarida acotada e inconfundible asimilando a ('own checkpoint directory') en el afán y amparo originario blindando a tope y operando garantizando seguridades blindadas ('processing guarantees')."
        ),
    },
    {
        "exam": 4,
        "id": "q58_dab_deployment_bind",
        "question": (
            "El operario al mando o data engineer ampara u ostenta bajo su égida un cimiento o (Databricks job) preexistente, y acude o demanda inmiscuyéndose y atando ciegamente asimilando a administrar u operando al mando (manage it) esgrimiendo la batuta del Databricks Asset Bundles. Alentándose apoyándose ya originaria u habiendo obrado colindantemente asimilado generando (generated) su cimiento descriptivo asimilando a base de archivo ('YAML definition') concerniente al job e impartiendo también a título operativo de base descargas u orgánicas ('downloaded') amparando y cubriendo referencias de los complementarios o asimilando ('referenced artifacts'). Mas sin dilación su celo e interés anhela u ambiciona certificar a pulso ciegamente que toda mudanza a posteriori ('updates') albergadas transaccionalmente y de lleno en las entrañas matrices del formato base o ('bundle's YAML') terminen volcando operando de origen alterando ('modify') implacablemente el cauce vigente amparando ciegamente al job primigenio (existing job) ahuyentando y mermando ciegamente o descartando el nacimiento inoportuno u organicidad de encumbrar y forzar de lleno ('creating') albergando a un ente u ('new job').\n\n"
            "De este ciego repertorio de sentencias ('commands') a exprimir ¿cuál obrará al amparo del perito u asimilando al ('data engineer') acudiendo orgánicamente en pro de rubricar u originar y finiquitar tal empeño y éxito (achieve this)?"
        ),
        "options": [
            "databricks bundle deployment link <bundle_job> <remote-job-id>",
            "databricks bundle deployment bind <bundle_job> <remote-job-id>",
            "databricks bundle deployment match <bundle_job> <remote-job-id>",
            "databricks bundle deployment mirror <bundle_job> <remote-job-id>"
        ],
        "answer": "databricks bundle deployment bind <bundle_job> <remote-job-id>",
        "explanation": (
            "El estandarte y orden correcta esgrimiendo asimilando en base orgánica es: databricks bundle deployment bind <bundle_job> <remote-job-id>. Dicha invocación ampara y propicia enganchar u atar fidedignamente ('links') el nudo de ejecución alejado a modo de matriz preexistente ('existing remote job') dirigiéndolo y asimilando su engranaje contra las fauces de tu bloque o (defined resource) albergado férreamente en cuna de Databricks Asset Bundle, blindando inmiscuyéndose en asimilar orgánicamente de forma tajante e irrenunciable ('ensuring') dictaminando a modo que las venideras metamorfosis u (updates) estipendiadas de lleno a la cúpula base (bundle's YAML definition) fustigarán y alterarán (modify) sin compasión al encadenado u arrastrado (linked job) arrinconando arcaicos o fallidos brotes en pos de erradicar u ('creating') amparando al que brote de la nada a título de ('new one')."
        ),
    },
    {
        "exam": 4,
        "id": "q59_delta_statistics_high_cardinality_string",
        "question": (
            "Amparando y abocándose a título orgánico forjado en las trincheras, una cuadrilla encuadrada en labores de base (data engineering team) aglutina bajo su feudo a una matriz u orgánica y ciclópea tabla Delta a título de ('user_messages') regida o cimentada subyacentemente al amparo de tal esquema o (schema):\n\n"
            "msg_id INT, user_id INT, msg_time TIMESTAMP, msg_title STRING, msg_body STRING\n\n"
            "Semejante y farragoso bloque asimilando de origen el componente u matriz ('msg_body field') asume operando y representando asimilando trazos de origen mensajes humanos u orgánicos amparando de origen y encuadrando su base en textos amorfos y desordenados libres de matriz a título ('free-form text'). El bloque entero de tu tabla se sume o padece mermando de forma dolorosa ahogándose transaccionalmente a título de ('performance issue') al recabar y verse instigada y sometida de modo asimilado u ('queried') al amparo de las cribas forzosas o ('filters') asimilando este frente u campo concreto.\n\n"
            "Sopesando en abanico analítico ¿cuál de todas estas sentencias describe o resume en veredicto franco orgánicamente rindiendo la causa ciegamente y certera justificando la de base u ('reason') arrastrando al atolladero del ('performance issue')?"
        ),
        "options": [
            "El bloque base o (table) carece de músculo o pericia para exprimir de lleno asimilando el salto u ('file skipping') amparando un uso colindante dictaminado de lleno a que no ha obrado asimilando orgánicamente con (Z-ORDER) blindando a tope y operando asimilando al ('msg_body column').",
            "El bloque base o (table) carece de músculo o pericia para exprimir de lleno asimilando el salto u ('file skipping') amparando un uso colindante dictaminado de lleno a que carece y obra de cimientos asimilando partición o ('partitioned') blindando a tope al ('msg_body column').",
            "El bloque base o (table) carece de músculo o pericia para exprimir de lleno asimilando el salto u ('file skipping') amparando un uso colindante dictaminado de lleno y asimilado en que las trazas matemáticas matrices (Delta Lake statistics) resultan mudas e infructuosas asimilando y tachadas de inoperantes ('uninformative') para con atributos o ('string fields') amparando y arrastrando un abanico y volumen inabarcable de valores únicos disparados a la alza ('very high cardinality')",
            "El bloque base o (table) carece de músculo o pericia para exprimir de lleno asimilando el salto u ('file skipping') amparando un uso colindante dictaminado de lleno a que las recabadoras o matrices (Delta Lake statistics) evaden por decreto operando y asimilando a un boicot de base ciegamente o censuradas ('not captured') sobre bases originarias amparando a columnas dictadas asimilando al molde y clase o ('STRING')"
        ],
        "answer": "El bloque base o (table) carece de músculo o pericia para exprimir de lleno asimilando el salto u ('file skipping') amparando un uso colindante dictaminado de lleno y asimilado en que las trazas matemáticas matrices (Delta Lake statistics) resultan mudas e infructuosas asimilando y tachadas de inoperantes ('uninformative') para con atributos o ('string fields') amparando y arrastrando un abanico y volumen inabarcable de valores únicos disparados a la alza ('very high cardinality')",
        "explanation": (
            "Semejante y farragoso bloque asimilando de origen el componente u matriz ('msg_body field') asume operando y representando asimilando trazos de origen mensajes humanos u orgánicos amparando de origen y encuadrando su base en textos amorfos y desordenados libres de matriz a título ('free-form text'). Ello ampara, deriva y arrastra incondicionalmente a base de pura lógica operando asimilando albergando un caos o abanico de origen desmedido, ergo goza empíricamente asimilando a una desbordante (very high cardinality). Las métricas u operativas de escrutinio numérico forzadas y aglutinadas asimilando (statistics gathered) sobre esta pared o matriz referencial operando bajo el amparo de (Delta Lake) pecan de inútiles asimilando a nivel genérico a modo ('uninformative') rindiendo cuentas asimiladas como baldías o desechables operando al servicio del salto (useless for data skipping)."
        ),
    }
]