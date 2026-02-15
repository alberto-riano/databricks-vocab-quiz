DATABRICKS_QUIZ = [
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
    {
        "exam": 1,
        "id": "q02_dlt_source_parquet_gcs",
        "question": (
            "Un Data Engineer está desarrollando un pipeline para procesar datos de sensores IoT almacenados en formato Parquet en Google "
            "Cloud Storage. El objetivo es crear una tabla Delta usando DLT (Delta Live Tables) que lea esos datos como fuente. "
            "Dado el contexto, ¿cuál de las siguientes opciones implementa correctamente esta funcionalidad en Python?"
        ),
        "options": [
            '@dlt.table\ndef iot_data():\n    return spark.read.parquet("gs://data/iot/raw/")',
            '@dlt.table\ndef iot_data():\n    return spark.write.parquet("gs://data/iot/raw/")',
            '@dlt.table\ndef iot_data():\n    return spark.parquet.load("gs://data/iot/raw/")',
            '@dlt.view\ndef iot_data():\n    return spark.read.format("parquet").load("gs://data/iot/raw/")',
        ],
        "answer": '@dlt.table\ndef iot_data():\n    return spark.read.parquet("gs://data/iot/raw/")',
        "explanation": (
            "Para definir una tabla en DLT, necesitas dos cosas:\n"
            "1. El decorador @dlt.table (para crear una tabla materializada, no una vista como en la opción @dlt.view).\n"
            "2. Devolver un DataFrame leido desde la fuente. Para ello usamos spark.read.parquet(...).\n"
            "• Nota sobre el error en la imagen: La opción seleccionada en la captura (spark.write.parquet) es incorrecta porque "
            "write se usa para guardar datos, no para leerlos como fuente de una tabla DLT, y además no devuelve un DataFrame."
        ),
    },
    {
        "exam": 1,
        "id": "q03_sql_udf_create_function",
        "question": "¿Cuál de los siguientes bloques de código puede usar un data engineer para crear una UDF (user defined function)?",
        "options": [
            "CREATE UDF plus_one(value INTEGER) RETURN value +1;",
            "CREATE UDF plus_one(value INTEGER) RETURNS INTEGER RETURN value +1;",
            "CREATE FUNCTION plus_one(value INTEGER) RETURNS INTEGER RETURN value +1;",
            "CREATE FUNCTION plus_one(value INTEGER) RETURN value +1",
        ],
        "answer": "CREATE FUNCTION plus_one(value INTEGER) RETURNS INTEGER RETURN value +1;",
        "explanation": (
            "Para crear una UDF permanente utilizando SQL en Databricks (y Spark SQL), la sintaxis correcta requiere tres "
            "elementos clave:\n"
            "1. CREATE FUNCTION: No se usa CREATE UDF.\n"
            "2. RETURNS <tipo>: Debes especificar explícitamente el tipo de dato que devolverá la función (en este caso, RETURNS INTEGER).\n"
            "3. RETURN <expresión>: El cuerpo de la función que calcula el resultado.\n"
            "• Nota: La opción que seleccionaste (CREATE UDF) es incorrecta porque esa sintaxis no es válida en este contexto."
        ),
    },
    {
        "exam": 1,
        "id": "q04_lakehouse_federation_purpose",
        "question": "¿Cuál es el propósito principal de Lakehouse Federation en una arquitectura de datos?",
        "options": [
            "Crear backups de datos almacenados en Databricks",
            "Migrar todos los datos a Databricks para un procesamiento centralizado",
            "Optimizar costes de almacenamiento comprimiendo datos",
            "Permitir consulta directa sobre múltiples fuentes sin duplicar datos",
        ],
        "answer": "Permitir consulta directa sobre múltiples fuentes sin duplicar datos",
        "explanation": (
            "Lakehouse Federation es una funcionalidad de Unity Catalog diseñada para la virtualización de datos. Su objetivo "
            "principal es permitirte conectar y consultar bases de datos externas (como PostgreSQL, MySQL, Snowflake o SQL Server) "
            "directamente desde Databricks sin necesidad de mover, copiar ni duplicar los datos (ETL). Los datos se quedan donde "
            "están, pero tú los ves y consultas como si estuvieran en Databricks."
        ),
    },
    {
        "exam": 1,
        "id": "q05_data_skew_not_solution",
        "question": (
            "Un data engineer analiza un dataset de eventos de clickstream de un sitio con mucho tráfico. El dataset incluye campos "
            "como user_id, timestamp, event_type y page_url. Durante un join entre los logs de clickstream y un dataset de perfiles de usuario "
            "(join por user_id), el rendimiento se degrada mucho por distribución desigual de datos. El análisis confirma skew causado por un pequeño "
            "subconjunto de usuarios que genera un volumen desproporcionado de eventos.\n"
            "¿Cuál de los siguientes enfoques NO es una solución adecuada para mitigar el skew en este escenario?"
        ),
        "options": [
            "Separar el procesamiento de claves sesgadas gestionando usuarios de alta frecuencia en un job dedicado.",
            "Hacer broadcast de las claves sesgadas a todos los worker nodes para evitar shuffle durante el join.",
            "Usar salting añadiendo un prefijo aleatorio a los valores sesgados de user_id para distribuir la carga entre particiones.",
            "Repartitionar el dataset de clickstream para incrementar el número de particiones antes del join.",
        ],
        "answer": "Hacer broadcast de las claves sesgadas a todos los worker nodes para evitar shuffle durante el join.",
        "explanation": (
            "El examen pregunta por la solución que NO funciona. El Broadcasting (retransmitir) consiste en enviar una copia completa "
            "de un dataset a la memoria RAM de todos los nodos de trabajo. Si intentas hacer esto con las \"claves sesgadas\" (skewed keys), "
            "que son justamente las que tienen un volumen masivo de datos (millones de eventos), saturarás la memoria de todos los nodos, "
            "provocando fallos (OOM). El broadcast es una técnica excelente para tablas pequeñas, pero desastrosa para la parte masiva de un join sesgado."
        ),
    },
    {
        "exam": 1,
        "id": "q06_databricks_sql_compute",
        "question": "En Databricks SQL, ¿qué recurso de cómputo está disponible?",
        "options": ["SQL warehouses", "Clusters single-node", "Clusters multi-node", "SQL engines"],
        "answer": "SQL warehouses",
        "explanation": (
            "En el entorno de Databricks SQL (la interfaz dedicada a analistas de datos y BI), el recurso de computación que se utiliza "
            "se llama SQL Warehouse (anteriormente conocido como SQL Endpoint).\n"
            "• Los SQL Warehouses están altamente optimizados para consultas SQL y dashboards, ofrecen escalado automático y tiempos de inicio rápidos "
            "(especialmente en la versión Serverless).\n"
            "• Los términos \"Clusters\" (Single-node o Multi-node) se utilizan en el entorno de Data Science & Engineering para ejecutar notebooks de Python/Scala "
            "o trabajos de Spark, no son el recurso nativo de la interfaz SQL."
        ),
    },
    {
        "exam": 1,
        "id": "q07_dabs_config_file",
        "question": "¿Cuál describe mejor el formato correcto del fichero de configuración de Databricks Asset Bundles?",
        "options": [
            "Un fichero YAML llamado bundle-config.yml con campos para variables de entorno y roles de usuario",
            "Un fichero YAML llamado databricks.yml que define la estructura del bundle, incluyendo targets, resources y configuraciones",
            "Un fichero JSON llamado databricks_asset.json que contiene definiciones de clusters y schedules de jobs",
            "Un fichero XML llamado asset_bundle.xml que especifica la ruta del workspace, IDs de jobs y especificaciones de cómputo",
        ],
        "answer": "Un fichero YAML llamado databricks.yml que define la estructura del bundle, incluyendo targets, resources y configuraciones",
        "explanation": (
            "Databricks Asset Bundles (DABs) es la herramienta moderna para definir y desplegar infraestructura como código (IaC) en Databricks.\n"
            "• Convención de nombres: El archivo de configuración debe llamarse obligatoriamente databricks.yml. Esta es la convención estricta que busca la CLI de Databricks.\n"
            "• Contenido: Este archivo actúa como el \"plano\" del proyecto, definiendo los targets (entornos como desarrollo, staging, producción), los resources (trabajos, pipelines DLT) y las variables.\n"
            "• Las otras opciones mencionan nombres de archivo inventados (bundle-config.yml, databricks_asset.json) que no funcionan con la herramienta."
        ),
    },
    {
        "exam": 1,
        "id": "q08_vacuum_default_retention",
        "question": "¿Cuál es el periodo de retención por defecto del comando VACUUM?",
        "options": ["365 días", "0 días", "30 días", "7 días"],
        "answer": "7 días",
        "explanation": (
            "El comando VACUUM se utiliza para eliminar archivos de datos antiguos que ya no están referenciados por la tabla Delta (limpieza).\n"
            "• Seguridad por defecto: Para evitar que borres accidentalmente archivos que aún podrías necesitar para \"viajar en el tiempo\" (Time Travel) "
            "o que están siendo usados por lectores lentos, Delta Lake impone un período de retención de seguridad predeterminado de 7 días.\n"
            "• Si intentas ejecutar VACUUM con una retención menor a 7 días (por ejemplo VACUUM table RETAIN 0 HOURS) sin activar una configuración especial "
            "de seguridad, el comando fallará y te dará un error de advertencia."
        ),
    },
    {
        "exam": 1,
        "id": "q09_sql_array_filter",
        "question": (
            "Dada la siguiente tabla faculties... Rellena el hueco para obtener los estudiantes matriculados en menos de 3 cursos del array students.\n"
            "SQL\n"
            "SELECT\n"
            "  faculty_id,\n"
            "  students,\n"
            "  ________ AS few_courses_students\n"
            "FROM faculties"
        ),
        "options": [
            "TRANSFORM (students, i -> i.total_courses < 3)",
            "TRANSFORM (students, total_courses < 3)",
            "FILTER (students, i -> i.total_courses < 3)",
            "FILTER (students, total_courses < 3)",
        ],
        "answer": "FILTER (students, i -> i.total_courses < 3)",
        "explanation": (
            "El objetivo es obtener un subconjunto del array original (quedarse solo con los estudiantes que cumplen una condición), "
            "por lo tanto, la función correcta es FILTER.\n"
            "• FILTER: Evalúa una condición y elimina los elementos que no la cumplen.\n"
            "• TRANSFORM: Se usa para modificar los datos (mapear), pero devuelve un array del mismo tamaño que el original, lo cual no es lo que se pide aquí.\n"
            "• Sintaxis Lambda (i -> i.field): En Databricks SQL, para iterar sobre un array de objetos (structs), se define una variable temporal (en este caso i) "
            "que representa cada elemento, y se accede a sus campos (i.total_courses)."
        ),
    },
    {
        "exam": 1,
        "id": "q10_medallion_gold_consumers",
        "question": "¿Cuál de las siguientes opciones suele consumir tablas Gold como fuente?",
        "options": ["Tablas Bronze", "Dashboards", "Tablas Silver", "Auto Loader"],
        "answer": "Dashboards",
        "explanation": (
            "En la arquitectura Medallion (Bronce → Plata → Oro), el flujo de datos siempre va hacia niveles de mayor refinamiento.\n"
            "• Gold Tables (Oro): Son la capa final. Contienen datos agregados, limpios y listos para el consumo de negocio.\n"
            "• Consumidores: Dado que son datos listos para el negocio, sus consumidores principales son herramientas de BI (Business Intelligence), reportes y Dashboards.\n"
            "• Las tablas Bronce y Plata son fuentes para llegar a Oro, no consumidores de ella."
        ),
    },
    {
        "exam": 1,
        "id": "q11_dlt_cdc_apply_changes",
        "question": (
            "Un data engineer diseña un pipeline de Delta Live Tables. El sistema origen genera ficheros con cambios capturados (CDC). "
            "Cada evento de cambio indica si el registro fue insertado, actualizado o borrado, además de una columna timestamp que indica el orden "
            "en que ocurrieron los cambios. El data engineer necesita actualizar una tabla destino a partir de esos eventos.\n"
            "¿Cuál de los siguientes comandos es el más adecuado para resolver este problema?"
        ),
        "options": ["UPDATE", "MERGE INTO", "COPY INTO", "APPLY CHANGES INTO"],
        "answer": "APPLY CHANGES INTO",
        "explanation": (
            "Esta es una funcionalidad específica y muy potente de Delta Live Tables (DLT) diseñada para manejar Change Data Capture (CDC).\n"
            "• APPLY CHANGES INTO: Simplifica drásticamente el procesamiento de flujos de cambios (inserts, updates, deletes). En lugar de escribir una lógica compleja "
            "de MERGE manual para manejar el orden de los eventos y las actualizaciones, este comando declarativo le dice a DLT: \"Toma esta fuente de cambios, usa "
            "esta columna de timestamp para saber cuál es el dato más reciente y actualiza la tabla destino automáticamente\".\n"
            "• Maneja automáticamente la des-duplicación y el ordenamiento de eventos fuera de secuencia (out-of-order data)."
        ),
    },
    {
        "exam": 1,
        "id": "q12_variable_explorer",
        "question": (
            "Un data engineer quiere validar que todas las variables definidas en su notebook de Python son valores numéricos. "
            "¿Cuál de las siguientes opciones permite inspección en tiempo real de las estructuras de datos y tipos en notebooks de Databricks?"
        ),
        "options": ["display()", "Notebook Variable Explorer", "print()", "dbutils.variables.summary()"],
        "answer": "Notebook Variable Explorer",
        "explanation": (
            "El Variable Explorer (Explorador de Variables) es una herramienta de la interfaz gráfica de los notebooks de Databricks (similar a lo que ofrecen IDEs como VS Code o PyCharm).\n"
            "• Permite ver en tiempo real una lista de todas las variables activas en la sesión, sus tipos de datos y sus valores actuales, sin necesidad de ejecutar código adicional.\n"
            "• display() sirve para visualizar DataFrames o gráficos, no para inspeccionar el estado de todas las variables del entorno.\n"
            "• print() solo muestra texto en la salida estándar y requiere que sepas qué variable quieres imprimir."
        ),
    },
    {
        "exam": 1,
        "id": "q13_cicd_dabs_best_practice",
        "question": (
            "Un equipo de data engineering desarrolla un pipeline ETL complejo en Databricks. Quieren que las configuraciones del workflow estén versionadas "
            "y se puedan desplegar de forma fiable entre entornos (staging y producción).\n"
            "¿Cuál es la solución más adecuada para conseguirlo?"
        ),
        "options": [
            "Aprovechar el versionado interno de Notebooks para control de código y crear jobs usando la UI de Databricks",
            "Guardar el código fuente en Git Folders y desplegar jobs usando la Databricks REST API",
            "Aprovechar el versionado interno de Notebooks para control de código y desplegar usando el entorno serverless del notebook",
            "Guardar el código fuente en Git Folders y desplegar jobs usando Databricks Asset Bundles (DAB)",
        ],
        "answer": "Guardar el código fuente en Git Folders y desplegar jobs usando Databricks Asset Bundles (DAB)",
        "explanation": (
            "Esta es la práctica recomendada moderna (\"best practice\") para la ingeniería de software en Databricks (CI/CD).\n"
            "• Git Folders (Repos): Aseguran que el código fuente esté versionado correctamente en un sistema estándar (GitHub, GitLab, etc.), no solo en el historial interno del notebook.\n"
            "• Databricks Asset Bundles (DABs): Es la herramienta diseñada específicamente para empaquetar y desplegar proyectos complejos (\"Infrastructure as Code\"). "
            "Permite definir la configuración del trabajo una sola vez (en archivos YAML) y desplegarla de manera idéntica y fiable en múltiples entornos (Staging, Prod) "
            "mediante comandos simples de CLI, garantizando consistencia."
        ),
    },
    {
        "exam": 1,
        "id": "q14_serverless_language_limits",
        "question": (
            "Un data engineer intenta ejecutar un script Scala en un compute serverless y falla. "
            "¿Cuál de las siguientes afirmaciones explica mejor el fallo?"
        ),
        "options": [
            "Scala no es un lenguaje soportado en la plataforma Databricks",
            "Scala requiere un classic compute con runtime 16.4 LTS o superior",
            "El compute serverless solo soporta SQL",
            "El compute serverless solo soporta SQL y Python",
        ],
        "answer": "El compute serverless solo soporta SQL y Python",
        "explanation": (
            "El cómputo \"Serverless\" en Databricks (especialmente en el contexto de Jobs y Notebooks serverless) tiene limitaciones específicas en cuanto a los lenguajes "
            "soportados en comparación con los clusters clásicos.\n"
            "• Actualmente, el entorno Serverless Compute for Notebooks/Jobs soporta principalmente SQL y Python.\n"
            "• Scala y R, que son lenguajes soportados en los clusters \"Classic\" (All-Purpose o Jobs Compute gestionados por el cliente), no están disponibles en la oferta serverless actual. Por eso el script falla."
        ),
    },
    {
        "exam": 1,
        "id": "q15_medallion_bronze_ingestion",
        "question": (
            "Dada la siguiente query de Structured Streaming:\n"
            "Python\n"
            "(spark.readStream\n"
            "  .format(\"cloudFiles\")\n"
            "  .option(\"cloudFiles.format\", \"json\")\n"
            "  .load(ordersLocation)\n"
            ".writeStream\n"
            "  .option(\"checkpointLocation\", checkpointPath)\n"
            "  .table(\"uncleanedOrders\")\n"
            ")\n"
            "¿Cuál de las siguientes opciones describe mejor el propósito de esta query dentro de una arquitectura Medallion?"
        ),
        "options": [
            "La query transfiere datos desde una tabla Gold hacia una aplicación de producción",
            "La query realiza ingesta raw hacia una tabla Bronze",
            "La query hace un salto de una tabla Silver a una tabla Gold",
            "La query hace un salto de una tabla Bronze a una tabla Silver",
        ],
        "answer": "La query realiza ingesta raw hacia una tabla Bronze",
        "explanation": (
            "En la arquitectura Medallion (Bronce, Plata, Oro), el flujo de datos se define por su nivel de refinamiento:\n"
            "• Bronze (Bronce): Es la capa de entrada. Almacena los datos \"crudos\" (raw) tal cual vienen de la fuente, a menudo con un historial de anexado (append-only).\n"
            "• Indicios en el código:\n"
            "1. format(\"cloudFiles\"): Esto es Auto Loader. Se utiliza típicamente para ingerir archivos desde almacenamiento en la nube (S3/ADLS/GCS) hacia Databricks. Esto marca el inicio del pipeline (Ingestión).\n"
            "2. table(\"uncleanedOrders\"): El nombre de la tabla destino sugiere que los datos aún no han sido limpiados (\"uncleaned\"). Los datos sin limpiar pertenecen a la capa Bronce. Las capas Plata y Oro contienen datos refinados."
        ),
    },
    {
        "exam": 1,
        "id": "q16_delta_not_true_xml",
        "question": "¿Cuál de las siguientes afirmaciones NO es cierta sobre Delta Lake?",
        "options": [
            "Delta Lake proporciona handling escalable de datos y metadatos",
            "Delta Lake se construye sobre formatos estándar: Parquet + XML",
            "Delta Lake ofrece garantías ACID",
            "Delta Lake ofrece audit history y time travel",
        ],
        "answer": "Delta Lake se construye sobre formatos estándar: Parquet + XML",
        "explanation": (
            "Esta afirmación es falsa (y por tanto la correcta en este contexto) porque Delta Lake no utiliza XML.\n"
            "• La realidad: Delta Lake se basa en archivos de datos Parquet (open source) combinados con un registro de transacciones (transaction log) basado en archivos JSON (_delta_log).\n"
            "• Las otras opciones son verdaderas: Delta Lake sí ofrece ACID, Time Travel, y manejo escalable de metadatos (puede manejar petabytes de datos mejor que un data lake tradicional)."
        ),
    },
    {
        "exam": 1,
        "id": "q17_pyspark_groupby_agg",
        "question": (
            "Un data engineer analiza transacciones de clientes y necesita calcular el máximo y mínimo importe por customer_id. "
            "Usa la siguiente estructura:\n"
            "Python\n"
            "result_df = df.________(\"customer_id\").agg(\n"
            "    F.max(\"transaction_amount\").alias(\"max_transaction\"),\n"
            "    F.min(\"transaction_amount\").alias(\"min_transaction\")\n"
            ")\n"
            "¿Qué función rellena correctamente el hueco?"
        ),
        "options": ["window", "withColumn", "groupBy", "select"],
        "answer": "groupBy",
        "explanation": (
            "Para realizar cálculos agregados (como sumas, promedios, máximos o mínimos) sobre grupos específicos de datos (en este caso, \"por cada cliente\"), "
            "es necesario utilizar la transformación groupBy().\n"
            "• El flujo estándar en PySpark es: DataFrame → groupBy(columna) → agg(funciones).\n"
            "• withColumn se usa para añadir o reemplazar columnas fila por fila, no para agregar. window se usa para cálculos de ventana (como rankings) pero tiene una sintaxis diferente (Window.partitionBy)."
        ),
    },
    {
        "exam": 1,
        "id": "q18_notebook_interactive_debugger",
        "question": (
            "Un data engineer trabaja en una transformación compleja en un notebook Python y obtiene None inesperados en una función. "
            "Quiere pausar en una línea concreta, ejecutar paso a paso y ver el estado de variables en tiempo real.\n"
            "¿Qué feature debería usar?"
        ),
        "options": ["%debug magic command", "%run magic command", "Databricks Web Terminal", "Notebook Interactive Debugger"],
        "answer": "Notebook Interactive Debugger",
        "explanation": (
            "Databricks introdujo un depurador interactivo (Interactive Debugger) nativo en los notebooks que funciona de manera muy similar a los depuradores de IDEs tradicionales (como VS Code o PyCharm).\n"
            "• Permite establecer puntos de interrupción (breakpoints) haciendo clic en el margen de la celda.\n"
            "• Puedes pausar la ejecución, avanzar paso a paso (\"step through\") y examinar el estado de las variables en ese momento exacto.\n"
            "• El comando %debug es una herramienta antigua de IPython para depuración post-mortem (después del error), y el %run es para ejecutar otros notebooks, no para depurar paso a paso."
        ),
    },
    {
        "exam": 1,
        "id": "q19_serverless_job_compute_hourly",
        "question": (
            "Un data engineer necesita ejecutar un notebook Python cada hora. El job procesa poco volumen. "
            "El entorno debe ser eficiente, sin necesidad de pre-warm del cluster, y dar rendimiento consistente.\n"
            "¿Qué opción de compute se recomienda?"
        ),
        "options": ["Serverless SQL Warehouse", "Classic All-purpose Cluster", "Classic Job Cluster", "Serverless Job Compute"],
        "answer": "Serverless Job Compute",
        "explanation": (
            "Para trabajos (jobs) programados que requieren un inicio rápido y eficiente sin necesidad de gestionar la infraestructura, Serverless Job Compute es la solución ideal.\n"
            "• \"No cluster pre-warming\": Los clusters clásicos tardan minutos en arrancar (provisioning). Serverless compute arranca casi instantáneamente, eliminando la necesidad de mantener clusters encendidos (pre-warmed) o esperar tiempos de inicio largos para trabajos pequeños.\n"
            "• Python Support: Serverless Job Compute soporta Python (a diferencia de SQL Warehouses que solo soportan SQL).\n"
            "• Eficiencia: Es ideal para trabajos pequeños y frecuentes (\"hourly\", \"small volumes\") porque solo pagas por los segundos exactos de ejecución sin tiempos muertos de arranque."
        ),
    },
    {
        "exam": 1,
        "id": "q20_magic_python_in_sql_notebook",
        "question": "Si el lenguaje por defecto del notebook es SQL, ¿cómo ejecutarías código Python en ese notebook SQL?",
        "options": [
            "No es posible; hay que cambiar el lenguaje por defecto del notebook a Python",
            "Añadir %python al inicio de la celda",
            "Añadir %language al inicio de la celda para forzar detección automática",
            "Primero hay que importar el módulo python en una celda",
        ],
        "answer": "Añadir %python al inicio de la celda",
        "explanation": (
            "En Databricks, los notebooks son políglotas, lo que significa que puedes mezclar lenguajes en un mismo notebook independientemente del lenguaje predeterminado que hayas elegido al crearlo.\n"
            "• Comandos Mágicos (Magic Commands): Para cambiar de lenguaje en una celda específica, se utiliza el símbolo de porcentaje % seguido del nombre del lenguaje al principio de la celda.\n"
            "• Si el notebook es SQL, poner %python al inicio de una celda le indica al driver que interprete el código de esa celda como Python. (De igual forma, usarías %sql en un notebook de Python)."
        ),
    },
    {
        "exam": 1,
        "id": "q21_dlt_continuous_production",
        "question": (
            "El equipo tiene un pipeline DLT que actualiza todas las tablas a intervalos definidos hasta que se detiene manualmente. "
            "Los recursos de compute se terminan cuando se para el pipeline.\n"
            "¿Cuál describe mejor los modos de ejecución de este pipeline DLT?"
        ),
        "options": [
            "Triggered Pipeline bajo Production mode",
            "Continuous Pipeline bajo Production mode",
            "Triggered Pipeline bajo Development mode",
            "Continuous Pipeline bajo Development mode",
        ],
        "answer": "Continuous Pipeline bajo Production mode",
        "explanation": (
            "Para responder esto, hay que analizar dos comportamientos descritos en la pregunta:\n"
            "1. \"Until manually stopped\" (Hasta que se detiene manualmente): Esto define el modo Continuous (Continuo). En DLT, un pipeline en modo Triggered se ejecuta una vez, procesa los datos disponibles y se apaga automáticamente. Si el pipeline se queda corriendo indefinidamente hasta que alguien lo para, es Continuous.\n"
            "2. \"Compute resources terminate when the pipeline is stopped\" (Los recursos terminan al parar): Esto es característico del modo Production. En el modo Development (Desarrollo), el cluster se mantiene encendido (en estado de espera) durante un tiempo después de que el pipeline se detiene o falla, para permitirte reiniciar rápidamente mientras desarrollas sin esperar a que se provisione un nuevo cluster. En Producción, se prioriza el ahorro de costes y limpieza, por lo que al parar el pipeline, se mata el cluster inmediatamente."
        ),
    },
    {
        "exam": 1,
        "id": "q22_delta_sharing_egress_costs",
        "question": (
            "Una empresa usa Delta Sharing para colaborar con un partner. La empresa almacena los datos en AWS US-East-1, "
            "mientras que el partner está en Azure en Europa. Tras iniciar un sharing masivo, la empresa ve costes inesperados.\n"
            "¿Cuál es la razón más probable del incremento de costes?"
        ),
        "options": [
            "Costes de storage porque Delta Sharing requiere replicación de datos entre clouds y regiones",
            "Se aplica una tarifa plana del protocolo Delta Sharing por todos los datos compartidos",
            "Costes de DBU por ejecutar servidores de Delta Sharing cruzando clouds",
            "Se incurren fees de egress al transferir datos entre proveedores cloud y regiones",
        ],
        "answer": "Se incurren fees de egress al transferir datos entre proveedores cloud y regiones",
        "explanation": (
            "Aunque Delta Sharing es un protocolo abierto y gratuito (no cobra licencias extra), la infraestructura de nube subyacente sí cobra por el movimiento de datos.\n"
            "• Egress Fees (Tarifas de salida): Los proveedores de nube (como AWS, Azure, Google Cloud) cobran cuando los datos \"salen\" de su red o región.\n"
            "• En este caso, los datos salen de AWS (EE.UU.) hacia Azure (Europa). Al cruzar tanto fronteras de proveedores como de regiones geográficas, se aplican tarifas de salida de datos (Network Egress) que pueden ser significativas si el volumen de datos es grande."
        ),
    },
    {
        "exam": 1,
        "id": "q23_sql_warehouse_auto_stop_benefit",
        "question": "¿Cuál es el beneficio de usar Auto Stop en un SQL Warehouse de Databricks?",
        "options": [
            "Mejora el rendimiento del warehouse deteniendo automáticamente servicios idle",
            "Minimiza el tiempo total en ejecución del warehouse",
            "Aumenta la disponibilidad deteniendo automáticamente queries largas",
            "Aporta más seguridad deteniendo puertos sin uso del warehouse",
        ],
        "answer": "Minimiza el tiempo total en ejecución del warehouse",
        "explanation": (
            "La función de Auto Stop (Auto-detención) está diseñada exclusivamente para el ahorro de costes.\n"
            "• Funcionamiento: Si el SQL Warehouse no recibe ninguna consulta (está inactivo o idle) durante un tiempo determinado (por ejemplo, 10 minutos), se apaga automáticamente.\n"
            "• Beneficio: Al apagarse cuando no se usa, reduce el tiempo total de ejecución (running time) por el que tienes que pagar.\n"
            "• Nota sobre el error: La opción que seleccionaste (\"stopping long-running SQL queries\") se refiere a Query Timeouts (límites de tiempo de consulta), no al Auto Stop del warehouse completo."
        ),
    },
    {
        "exam": 1,
        "id": "q24_time_travel_missing_files_vacuum",
        "question": (
            "Un data engineer está intentando usar la funcionalidad de Delta Time Travel para hacer rollback de una tabla a una versión anterior, "
            "pero recibe un error indicando que los archivos de datos ya no están presentes.\n"
            "¿Cuál de los siguientes comandos se ejecutó sobre la tabla y causó el borrado de los archivos de datos?"
        ),
        "options": ["VACUUM", "ZORDER BY", "DELETE", "OPTIMIZE"],
        "answer": "VACUUM",
        "explanation": (
            "El \"Time Travel\" de Delta Lake funciona gracias a que, cuando actualizas o borras datos, Delta no elimina los archivos físicos antiguos inmediatamente, sino que los marca como inactivos en el registro de transacciones.\n"
            "• El culpable: El comando VACUUM es el encargado de la limpieza física. Su función es escanear el almacenamiento y borrar permanentemente los archivos de datos que ya no son necesarios por la versión actual de la tabla y que son más antiguos que el periodo de retención.\n"
            "• Consecuencia: Una vez ejecutado VACUUM, los archivos físicos desaparecen del disco. Si intentas hacer un rollback a una versión que dependía de esos archivos borrados, recibirás el error de que \"los archivos de datos ya no están presentes\".\n"
            "* Nota: El comando DELETE es una operación lógica (marca datos como borrados), pero los archivos físicos siguen ahí hasta que pasas el VACUUM."
        ),
    },
    {
        "exam": 1,
        "id": "q25_delete_keep_salary_logic",
        "question": (
            "El equipo de data engineering tiene una tabla Delta llamada employees que contiene información personal de los empleados, incluyendo sus salarios brutos.\n"
            "¿Cuál de los siguientes bloques de código mantendrá en la tabla únicamente a los empleados con un salario mayor que 3000?"
        ),
        "options": [
            "UPDATE employees WHERE salary <= 3000 WHEN MATCHED DELETE;",
            "DELETE FROM employees WHERE salary > 3000;",
            "SELECT CASE WHEN salary <= 3000 THEN DELETE ELSE UPDATE END FROM employees;",
            "DELETE FROM employees WHERE salary <= 3000;",
        ],
        "answer": "DELETE FROM employees WHERE salary <= 3000;",
        "explanation": (
            "Este es un ejercicio de lógica inversa.\n"
            "• El objetivo: Quedarse (keep) con los salarios mayores a 3000 (> 3000).\n"
            "• La acción: Para lograr esto mediante un borrado (DELETE), debes eliminar todo lo que NO cumpla esa condición.\n"
            "• La lógica: Lo opuesto a \"Mayor que 3000\" es \"Menor o igual a 3000\" (≤ 3000). Por tanto, si borras los salarios menores o iguales a 3000, lo único que queda en la tabla son los salarios mayores a 3000.\n"
            "• Nota sobre el error: La opción que seleccionaste (DELETE ... WHERE salary > 3000) hace justo lo contrario: borra a los que querías salvar."
        ),
    },
    {
        "exam": 1,
        "id": "q26_databricks_web_app_location",
        "question": "¿Cuál de las siguientes ubicaciones aloja la aplicación web de Databricks?",
        "options": ["Cluster gestionado por Databricks", "Control plane", "Data plane", "Cuenta cloud del cliente"],
        "answer": "Control plane",
        "explanation": (
            "La arquitectura de Databricks se divide en dos planos principales:\n"
            "• Control Plane (Plano de Control): Es gestionado íntegramente por Databricks en su propia cuenta de nube. Aquí es donde vive la aplicación web "
            "(la interfaz de usuario que ves en el navegador), así como los notebooks, la gestión de usuarios, el historial de trabajos y la configuración de clusters.\n"
            "• Data Plane (Plano de Datos): Es donde residen tus datos y donde se realiza el procesamiento pesado (los clusters). Generalmente vive en tu cuenta de nube (Customer Cloud Account) para mayor seguridad."
        ),
    },
    {
        "exam": 1,
        "id": "q27_medallion_silver_description",
        "question": "En la arquitectura Medallion, ¿cuál de las siguientes afirmaciones describe mejor las tablas de la capa Silver?",
        "options": [
            "La estructura de la tabla en esta capa se parece a la estructura del sistema fuente, con columnas adicionales de metadatos como la hora de carga y el nombre del fichero de entrada.",
            "Mantienen datos que alimentan analítica, machine learning y aplicaciones de producción",
            "Mantienen datos raw ingeridos desde distintas fuentes",
            "Proporcionan una vista más refinada de los datos raw, donde se filtran, limpian y enriquecen.",
        ],
        "answer": "Proporcionan una vista más refinada de los datos raw, donde se filtran, limpian y enriquecen.",
        "explanation": (
            "En la arquitectura Medallion, cada capa tiene un propósito específico:\n"
            "• Bronze (Bronce): Datos \"crudos\" (raw), copia exacta de la fuente. (Descripción de la opción 1 y 3).\n"
            "• Silver (Plata): Datos limpios, filtrados y enriquecidos. Es la capa de \"calidad empresarial\". Aquí se eliminan duplicados, se corrigen tipos de datos y se unen tablas. Es la base para el análisis, pero aún no está agregada para reportes finales.\n"
            "• Gold (Oro): Datos agregados y listos para el consumo de negocio (Dashboards, ML). (Descripción de la opción 2)."
        ),
    },
    {
        "exam": 1,
        "id": "q28_autoloader_schema_evolution_fail",
        "question": (
            "Un data engineer ha implementado el siguiente código de ingesta en streaming usando Databricks Auto Loader:\n"
            "Python\n"
            "spark.readStream \\\n"
            "    .format(\"cloudFiles\") \\\n"
            "    .option(\"cloudFiles.schemaEvolutionMode\", \"failOnNewColumns\") \\\n"
            "    .load(\"s3://vendor/raw/sales/json/\")\n"
            "¿Cuál es el comportamiento esperado de este streaming job si aparece una nueva columna en los ficheros JSON entrantes y dicha columna no forma parte del schema original?"
        ),
        "options": [
            "El stream falla y todas las columnas nuevas se guardan en una rescued data column para su procesamiento posterior.",
            "El stream falla temporalmente pero continúa ignorando las columnas nuevas sin actualizar el schema.",
            "El stream falla y no se reiniciará a menos que el schema se actualice manualmente o se elimine el fichero de datos problemático.",
            "El stream falla, pero se reinicia automáticamente tras actualizar el schema con las columnas nuevas.",
        ],
        "answer": "El stream falla y no se reiniciará a menos que el schema se actualice manualmente o se elimine el fichero de datos problemático.",
        "explanation": (
            "El modo failOnNewColumns es la configuración más estricta de Auto Loader.\n"
            "• Comportamiento: Al detectar una columna que no existe en el esquema definido, Databricks detiene el flujo inmediatamente para proteger la integridad de la tabla destino.\n"
            "• Resolución: No se reinicia solo. Requiere intervención manual: o bien actualizas tu código/esquema para aceptar la nueva columna y reinicias el stream, o borras el archivo que contiene la columna extra si son datos corruptos.\n"
            "• Nota: Si quisieras que siguiera funcionando guardando los datos extra, usarías el modo rescue (por defecto) o addNewColumns para evolucionar el esquema automáticamente."
        ),
    },
    {
        "exam": 1,
        "id": "q29_spot_instances_benefit",
        "question": "¿Cuál es el beneficio principal de usar spot instances en clusters de Databricks?",
        "options": [
            "Costes de cómputo significativamente más bajos",
            "Mayor seguridad y compliance",
            "Tiempo de ejecución del job garantizado",
            "Menor latencia de almacenamiento de datos",
        ],
        "answer": "Costes de cómputo significativamente más bajos",
        "explanation": (
            "Las Spot Instances (instancias puntuales) son máquinas virtuales que los proveedores de nube (AWS, Azure, GCP) ofrecen con un descuento masivo "
            "(a menudo entre un 60% y un 90% más baratas) porque son capacidad sobrante que no están usando en ese momento.\n"
            "• Beneficio: El ahorro de costes es drástico.\n"
            "• Riesgo: El proveedor puede reclamarlas (quitártelas) con muy poco aviso si necesita esa capacidad para clientes que pagan la tarifa completa (On-Demand). "
            "Por eso son ideales para trabajos que toleran fallos o reintentos, pero no para trabajos críticos que necesitan garantía absoluta de tiempo."
        ),
    },
    {
        "exam": 1,
        "id": "q30_create_table_comment_ctas",
        "question": "¿Cuál de los siguientes comandos puede usar un data engineer para crear una tabla nueva junto con un comentario?",
        "options": [
            'CREATE TABLE payments AS SELECT * FROM bank_transactions COMMENT "..."',
            'CREATE TABLE payments COMMENT("...") AS SELECT * FROM bank_transactions',
            'COMMENT("...") CREATE TABLE payments AS SELECT * FROM bank_transactions',
            'CREATE TABLE payments COMMENT "This table contains sensitive information" AS SELECT * FROM bank_transactions',
        ],
        "answer": 'CREATE TABLE payments COMMENT "This table contains sensitive information" AS SELECT * FROM bank_transactions',
        "explanation": (
            "Esta pregunta evalúa tu conocimiento de la sintaxis SQL específica de Databricks (Spark SQL) para crear tablas a partir de una consulta "
            "(CTAS - Create Table As Select) añadiendo metadatos.\n"
            "• Sintaxis correcta: La cláusula COMMENT debe ir inmediatamente después del nombre de la tabla (y antes de la cláusula AS SELECT).\n"
            "• Estructura: CREATE TABLE [nombre_tabla] COMMENT \"[texto]\" AS SELECT ...\n"
            "• Esto es útil para la gobernanza de datos, ya que el comentario aparece en el catálogo de datos (Unity Catalog o Hive Metastore) para que otros usuarios sepan qué contiene la tabla."
        ),
    },
    {
        "exam": 1,
        "id": "q31_uc_modify_privilege",
        "question": (
            "Un data engineer utiliza la siguiente query SQL:\n"
            "SQL\n"
            "GRANT MODIFY ON TABLE employees TO hr_team\n"
            "¿Cuál de las siguientes opciones describe la capacidad que otorga el privilegio MODIFY?"
        ),
        "options": [
            "Da la capacidad de añadir, actualizar o borrar datos dentro de la tabla",
            "Da la capacidad de modificar datos en la tabla",
            "Da la capacidad de borrar datos de la tabla",
            "Da la capacidad de añadir datos de la tabla",
        ],
        "answer": "Da la capacidad de añadir, actualizar o borrar datos dentro de la tabla",
        "explanation": (
            "En el modelo de permisos de Databricks (especialmente en Unity Catalog), los privilegios son granulares:\n"
            "• SELECT: Permite leer (consultar) datos.\n"
            "• MODIFY: Permite modificar los datos existentes o añadir nuevos. Esto engloba específicamente las operaciones DML (Data Manipulation Language): "
            "INSERT (añadir), UPDATE (actualizar), DELETE (borrar), MERGE y TRUNCATE.\n"
            "• Por lo tanto, otorgar MODIFY es dar permiso completo para alterar el contenido de los datos de la tabla, no su estructura (para cambiar la estructura, como añadir columnas, se necesitaría ALTER o ser el dueño)."
        ),
    },
    {
        "exam": 1,
        "id": "q32_grant_all_privileges_syntax",
        "question": "¿Cuál de los siguientes comandos puede usar un data engineer para conceder permisos completos al equipo de HR sobre la tabla employees?",
        "options": [
            "GRANT ALL PRIVILEGES ON TABLE employees TO hr_team",
            "GRANT ALL PRIVILEGES ON employees TO hr_team",
            "GRANT FULL PRIVILEGES ON TABLE employees TO hr_team",
            "GRANT SELECT, MODIFY, CREATE, READ_METADATA ON TABLE employees TO hr_team",
        ],
        "answer": "GRANT ALL PRIVILEGES ON TABLE employees TO hr_team",
        "explanation": (
            "Esta pregunta es muy específica sobre la sintaxis correcta en Databricks SQL (especialmente en Unity Catalog).\n"
            "1. ALL PRIVILEGES: Es la palabra clave estándar en SQL para otorgar \"todos\" los permisos disponibles. FULL PRIVILEGES no existe en la sintaxis estándar.\n"
            "2. ON TABLE: Databricks requiere (o prefiere fuertemente en este examen) que especifiques el tipo de objeto sobre el que estás otorgando permisos para evitar ambigüedades. "
            "Por eso la opción que seleccionaste (ON employees) fue marcada como incorrecta; faltaba especificar ON TABLE.\n"
            "3. Sintaxis completa: GRANT [Privilegio] ON [Tipo Objeto] [Nombre Objeto] TO [Principal]."
        ),
    },
    {
        "exam": 1,
        "id": "q33_data_plane_customer_account",
        "question": "Según la arquitectura Lakehouse de Databricks, ¿cuál de los siguientes elementos se encuentra en la cuenta cloud del cliente?",
        "options": ["Aplicación web de Databricks", "Máquinas virtuales del cluster", "Workflows", "Notebooks"],
        "answer": "Máquinas virtuales del cluster",
        "explanation": (
            "Esta pregunta es la contraparte de la Pregunta 26.\n"
            "• Control Plane (Gestionado por Databricks): Contiene la aplicación web, los notebooks (la interfaz y el código guardado), el programador de trabajos (Workflows) y la gestión de usuarios.\n"
            "• Data Plane (Gestionado por el Cliente): Es donde ocurre el procesamiento real para mantener la seguridad y privacidad de los datos. En la arquitectura clásica (no serverless), las máquinas virtuales (VMs) que forman los clusters se despliegan dentro de la cuenta de nube del cliente (AWS/Azure/GCP). Así, los datos se procesan sin salir de tu entorno de nube."
        ),
    },
    {
        "exam": 1,
        "id": "q34_uc_data_lineage",
        "question": (
            "\"Una funcionalidad que ilustra la relación entre distintos activos de datos incluyendo tablas, queries, notebooks y dashboards, permitiendo a los usuarios "
            "trazar el origen y el flujo de los datos a través de toda la plataforma lakehouse.\"\n"
            "¿Qué se está describiendo en la afirmación anterior?"
        ),
        "options": ["DAGs de Delta Live Tables", "Databricks Lakeflow", "Databricks Jobs",
                    "Data Lineage de Unity Catalog"],
        "answer": "Data Lineage de Unity Catalog",
        "explanation": (
            "El concepto clave aquí es la trazabilidad \"across the entire lakehouse platform\" (a través de toda la plataforma).\n"
            "• Unity Catalog Data Lineage: Es la función de gobernanza que captura automáticamente el flujo de datos en tiempo de ejecución. Muestra visualmente cómo los datos se mueven desde la fuente, pasan por tablas, queries, notebooks, y terminan en dashboards.\n"
            "• Diferencia clave: Los DLT DAGs (grafos dirigidos acíclicos) también muestran flujo de datos, pero solo dentro de un pipeline específico de Delta Live Tables. No rastrean qué pasa después con esos datos (por ejemplo, si un dashboard los lee o si un notebook externo los consulta). Unity Catalog sí lo hace a nivel global."
        ),
    },
    {
        "exam": 1,
        "id": "q35_job_email_notifications",
        "question": (
            "Un equipo de data engineering tiene un Job de múltiples tareas (multi-tasks) que es de larga duración. El equipo necesita ser notificado cuando finalice una ejecución de este job.\n"
            "¿Cuál de los siguientes enfoques se puede usar para enviar emails a los miembros del equipo cuando el job termina?"
        ),
        "options": [
            "Solo el owner del Job puede configurarse para recibir notificaciones cuando el job termina",
            "Pueden usar la Job API para enviar emails programáticamente según el estado de cada task",
            "Pueden configurar las notificaciones por email en la página del job",
            "No hay forma de notificar a usuarios cuando el job termina",
        ],
        "answer": "Pueden configurar las notificaciones por email en la página del job",
        "explanation": (
            "Databricks tiene una funcionalidad nativa y muy sencilla para esto en la interfaz de usuario (UI) de Jobs.\n"
            "• Configuración: En la configuración del Job (apartado \"Job details\" o \"Notifications\"), puedes añadir direcciones de correo electrónico (o destinos del sistema como Slack/Teams si están configurados) para recibir alertas.\n"
            "• Flexibilidad: Puedes configurar alertas para diferentes eventos: cuando el trabajo empieza (Start), cuando tiene éxito (Success) o cuando falla (Failure).\n"
            "• Destinatarios: No está limitado al dueño del trabajo (\"Job owner\"); puedes añadir una lista de distribución del equipo (team@company.com) o múltiples correos individuales."
        ),
    },
    {
        "exam": 1,
        "id": "q36_autoloader_definition",
        "question": "¿Cuál de las siguientes afirmaciones describe mejor Auto Loader?",
        "options": [
            "Auto Loader monitoriza una ubicación fuente donde se acumulan ficheros, para identificar e ingerir únicamente los ficheros nuevos que llegan en cada ejecución. Los ficheros ya ingeridos en ejecuciones anteriores se omiten.",
            "Auto Loader permite clonar una tabla Delta origen a un destino en una versión específica.",
            "Auto Loader permite aplicar un feed de Change Data Capture (CDC) para actualizar tablas basándose en cambios capturados en los datos fuente.",
            "Auto Loader habilita operaciones eficientes de insert, update, deletes y rollback añadiendo una capa de almacenamiento que aporta mayor fiabilidad a los data lakes.",
        ],
        "answer": "Auto Loader monitoriza una ubicación fuente donde se acumulan ficheros, para identificar e ingerir únicamente los ficheros nuevos que llegan en cada ejecución. Los ficheros ya ingeridos en ejecuciones anteriores se omiten.",
        "explanation": (
            "• Qué es Auto Loader (cloudFiles): Es una herramienta optimizada para la ingestión incremental de archivos. Su función principal es \"escuchar\" una carpeta en la nube (S3/ADLS/GCS), detectar cuándo llega un archivo nuevo y procesarlo una sola vez.\n"
            "* Por qué tu respuesta fue incorrecta: Seleccionaste la opción que describe a Delta Lake (\"efficient insert, update, deletes, and rollback capabilities...\"). Delta Lake es el formato de almacenamiento; Auto Loader es el mecanismo para meter datos dentro de Delta Lake desde archivos externos.\n"
            "• Nota: La opción de CDC se refiere a Delta Live Tables (APPLY CHANGES INTO)."
        ),
    },
    {
        "exam": 1,
        "id": "q37_liquid_clustering_function",
        "question": "¿Cuál es la función principal de Liquid Clustering en Databricks?",
        "options": [
            "Automatizar la creación de nuevos data pipelines",
            "Mejorar la velocidad de conectividad de red entre nodos",
            "Optimizar incrementalmente el layout de los datos para mejorar el rendimiento de las queries",
            "Encriptar datos almacenados en Delta Lake",
        ],
        "answer": "Optimizar incrementalmente el layout de los datos para mejorar el rendimiento de las queries",
        "explanation": (
            "Liquid Clustering es una funcionalidad moderna de Delta Lake diseñada para reemplazar y mejorar el particionado tradicional (Partitioning) y el Z-Ordering.\n"
            "• El problema del particionado tradicional: Si eliges mal las columnas de partición, puedes acabar con archivos muy pequeños (small files) o skew data, lo que afecta el rendimiento. Además, cambiar las columnas de partición requiere reescribir toda la tabla.\n"
            "• Liquid Clustering: Soluciona esto optimizando automática e incrementalmente cómo se guardan los datos físicos en el disco basándose en claves de clustering flexibles.\n"
            "• Objetivo: Mejorar el \"Data Skipping\" (saltar datos irrelevantes al leer) para que las consultas sean mucho más rápidas, sin la rigidez del particionado de directorios.\n"
            "• Nota: La opción que seleccionaste (\"automate creation of pipelines\") se refiere más a Delta Live Tables."
        ),
    },
    {
        "exam": 1,
        "id": "q38_streaming_trigger_processing_time",
        "question": (
            "Dada la siguiente query de Structured Streaming:\n"
            "Python\n"
            "(spark.table(\"orders\")\n"
            "    .withColumn(\"total_after_tax\", col(\"total\") + col(\"tax\"))\n"
            "  .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .outputMode(\"append\")\n"
            "    .________\n"
            "    .table(\"new_orders\")\n"
            ")\n"
            "Rellena el hueco para que la query ejecute un micro-batch procesando datos cada 2 minutos."
        ),
        "options": [
            'trigger(once="2 minutes")',
            'trigger("2 minutes")',
            'processingTime("2 minutes")',
            'trigger(processingTime="2 minutes")',
        ],
        "answer": 'trigger(processingTime="2 minutes")',
        "explanation": (
            "En Structured Streaming, la función trigger() controla cuándo se procesan los datos nuevos.\n"
            "• Sintaxis correcta: El método trigger acepta un parámetro llamado processingTime (tiempo de procesamiento) que define el intervalo de micro-lotes. La sintaxis exacta es .trigger(processingTime=\"2 minutes\").\n"
            "• Otras opciones:\n"
            "o trigger(once=True): Procesa un solo lote y se detiene (no acepta tiempo como \"2 minutes\").\n"
            "o trigger(\"2 minutes\"): Sintaxis inválida.\n"
            "o AvailableNow: Es otro modo de trigger, pero la pregunta pide específicamente un intervalo de tiempo fijo (\"every 2 minutes\")."
        ),
    },
    {
        "exam": 1,
        "id": "q39_autoloader_path_glob_filter",
        "question": (
            "Un entorno de producción tiene un bucket S3 que recibe miles de archivos de imagen diariamente en distintos formatos (.png, .jpg, .gif). "
            "Se le pide a un data engineer modificar el siguiente script de ingesta en streaming para asegurar que solo se procesen ficheros .jpg.\n"
            "Python\n"
            "df = spark.readStream \\\n"
            "    .format(\"cloudFiles\") \\\n"
            "    .option(\"cloudFiles.format\", \"binaryFile\") \\\n"
            "    .option(\"________________\", \"*.jpg\") \\\n"
            "    .load(\"s3://shop/raw/invoices/\")"
        ),
        "options": ["fileExtension", "cloudFiles.pathGlobFilter", "cloudFiles.fileExtension", "pathGlobFilter"],
        "answer": "pathGlobFilter",
        "explanation": (
            "Para filtrar archivos basándose en patrones de nombres (como extensiones) en Databricks (tanto en lectura por lotes estándar como en Structured Streaming/Auto Loader), se utiliza la opción nativa de Spark.\n"
            "• pathGlobFilter: Es una opción estándar de Apache Spark para fuentes de datos basadas en archivos. Permite especificar un patrón \"glob\" (como *.jpg) para incluir solo los archivos que coincidan con ese patrón.\n"
            "• Por qué funciona con Auto Loader: Auto Loader (cloudFiles) se basa en la fuente de archivos de Structured Streaming, por lo que hereda y respeta las opciones estándar como pathGlobFilter, recursiveFileLookup, etc."
        ),
    },
    {
        "exam": 1,
        "id": "q40_dlt_expectations_default_retain",
        "question": (
            "Un data engineer ha definido la siguiente constraint de calidad de datos en un pipeline de Delta Live Tables:\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ________\n"
            "Rellena el hueco para que los registros que violen esta constraint se añadan a la tabla destino y se reporten en métricas."
        ),
        "options": [
            "ON VIOLATION FAIL UPDATE",
            "No es necesario añadir una cláusula ON VIOLATION. Por defecto, los registros que violen la constraint se mantendrán y se reportarán como inválidos en el event log",
            "ON VIOLATION NONE",
            "ON VIOLATION ADD ROW",
        ],
        "answer": "No es necesario añadir una cláusula ON VIOLATION. Por defecto, los registros que violen la constraint se mantendrán y se reportarán como inválidos en el event log",
        "explanation": (
            "Las \"Expectations\" (expectativas) en Delta Live Tables tienen tres niveles de severidad para gestionar la calidad de los datos:\n"
            "1. Retain (Retener): Si se viola la regla, el registro se mantiene en la tabla destino y la violación se reporta en las métricas. Este es el comportamiento por defecto (usando solo la palabra clave EXPECT). No necesitas añadir ninguna cláusula ON VIOLATION.\n"
            "2. Drop (Descartar): Si se viola la regla, el registro se elimina. Se usa ON VIOLATION DROP ROW.\n"
            "2. Fail (Fallar): Si se viola la regla, el pipeline se detiene inmediatamente. Se usa ON VIOLATION FAIL UPDATE."
        ),
    },
    {
        "exam": 1,
        "id": "q41_drop_external_table_keeps_files",
        "question": (
            "Al hacer DROP de una tabla Delta, ¿cuál de las siguientes opciones explica por qué solo se elimina la metadata de la tabla, mientras que los data files se mantienen en el storage?"
        ),
        "options": [
            "La tabla es external",
            "El usuario que ejecuta el comando no tiene permisos para borrar los data files",
            "La tabla es managed",
            "Delta evita borrar archivos por debajo del umbral de retención para asegurar que no haya operaciones de larga duración que sigan referenciando archivos que se van a borrar",
        ],
        "answer": "La tabla es external",
        "explanation": (
            "Esta es una diferencia fundamental en Databricks y Delta Lake:\n"
            "• Managed Tables (Tablas Gestionadas): Databricks gestiona tanto los metadatos como los archivos de datos físicos en su almacenamiento. Si haces DROP TABLE, se borra todo (metadatos + archivos).\n"
            "• External Tables (Tablas Externas o Unmanaged): Tú gestionas los archivos de datos en tu propio almacenamiento (S3, ADLS, GCS) y le dices a Databricks dónde están. Si haces DROP TABLE, Databricks solo borra el \"puntero\" (los metadatos en el catálogo). Los archivos físicos (.parquet) permanecen intactos en el almacenamiento en la nube para seguridad."
        ),
    },
    {
        "exam": 1,
        "id": "q42_cron_syntax_jobs",
        "question": "¿Cuál de las siguientes opciones describe la sintaxis Cron en Databricks Jobs?",
        "options": [
            "Es una expresión para representar el run timeout de un job",
            "Es una expresión para representar un schedule complejo de un job que puede definirse programáticamente",
            "Es una expresión para representar la política de reintentos de un job",
            "Es una expresión para representar el máximo de ejecuciones concurrentes de un job",
        ],
        "answer": "Es una expresión para representar un schedule complejo de un job que puede definirse programáticamente",
        "explanation": (
            "La sintaxis Cron (original de los sistemas Unix/Linux) es el estándar de oro para programar tareas en momentos específicos.\n"
            "• Propósito: Permite definir horarios complejos que a veces no se pueden configurar con selectores simples de \"Día/Hora\". Por ejemplo: \"Ejecutar cada primer lunes del mes a las 3 AM\".\n"
            "• Formato: Es una cadena de texto (expression) con 5 o 6 campos (minuto, hora, día del mes, mes, día de la semana, etc.). Ejemplo: 0 0 12 * * ? (todos los días a las 12:00).\n"
            "• En Databricks: Se usa en la configuración de \"Schedules\" de los Jobs, especialmente cuando usas la API o necesitas precisión programática."
        ),
    },
    {
        "exam": 1,
        "id": "q43_sql_union_no_duplicates",
        "question": (
            "Dadas las dos tablas students_course_1 y students_course_2, ¿cuál de los siguientes comandos puede usar un data engineer para obtener todos los estudiantes "
            "de ambas tablas sin registros duplicados?"
        ),
        "options": [
            "SELECT * FROM students_course_1 INTERSECT SELECT * FROM students_course_2",
            "SELECT * FROM students_course_1 CROSS JOIN SELECT * FROM students_course_2",
            "SELECT * FROM students_course_1 UNION SELECT * FROM students_course_2",
            "SELECT * FROM students_course_1 INNER JOIN SELECT * FROM students_course_2",
        ],
        "answer": "SELECT * FROM students_course_1 UNION SELECT * FROM students_course_2",
        "explanation": (
            "Esta pregunta evalúa el conocimiento de operaciones de conjuntos (Set Operations) en SQL estándar.\n"
            "• UNION: Combina los resultados de dos consultas y elimina automáticamente los duplicados. Es exactamente lo que pide la pregunta (\"without duplicate records\").\n"
            "• UNION ALL: (No está en las opciones, pero es importante saberlo) Combina los resultados pero mantiene los duplicados.\n"
            "• INTERSECT: Devuelve solo las filas que aparecen en ambas tablas (la intersección), no \"todos\" los estudiantes.\n"
            "• INNER JOIN: Une tablas basándose en una columna común; no es una operación de conjunto para apilar resultados."
        ),
    },
    {
        "exam": 1,
        "id": "q44_optimize_zorder_compute_optimized",
        "question": (
            "El equipo de ingeniería está gestionando una tabla grande de Delta Lake con updates y deletes frecuentes. Observan que el rendimiento de las queries empeora con el tiempo "
            "debido a un aumento de small data files. Para solucionarlo, deciden ejecutar el comando OPTIMIZE con Z-Order indexing en columnas usadas frecuentemente en filtros.\n"
            "¿Qué tipo de optimización de recursos debería priorizar el equipo para que estos comandos se ejecuten de forma eficiente?"
        ),
        "options": ["GPU Optimized", "Compute Optimized", "Storage Optimized", "Memory Optimized"],
        "answer": "Compute Optimized",
        "explanation": (
            "El comando OPTIMIZE, y especialmente cuando se combina con ZORDER BY (intercalado multidimensional), es una operación intensiva en CPU.\n"
            "• Por qué Compute Optimized: Esta operación implica leer los archivos existentes, descodificar los datos, ordenar y barajar (shuffle) grandes volúmenes de registros para agrupar datos similares (Z-Ordering), y finalmente codificar y comprimir los nuevos archivos Parquet. Todo este proceso de ordenamiento y compresión requiere mucha potencia de procesamiento.\n"
            "• Comparación:\n"
            "o Memory Optimized: Útil para joins masivos o cachés grandes, pero el cuello de botella en ZORDER suele ser la velocidad de cálculo para ordenar y comprimir.\n"
            "o Storage Optimized: Útil para cargas de trabajo con I/O masivo o bases de datos NoSQL, pero Delta Lake en Databricks ya desacopla el almacenamiento (S3/ADLS), por lo que optimizar el disco local de la VM es menos crítico que la CPU para esta tarea específica.\n"
            "o GPU Optimized: Se usa para Deep Learning, no para operaciones estándar de mantenimiento de tablas."
        ),
    },
    {
        "exam": 1,
        "id": "q45_delta_sharing_databricks_to_databricks",
        "question": (
            "Una empresa de retail almacena datos de ventas en tablas Delta dentro de Databricks Unity Catalog. Necesitan compartir de forma segura tablas específicas con una empresa externa de auditoría "
            "que usa Databricks en un proveedor cloud diferente.\n"
            "¿Cuál de las siguientes opciones permite lograr esta tarea sin replicación de datos?"
        ),
        "options": ["Shallow clone", "Databricks Connect", "Databricks-to-Databricks Delta Sharing",
                    "External schema in Unity Catalog"],
        "answer": "Databricks-to-Databricks Delta Sharing",
        "explanation": (
            "El escenario plantea tres retos clave: compartir datos externamente, entre diferentes nubes (cross-cloud) y sin duplicar/replicar los datos.\n"
            "• Delta Sharing: Es el protocolo abierto de Databricks diseñado específicamente para esto. Permite compartir datos en vivo sin necesidad de copiarlos físicamente al entorno del receptor (Zero-Copy).\n"
            "• Databricks-to-Databricks: Como la empresa auditora también usa Databricks (aunque sea en otra nube), la integración es nativa a través de Unity Catalog. El receptor \"monta\" los datos compartidos como un catálogo de solo lectura y puede consultarlos directamente. Los datos permanecen en el almacenamiento de origen y solo viajan cuando se ejecuta una consulta (egress), pero no se almacenan/replican persistentemente en el destino."
        ),
    },
# ---------------- EXAM 2 ----------------

    {
        "exam": 2,
        "id": "q01_git_save_local_changes_remote_repo",
        "question": (
            "¿Cuál de las siguientes operaciones puede usar un data engineer para guardar los cambios locales de una carpeta Git en su repositorio remoto?"
        ),
        "options": ["Merge & Pull", "Merge & Push", "Commit & Pull", "Commit & Push"],
        "answer": "Commit & Push",
        "explanation": (
            "Commit & Push se utiliza para guardar los cambios en el repositorio local y después subir ese contenido al repositorio remoto.\n"
            "• commit: registra los cambios en el repositorio local.\n"
            "• push: envía esos commits al remote repository."
        ),
    },
    {
        "exam": 2,
        "id": "q02_sql_rotate_rows_into_columns",
        "question": (
            "¿Cuál de las siguientes palabras clave SQL puede usarse para rotar filas de una tabla convirtiendo valores de filas en múltiples columnas?"
        ),
        "options": ["PIVOT", "ZORDER BY", "ROTATE", "TRANSFORM"],
        "answer": "PIVOT",
        "explanation": (
            "PIVOT transforma las filas de una tabla rotando los valores únicos de una columna específica en columnas separadas.\n"
            "En otras palabras, convierte una tabla de formato long a formato wide."
        ),
    },
    {
        "exam": 2,
        "id": "q03_git_folder_functionality",
        "question": (
            "¿Cuál de las siguientes funcionalidades puede realizarse en Git folders?"
        ),
        "options": [
            "Crear pull requests",
            "Eliminar branches",
            "Hacer pull de cambios desde un repositorio Git remoto",
            "Crear nuevos repositorios Git remotos"
        ],
        "answer": "Hacer pull de cambios desde un repositorio Git remoto",
        "explanation": (
            "Los Git folders soportan la operación git pull.\n"
            "Se utiliza para hacer fetch y descargar contenido desde un remote repository y actualizar inmediatamente el repositorio local para que coincida con ese contenido."
        ),
    },
    {
        "exam": 2,
        "id": "q04_databricks_data_warehousing_service",
        "question": (
            "¿Cuál de los siguientes servicios proporciona una solución de data warehousing en Databricks Intelligence Platform?"
        ),
        "options": [
            "Delta Lives Tables (DLT)",
            "Unity Catalog",
            "Databricks SQL",
            "SQL warehouse"
        ],
        "answer": "Databricks SQL",
        "explanation": (
            "Databricks SQL (DB SQL) es el data warehouse dentro de la Databricks Lakehouse Platform que permite ejecutar consultas SQL y aplicaciones de BI a escala."
        ),
    },
    {
        "exam": 2,
        "id": "q05_create_table_from_csv_location",
        "question": (
            "Rellena el siguiente espacio en blanco para crear correctamente una tabla usando datos de archivos CSV ubicados en /path/input\n\n"
            "CREATE TABLE my_table\n"
            "(col1 STRING, col2 STRING)\n"
            "__________\n"
            "OPTIONS (header = \"true\", delimiter = \";\")\n"
            "LOCATION = \"/path/input\""
        ),
        "options": ["FROM CSV", "USING DELTA", "AS CSV", "USING CSV"],
        "answer": "USING CSV",
        "explanation": (
            "CREATE TABLE USING permite especificar un external data source como formato CSV junto con opciones adicionales.\n"
            "Esto crea una external table que apunta a archivos almacenados en una ubicación externa."
        ),
    },
    {
        "exam": 2,
        "id": "q06_autoloader_tracking_ingestion_progress",
        "question": (
            "¿Cuál de las siguientes técnicas permite a Auto Loader rastrear el progreso de ingesta y almacenar metadatos de los archivos descubiertos?"
        ),
        "options": ["Photon engine", "COPY INTO", "Watermarking", "Checkpointing"],
        "answer": "Checkpointing",
        "explanation": (
            "Auto Loader realiza el seguimiento de los archivos descubiertos usando checkpointing en la checkpoint location.\n"
            "El checkpointing permite a Auto Loader proporcionar garantías de ingesta exactly-once."
        ),
    },
    {
        "exam": 2,
        "id": "q07_describe_schema_location",
        "question": (
            "Un data engineer tiene un schema de custom-location llamado db_hr, y quiere saber dónde fue creado este schema en el almacenamiento subyacente.\n\n"
            "¿Cuál de los siguientes comandos puede usar el data engineer para completar esta tarea?"
        ),
        "options": [
            "DESCRIBE DATABASE db_hr",
            "DESCRIBE EXTENDED db_hr",
            "DESCRIBE db_hr",
            "SELECT location FROM db_hr.db"
        ],
        "answer": "DESCRIBE DATABASE db_hr",
        "explanation": (
            "DESCRIBE DATABASE (o DESCRIBE SCHEMA) devuelve la metadata de un schema existente, incluyendo su location en el filesystem.\n"
            "Si se usa la opción EXTENDED también se devuelven propiedades adicionales de la base de datos."
        ),
    },
    {
        "exam": 2,
        "id": "q08_unity_catalog_least_privilege_access",
        "question": (
            "Un data scientist del departamento de marketing requiere acceso de solo lectura a la tabla ‘customer_insights’ ubicada en el schema analytics, que forma parte del catálogo BI. "
            "Los datos se usarán para generar informes trimestrales de customer engagement. De acuerdo con el principio de least privilege, solo deben concederse los permisos mínimos necesarios para realizar las tareas requeridas.\n\n"
            "¿Qué comandos SQL otorgarán correctamente el acceso con los mínimos privilegios?"
        ),
        "options": [
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;"
        ],
        "answer": "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;",
        "explanation": (
            "Para acceder a una tabla concreta en Unity Catalog, el usuario necesita permisos SELECT sobre la tabla, USE SCHEMA sobre el schema contenedor y USE CATALOG sobre el catálogo padre.\n"
            "Esto proporciona el acceso mínimo necesario para operaciones de lectura sin sobreasignar privilegios."
        ),
    },
    {
        "exam": 2,
        "id": "q09_silver_layer_incorrect_statement",
        "question": (
            "Un equipo de data engineering está usando la Silver Layer en la Medallion Architecture para hacer joins de datos de clientes con external lookup tables y aplicar filtros.\n\n"
            "Un miembro del equipo hace las siguientes afirmaciones sobre la Silver Layer. ¿Cuál de estas afirmaciones es incorrecta?"
        ),
        "options": [
            "La Silver Layer gestiona la deduplicación de datos",
            "La Silver Layer se integra con otras fuentes para enriquecimiento de datos",
            "La Silver Layer almacena raw data enriquecido con detalles del source file y timestamps de ingesta",
            "La Silver Layer es responsable de la limpieza y el filtrado de datos"
        ],
        "answer": "La Silver Layer almacena raw data enriquecido con detalles del source file y timestamps de ingesta",
        "explanation": (
            "La Silver layer contiene datos refinados y limpios procedentes de la Bronze layer.\n"
            "Aquí se realizan operaciones de cleansing, filtering, deduplication y enrichment mediante joins con otras fuentes.\n"
            "El almacenamiento de raw data con metadata de ingesta pertenece a la Bronze layer, no a la Silver layer."
        ),
    },
    {
        "exam": 2,
        "id": "q10_drop_delta_table_managed",
        "question": (
            "Al hacer drop de una Delta table, ¿cuál de las siguientes opciones explica por qué se eliminarán tanto los metadatos de la tabla como los data files?"
        ),
        "options": [
            "Los data files son más antiguos que el periodo de retención por defecto",
            "La tabla es shallow cloned",
            "La tabla es managed",
            "La tabla es external"
        ],
        "answer": "La tabla es managed",
        "explanation": (
            "En una managed table, tanto la metadata como los data files están gestionados por Databricks.\n"
            "Al ejecutar DROP TABLE sobre una managed table se eliminan tanto la metadata como los archivos físicos.\n"
            "En una external table solo se elimina la metadata, no los datos."
        ),
    },
    {
        "exam": 2,
        "id": "q11_streaming_read_table",
        "question": (
            "¿Cuál de los siguientes bloques de código puede usar un data engineer para consultar la tabla events como una fuente streaming?"
        ),
        "options": [
            'spark.readStream.table("events")',
            'spark.read.table("events")',
            'spark.readStream("events")',
            'spark.readStream().table("events")'
        ],
        "answer": 'spark.readStream.table("events")',
        "explanation": (
            "Delta Lake está profundamente integrado con Spark Structured Streaming.\n"
            "Para leer una tabla como streaming source se utiliza spark.readStream.table(<table_name>).\n"
            "spark.read.table realiza lectura batch, no streaming."
        ),
    },
    {
        "exam": 2,
        "id": "q12_serverless_scaling_batch_jobs",
        "question": (
            "Una empresa de e-commerce experimenta un rápido crecimiento de datos debido a picos estacionales de tráfico. Su equipo de ingeniería necesita asegurar que los jobs de batch processing se completen dentro de un timeframe fijo, incluso durante las horas pico. "
            "El equipo tiene recursos humanos limitados para la gestión de infraestructura y busca una solución con escalado y optimización automatizados.\n\n"
            "¿Qué opción cumple mejor estas condiciones?"
        ),
        "options": [
            "Databricks Serverless compute",
            "All-purpose clusters con autoscaling habilitado",
            "Dedicated clusters con Photon habilitado",
            "Job clusters con asignación máxima de recursos"
        ],
        "answer": "Databricks Serverless compute",
        "explanation": (
            "Databricks Serverless compute ajusta automáticamente los recursos a workloads variables.\n"
            "Evita el overprovisioning en horas valle y sigue cumpliendo SLAs en picos de demanda.\n"
            "Además elimina la gestión de infraestructura permitiendo a los engineers centrarse en la lógica de datos."
        ),
    },
    {
        "exam": 2,
        "id": "q13_dlt_continuous_development_mode",
        "question": (
            "El equipo de data engineer tiene un pipeline de DLT que actualiza todas las tablas a intervalos definidos hasta que se detiene manualmente. "
            "Los recursos de cómputo del pipeline continúan ejecutándose para permitir quick testing.\n\n"
            "¿Cuál de las siguientes opciones describe mejor los execution modes de este pipeline DLT?"
        ),
        "options": [
            "El pipeline DLT se ejecuta en Triggered Pipeline mode bajo Production mode.",
            "El pipeline DLT se ejecuta en Continuous Pipeline mode bajo Development mode.",
            "El pipeline DLT se ejecuta en Continuous Pipeline mode bajo Production mode.",
            "El pipeline DLT se ejecuta en Triggered Pipeline mode bajo Development mode."
        ],
        "answer": "El pipeline DLT se ejecuta en Continuous Pipeline mode bajo Development mode.",
        "explanation": (
            "Los pipelines Continuous actualizan las tablas continuamente hasta que se detienen manualmente.\n"
            "En Development mode el cluster permanece activo para facilitar pruebas rápidas y evitar reinicios constantes."
        ),
    },
    {
        "exam": 2,
        "id": "q14_schedule_query_refresh",
        "question": (
            "¿Desde cuál de las siguientes ubicaciones puede un data engineer definir un schedule para refrescar automáticamente una query SQL en Databricks?"
        ),
        "options": [
            "No hay forma de refrescar automáticamente una query en Databricks. Los schedules solo pueden definirse para dashboards y alerts para refrescar sus queries subyacentes.",
            "Desde el SQL Editor en Databricks SQL",
            "Desde la UI de Jobs",
            "Desde la página de Alerts en Databricks SQL"
        ],
        "answer": "Desde el SQL Editor en Databricks SQL",
        "explanation": (
            "En Databricks SQL se puede programar la ejecución automática de una query directamente desde el SQL Editor."
        ),
    },
    {
        "exam": 2,
        "id": "q15_databricks_connect_purpose",
        "question": (
            "¿Cuál de las siguientes opciones describe mejor el propósito y la funcionalidad de Databricks Connect?"
        ),
        "options": [
            "Databricks Connect es una client library que permite a los engineers desarrollar Spark code localmente usando su IDE, mientras ejecutan ese código remotamente en un Databricks cluster.",
            "Databricks Connect es una herramienta de visualización de datos diseñada para crear y renderizar dashboards interactivos fuera del entorno Databricks.",
            "Databricks Connect es una solución de ingesta de datos que ofrece conectores simples y eficientes para ingerir datos desde varias fuentes de datos al lakehouse.",
            "Databricks Connect es un protocolo abierto desarrollado por Databricks para secure data sharing con otras organizaciones independientemente de las plataformas de cómputo que usen."
        ],
        "answer": "Databricks Connect es una client library que permite a los engineers desarrollar Spark code localmente usando su IDE, mientras ejecutan ese código remotamente en un Databricks cluster.",
        "explanation": (
            "Databricks Connect es una client library del Databricks Runtime que permite desarrollar Spark code localmente desde un IDE y ejecutarlo remotamente en un Databricks cluster.\n"
            "Facilita escribir, testear y debuggear código aprovechando la capacidad de cómputo del cluster remoto."
        ),
    },
    {
        "exam": 2,
        "id": "q16_change_table_owner_catalog_explorer",
        "question": (
            "¿En cuál de las siguientes ubicaciones puede un data engineer cambiar el owner de una tabla?"
        ),
        "options": [
            "En el Catalog Explorer, bajo la pestaña Permissions de la página de la tabla",
            "En el Catalog Explorer, desde el campo Owner en la página de la base de datos, ya que los owners se establecen a nivel de base de datos",
            "En el Catalog Explorer, bajo la pestaña Permissions de la página de la base de datos, ya que los owners se establecen a nivel de base de datos",
            "En el Catalog Explorer, desde el campo Owner en la página de la tabla"
        ],
        "answer": "En el Catalog Explorer, desde el campo Owner en la página de la tabla",
        "explanation": (
            "Desde el Catalog Explorer se puede navegar a la página de la tabla y cambiar su propietario directamente en el campo Owner.\n"
            "El ownership se gestiona a nivel de objeto (tabla), no únicamente a nivel de database."
        ),
    },
    {
        "exam": 2,
        "id": "q17_git_folders_vs_notebook_versioning",
        "question": (
            "Un junior data engineer usa el versionado integrado de Databricks Notebooks para source control. Un senior data engineer recomendó usar Git folders en su lugar.\n\n"
            "¿Cuál de las siguientes opciones podría explicar por qué se recomienda Git folders en lugar del versionado de Databricks Notebooks?"
        ),
        "options": [
            "Git folders almacenan archivos de código fuente en Unity Catalog para seguridad y gobernanza centralizadas",
            "Git folders sincronizan automáticamente todos los cambios de notebooks con el repositorio Git remoto en tiempo real",
            "Git folders soportan resolución automática de conflictos cuando varios usuarios editan el mismo notebook",
            "Git folders soportan crear y gestionar branches para trabajo de desarrollo."
        ],
        "answer": "Git folders soportan crear y gestionar branches para trabajo de desarrollo.",
        "explanation": (
            "Una ventaja clave de Git folders frente al versionado nativo de Notebooks es que permiten trabajar con branches.\n"
            "Esto facilita workflows de desarrollo como feature branches, pull requests y colaboración entre developers."
        ),
    },
    {
        "exam": 2,
        "id": "q18_notebook_interactive_debugger",
        "question": (
            "Un data scientist quiere probar una nueva función en Python que parsea y normaliza la entrada del usuario. En lugar de depender únicamente de print statements o logs, prefieren una forma más dinámica de seguir el flujo de datos y entender cómo cambian diferentes variables a medida que la función se ejecuta.\n\n"
            "¿Qué herramienta debería usar el data scientist para obtener estos insights de forma efectiva dentro de un notebook de Databricks?"
        ),
        "options": [
            "Audit logs",
            "Markdown cells",
            "Notebook Interactive Debugger",
            "Spark UI"
        ],
        "answer": "Notebook Interactive Debugger",
        "explanation": (
            "El Notebook Interactive Debugger permite inspeccionar variables de forma interactiva y paso a paso durante la ejecución.\n"
            "Es más potente que usar prints o logs para entender cómo se transforman los datos en cada etapa."
        ),
    },
    {
        "exam": 2,
        "id": "q19_liquid_clustering_benefit",
        "question": (
            "¿Cuál es un beneficio clave de Liquid Clustering para analytical workloads en Databricks?"
        ),
        "options": [
            "Reduce el volumen de datos escaneados durante la ejecución de la query",
            "Encripta automáticamente campos de datos sensibles durante la ingesta",
            "Previene la duplicación de datos, mejorando así el rendimiento de la query",
            "Asegura streaming en tiempo real desde el input datasource"
        ],
        "answer": "Reduce el volumen de datos escaneados durante la ejecución de la query",
        "explanation": (
            "Liquid Clustering optimiza el layout físico de las tablas Delta organizando los datos por clustering keys.\n"
            "Esto permite data skipping y reduce la cantidad de datos escaneados durante la ejecución de queries, mejorando el rendimiento."
        ),
    },
    {
        "exam": 2,
        "id": "q20_repair_failed_job_task",
        "question": (
            "Un Databricks job grande falla en la tarea 12 de 15 debido a que falta un configuration file. Tras resolver el problema, ¿cuál es la acción más apropiada para reanudar el workflow?"
        ),
        "options": [
            "Repair run desde la tarea 12",
            "Esperar a la siguiente ejecución programada",
            "Ejecutar manualmente el notebook asociado a la tarea 12",
            "Reiniciar el job"
        ],
        "answer": "Repair run desde la tarea 12",
        "explanation": (
            "Databricks permite reparar jobs fallidos ejecutando solo las tareas que fallaron y sus dependencias.\n"
            "Las tareas completadas correctamente no se vuelven a ejecutar, reduciendo tiempo y consumo de recursos."
        ),
    },
    {
        "exam": 2,
        "id": "q21_insert_into_delta_table",
        "question": (
            "¿Cuál de los siguientes comandos SQL añadirá (append) esta nueva fila a la tabla Delta users existente?\n\n"
            "user_id | name | age\n"
            "0015    | Adam | 23"
        ),
        "options": [
            'INSERT VALUES ("0015", "Adam", 23) INTO users',
            'APPEND VALUES ("0015", "Adam", 23) INTO users',
            'INSERT INTO users VALUES ("0015", "Adam", 23)',
            'APPEND INTO users VALUES ("0015", "Adam", 23)'
        ],
        "answer": 'INSERT INTO users VALUES ("0015", "Adam", 23)',
        "explanation": (
            "INSERT INTO permite añadir nuevas filas a una tabla Delta existente especificando los valores directamente.\n"
            "No existe la sentencia APPEND en SQL estándar de Databricks."
        ),
    },
    {
        "exam": 2,
        "id": "q22_run_sql_in_python_notebook",
        "question": (
            "Si el lenguaje por defecto del notebook es Python, ¿cuál de las siguientes opciones puede usar un data engineer para ejecutar comandos SQL en este Python Notebook?"
        ),
        "options": [
            "Pueden añadir %sql al inicio de una celda.",
            "¡Esto no es posible! Necesitan cambiar el lenguaje por defecto del notebook a SQL",
            "Pueden añadir el magic command %language al inicio de una celda para forzar la detección de lenguaje.",
            "Primero necesitan importar la librería SQL en una celda"
        ],
        "answer": "Pueden añadir %sql al inicio de una celda.",
        "explanation": (
            "En un notebook de Databricks se puede sobrescribir el lenguaje por celda usando magic commands.\n"
            "Añadiendo %sql al inicio de la celda se pueden ejecutar sentencias SQL dentro de un notebook Python."
        ),
    },
    {
        "exam": 2,
        "id": "q23_job_failure_email_notifications",
        "question": (
            "Un equipo de data engineering tiene un Job multi-tasks en producción. Los miembros del equipo necesitan ser notificados en caso de fallo del job.\n\n"
            "¿Cuál de los siguientes enfoques puede usarse para enviar emails a los miembros del equipo en caso de fallo del job?"
        ),
        "options": [
            "Solo el owner del Job puede configurarse para ser notificado en caso de fallo del job",
            "Pueden configurar los ajustes de notificaciones por email en la página del job",
            "No hay forma de notificar a los usuarios en caso de fallo del job",
            "Pueden usar la Job API para enviar emails programáticamente según el estado de cada task"
        ],
        "answer": "Pueden configurar los ajustes de notificaciones por email en la página del job",
        "explanation": (
            "Databricks Jobs permite configurar notificaciones por email para eventos como start, success o failure desde la página del job.\n"
            "Se pueden añadir múltiples direcciones en la sección de notificaciones."
        ),
    },
    {
        "exam": 2,
        "id": "q24_autoloader_schema_evolution_rescue",
        "question": (
            "Un data engineer está diseñando un pipeline de ingestión streaming usando Auto Loader. El requisito es que el pipeline nunca falle ante cambios de schema, pero debe capturar cualquier nueva columna que llegue en los datos para su inspección posterior.\n\n"
            "¿Cuál de los siguientes modos de schema evolution debería usar el engineer para cumplir este requisito?"
        ),
        "options": [
            "addNewColumns",
            "none",
            "failOnNewColumns",
            "rescue"
        ],
        "answer": "rescue",
        "explanation": (
            "Auto Loader soporta varios modos de schema evolution:\n"
            "• addNewColumns (default): añade nuevas columnas al schema pero el stream falla al detectarlas hasta actualizar la tabla.\n"
            "• failOnNewColumns: el stream falla cuando aparecen columnas nuevas.\n"
            "• none: ignora las nuevas columnas y no las captura.\n"
            "• rescue: el stream nunca falla y las nuevas columnas se almacenan en la rescued data column.\n\n"
            "Como el requisito es no fallar nunca y además capturar las columnas nuevas para inspección posterior, el único modo que cumple ambas condiciones es rescue."
        ),
    },
    {
        "exam": 2,
        "id": "q25_delta_sharing_tables_and_notebooks",
        "question": (
            "Un data engineer de una empresa global de logística necesita compartir datasets específicos y analysis notebooks con un proveedor externo de analítica, que es cliente de Databricks. "
            "Los datos están almacenados como Delta tables en Unity Catalog, y el proveedor no tiene acceso a la cuenta de Databricks de la empresa.\n\n"
            "¿Cuál es la forma más efectiva y segura de compartir los datos y notebooks con el proveedor externo?"
        ),
        "options": [
            "Compartir las Delta tables usando Delta Sharing, y enviar todos los notebooks juntos en un único fichero DBC",
            "Compartir las Delta tables usando Delta Sharing, y publicar los notebooks como páginas HTML de forma programática",
            "Compartir las Delta tables usando Delta Sharing, y conceder acceso a cada notebook mediante su funcionalidad de colaboración integrada",
            "Compartir las Delta tables y los notebooks usando Delta Sharing"
        ],
        "answer": "Compartir las Delta tables y los notebooks usando Delta Sharing",
        "explanation": (
            "Databricks-to-Databricks Delta Sharing permite compartir de forma segura tablas, notebooks, volumes y modelos ML con otros clientes de Databricks.\n"
            "No requiere acceso al mismo workspace y mantiene governance y control de acceso a través de Unity Catalog.\n"
            "Es la opción más segura y escalable para compartir datos y artefactos analíticos externamente."
        ),
    },
    {
        "exam": 2,
        "id": "q26_revoke_table_permissions",
        "question": (
            "¿Qué parte de la plataforma Databricks puede usar un data engineer para revocar permisos de usuarios sobre tablas?"
        ),
        "options": [
            "Catalog Explorer",
            "Workspace Admin Console",
            "Account Console",
            "No hay forma de revocar permisos en la plataforma Databricks. El data engineer necesita clonar la tabla con los permisos actualizados"
        ],
        "answer": "Catalog Explorer",
        "explanation": (
            "En Catalog Explorer (Data Explorer) se gestionan los permisos de objetos en Unity Catalog.\n"
            "Desde ahí se pueden conceder y revocar privilegios sobre tablas, schemas y catálogos."
        ),
    },
    {
        "exam": 2,
        "id": "q27_task_orchestration_service",
        "question": (
            "¿Cuál de los siguientes servicios puede usar un data engineer para task orchestration en la plataforma Databricks?"
        ),
        "options": [
            "Delta Live Tables",
            "Unity Catalog Lineage",
            "Databricks Jobs",
            "Databricks Connect"
        ],
        "answer": "Databricks Jobs",
        "explanation": (
            "Databricks Jobs permite orquestar tareas de procesamiento de datos definiendo dependencias entre tareas en forma de DAG.\n"
            "Facilita ejecutar, programar y gestionar workflows completos dentro de la plataforma."
        ),
    },
    {
        "exam": 2,
        "id": "q28_streaming_default_trigger_interval",
        "question": (
            "Dada la siguiente query de Structured Streaming:\n\n"
            "(spark.readStream\n"
            "    .table(\"orders\")\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .table(\"Output_Table\")\n"
            ")\n\n"
            "¿Cuál de las siguientes opciones es el trigger Interval de esta query?"
        ),
        "options": [
            "Cada medio segundo",
            "La query se ejecutará en batch mode para procesar todos los datos disponibles de una vez, y después el trigger se detiene.",
            "Cada medio minuto",
            "Cada media hora"
        ],
        "answer": "Cada medio segundo",
        "explanation": (
            "Si no se especifica un trigger en Structured Streaming, Spark usa el valor por defecto.\n"
            "El processingTime por defecto es 500 ms, equivalente a ejecutar un micro-batch cada medio segundo."
        ),
    },
    {
        "exam": 2,
        "id": "q29_storage_optimized_for_ad_hoc_queries",
        "question": (
            "Un analista ejecuta queries ad hoc frecuentes sobre un dataset grande de Delta Lake usando Databricks. Sin embargo, el analista está experimentando un rendimiento lento en las queries. "
            "Necesitan conseguir respuestas rápidas e interactivas para exploratory analysis basándose en datos cacheados.\n\n"
            "¿Qué tipo de instancia es la más adecuada para este workload?"
        ),
        "options": [
            "Compute Optimized",
            "Memory Optimized",
            "Storage Optimized",
            "General Purpose"
        ],
        "answer": "Storage Optimized",
        "explanation": (
            "Las instancias Storage Optimized aprovechan Delta caching y alto throughput de disco.\n"
            "Son ideales para queries ad hoc e interactivas sobre grandes datasets, reduciendo la latencia de lectura."
        ),
    },
    {
        "exam": 2,
        "id": "q30_dlt_streaming_table_readstream",
        "question": (
            "Un data engineer en una empresa global de e-commerce tiene la tarea de construir un pipeline de analítica near real-time usando Delta Live Tables (DLT). "
            "El objetivo es procesar continuamente clickstream data que registra interacciones de usuarios a través de múltiples sitios web regionales.\n\n"
            "Los raw event data se ingieren desde edge services en dos ubicaciones principales:\n"
            "1- Una Delta table registrada en Unity Catalog, ubicada en: ecommerce.analytics.raw_clickstream\n"
            "2- Un bucket S3, donde los datos entrantes llegan como archivos Parquet, en la siguiente ruta: s3://ecommerce/analytics/clickstream/\n\n"
            "Para soportar casos de uso near real-time como personalized product recommendations, fraud detection, y live performance dashboards, el data engineer necesita definir una streaming table dentro de un pipeline DLT. "
            "Esta tabla debe ingerir continuamente nuevos registros a medida que llegan a la data source.\n\n"
            "El engineer ha redactado los siguientes bloques de código candidatos usando el decorador @dlt.table y necesita identificar cuáles definen correctamente una streaming DLT table.\n\n"
            "¿Cuál de los siguientes bloques de código crea correctamente una streaming table llamada clickstream_events?"
        ),
        "options": [
            '@dlt.table(name = "clickstream_events")\ndef load_clickstream():\n    return spark.read.table("ecommerce.analytics.raw_clickstream")',
            '@dlt.table(name = "clickstream_events")\ndef load_clickstream():\n    return spark.read.format("parquet").load("s3://ecommerce/analytics/clickstream/")',
            '@dlt.table(name = "clickstream_events")\ndef load_clickstream():\n    return (spark.read\n        .format("cloudFiles")\n        .option("cloudFiles.format", "parquet")\n        .load("s3://ecommerce/analytics/clickstream/")\n    )',
            '@dlt.table(name = "clickstream_events")\ndef load_clickstream():\n    return spark.readStream.table("ecommerce.analytics.raw_clickstream")'
        ],
        "answer": '@dlt.table(name = "clickstream_events")\ndef load_clickstream():\n    return spark.readStream.table("ecommerce.analytics.raw_clickstream")',
        "explanation": (
            "Para definir una streaming table en DLT se debe usar Structured Streaming.\n"
            "spark.readStream.table(...) permite leer continuamente nuevos datos de una Delta table.\n"
            "spark.read es batch y no crea una tabla streaming, por lo que no cumple el requisito near real-time."
        ),
    },
    {
        "exam": 2,
        "id": "q31_purge_stale_delta_files",
        "question": (
            "¿Cuál de los siguientes comandos puede usar un data engineer para purgar stale data files de una Delta table?"
        ),
        "options": [
            "OPTIMIZE",
            "DELETE",
            "CLEAN",
            "VACUUM"
        ],
        "answer": "VACUUM",
        "explanation": (
            "VACUUM elimina los data files obsoletos que ya no son referenciados por la tabla Delta después del retention period.\n"
            "Se utiliza para limpiar almacenamiento tras operaciones como DELETE, MERGE o UPDATE."
        ),
    },
    {
        "exam": 2,
        "id": "q32_delta_transaction_log_format",
        "question": (
            "En tablas Delta Lake, ¿cuál de los siguientes es el formato principal de los archivos del transaction log?"
        ),
        "options": [
            "Delta",
            "JSON",
            "XML",
            "Parquet"
        ],
        "answer": "JSON",
        "explanation": (
            "El transaction log de Delta Lake se almacena en archivos JSON dentro de la carpeta _delta_log.\n"
            "Los datos de la tabla se guardan en formato Parquet, pero el log de transacciones es JSON."
        ),
    },
    {
        "exam": 2,
        "id": "q33_delta_lake_description",
        "question": (
            "\"Una de las tecnologías fundacionales proporcionadas por Databricks Intelligence Platform es un formato de almacenamiento open-source, basado en ficheros, que aporta fiabilidad a los data lakes\"\n\n"
            "¿Cuál de las siguientes tecnologías se describe en la afirmación anterior?"
        ),
        "options": [
            "Apache Spark",
            "Delta Lives Tables (DLT)",
            "Delta Lake",
            "Unity Catalog"
        ],
        "answer": "Delta Lake",
        "explanation": (
            "Delta Lake es un formato open source que añade un transaction log a archivos Parquet para soportar transacciones ACID.\n"
            "Esto aporta fiabilidad y consistencia a los data lakes."
        ),
    },
    {
        "exam": 2,
        "id": "q34_dlt_expectation_drop_row",
        "question": (
            "Un data engineer ha definido la siguiente data quality constraint en un pipeline de Delta Live Tables:\n\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ______\n\n"
            "Rellena el espacio en blanco para que los registros que violen esta constraint se descarten (dropped) y se reporten en métricas"
        ),
        "options": [
            "ON VIOLATION DROP ROW",
            "ON VIOLATION DELETE ROW",
            "No es necesario añadir la cláusula ON VIOLATION. Por defecto, los registros que violen la constraint serán descartados y reportados como inválidos en el event log",
            "ON VIOLATION FAIL UPDATE"
        ],
        "answer": "ON VIOLATION DROP ROW",
        "explanation": (
            "En Delta Live Tables, ON VIOLATION DROP ROW elimina los registros que no cumplen la expectation.\n"
            "Las violaciones se registran en métricas sin detener el pipeline."
        ),
    },
    {
        "exam": 2,
        "id": "q35_databricks_asset_bundle_targets",
        "question": (
            "¿Cuál es el propósito principal de la sección targets en el fichero databricks.yml de un Databricks Asset Bundle?"
        ),
        "options": [
            "Listar las librerías externas requeridas por el bundle",
            "Especificar diferentes deployment environments con sus respectivas configuraciones",
            "Definir user roles y políticas de acceso para el workspace",
            "Configurar la versión de Databricks Runtime"
        ],
        "answer": "Especificar diferentes deployment environments con sus respectivas configuraciones",
        "explanation": (
            "La sección targets en databricks.yml se utiliza para definir múltiples entornos de despliegue (por ejemplo, dev, staging y prod).\n"
            "Cada target puede tener configuraciones específicas como paths del workspace, settings de clusters y variables por entorno.\n"
            "Esto permite aislamiento entre entornos y flexibilidad en el deployment."
        ),
    },
    {
        "exam": 2,
        "id": "q36_gold_layer_description",
        "question": (
            "En la Medallion Architecture, ¿cuál de las siguientes afirmaciones describe mejor las tablas de la Gold layer?"
        ),
        "options": [
            "Proporcionan agregaciones a nivel de negocio que impulsan analytics, machine learning y production applications",
            "La estructura de la tabla en esta capa se parece a la estructura de la tabla del sistema origen con columnas de metadata adicionales como el load time y el input file name.",
            "Representan una versión filtrada, limpia y enriquecida de los datos",
            "Mantienen raw data ingerido desde varias fuentes"
        ],
        "answer": "Proporcionan agregaciones a nivel de negocio que impulsan analytics, machine learning y production applications",
        "explanation": (
            "La Gold layer contiene agregaciones a nivel de negocio preparadas para reporting, BI y machine learning.\n"
            "Es la capa final consumida por usuarios y aplicaciones."
        ),
    },
    {
        "exam": 2,
        "id": "q37_left_join_students_enrollments",
        "question": (
            "Dadas las siguientes 2 tablas:\n\n"
            "students\n"
            "student_id | name  | age\n"
            "U0001      | Adam  | 23\n"
            "U0002      | Sarah | 19\n"
            "U0003      | John  | 36\n\n"
            "enrollments\n"
            "course_id | student_id\n"
            "C0055     | U0001\n"
            "C0066     | U0001\n"
            "C0077     | U0002\n\n"
            "Rellena el espacio en blanco para que la siguiente query devuelva el resultado de abajo:\n\n"
            "SELECT students.name, students.age, enrollments.course_id\n"
            "FROM students\n"
            "_____ enrollments\n"
            "ON students.student_id = enrollments.student_id\n\n"
            "Query result:\n"
            "name  | age | course_id\n"
            "Adam  | 23  | C0055\n"
            "Adam  | 23  | C0066\n"
            "Sarah | 19  | C0077\n"
            "John  | 36  | NULL"
        ),
        "options": [
            "ANTI JOIN",
            "RIGHT JOIN",
            "LEFT JOIN",
            "INNER JOIN"
        ],
        "answer": "LEFT JOIN",
        "explanation": (
            "LEFT JOIN devuelve todas las filas de la tabla izquierda (students) y las coincidencias de la derecha (enrollments).\n"
            "Si no hay coincidencia, las columnas de la derecha quedan en NULL, como ocurre con John."
        ),
    },
    {
        "exam": 2,
        "id": "q38_transform_array_lambda",
        "question": (
            "Rellena el siguiente espacio en blanco para incrementar el número de cursos en 1 para cada estudiante en la columna array students:\n\n"
            "SELECT\n"
            "    faculty_id,\n"
            "    students,\n"
            "    __________ AS new_totals\n"
            "FROM faculties"
        ),
        "options": [
            "TRANSFORM (students, total_courses + 1)",
            "FILTER (students, i -> i.total_courses + 1)",
            "TRANSFORM (students, i -> i.total_courses + 1)",
            "FILTER (students, total_courses + 1)"
        ],
        "answer": "TRANSFORM (students, i -> i.total_courses + 1)",
        "explanation": (
            "TRANSFORM es una higher-order function que aplica una lambda a cada elemento de un array y devuelve un nuevo array.\n"
            "Permite modificar cada elemento individualmente, como incrementar total_courses en 1."
        ),
    },
    {
        "exam": 2,
        "id": "q39_temporary_view_session_scope",
        "question": (
            "Un data engineer quiere crear un objeto relacional obteniendo datos de dos tablas. El objeto relacional solo se usará en la sesión actual. "
            "Para ahorrar costes de almacenamiento, el data engineer quiere evitar copiar y almacenar datos físicos.\n\n"
            "¿Cuál de los siguientes objetos relacionales debería crear el data engineer?"
        ),
        "options": [
            "Managed table",
            "Global Temporary view",
            "View",
            "Temporary view"
        ],
        "answer": "Temporary view",
        "explanation": (
            "Una temporary view es una tabla virtual sin datos físicos almacenados.\n"
            "Solo existe durante la sesión de Spark y se elimina automáticamente al finalizarla."
        ),
    },
    {
        "exam": 2,
        "id": "q40_update_delta_discount",
        "question": (
            "El equipo de data engineering tiene una Delta table llamada products que contiene detalles de los productos, incluido el net price.\n\n"
            "¿Cuál de los siguientes bloques de código aplicará un descuento del 50% a todos los productos cuyo precio sea mayor que 1000 y guardará el nuevo precio en la tabla?"
        ),
        "options": [
            "SELECT price * 0.5 AS new_price FROM products WHERE price > 1000;",
            "UPDATE products SET price = price * 0.5 WHERE price > 1000;",
            "MERGE INTO products WHERE price > 1000 WHEN MATCHED UPDATE price = price * 0.5;",
            "UPDATE products SET price = price * 0.5 WHERE price >= 1000;"
        ],
        "answer": "UPDATE products SET price = price * 0.5 WHERE price > 1000;",
        "explanation": (
            "UPDATE modifica directamente los registros de la tabla Delta que cumplen la condición.\n"
            "Se aplica el descuento solo a los productos con precio estrictamente mayor a 1000."
        ),
    },
    {
        "exam": 2,
        "id": "q41_pyspark_agg_duplicate_keys",
        "question": (
            "Un data engineer en una empresa de HR analytics está desarrollando un pipeline PySpark para analizar métricas de salario por departamentos. "
            "Escribieron la siguiente línea de código para calcular el total, el promedio y el conteo de salarios por departamento:\n\n"
            "result_df = df.groupBy(\"department\").agg({\"salary\": \"sum\", \"salary\": \"avg\", \"salary\": \"count\"})\n\n"
            "Tras ejecutar el código, observaron que el DataFrame resultante solo contiene un valor agregado en lugar de las tres métricas esperadas.\n\n"
            "¿Cuál es la causa más probable de este problema?"
        ),
        "options": [
            "El método agg() debe usar una lista de tuplas en lugar de un diccionario al agregar múltiples funciones.",
            "Los diccionarios de Python no permiten claves duplicadas, por lo que solo se aplica la última agregación.",
            "El método groupBy() debe ir precedido por un método select() para las columnas usadas en la agregación.",
            "El método agg() solo soporta una función de agregación a la vez."
        ],
        "answer": "Los diccionarios de Python no permiten claves duplicadas, por lo que solo se aplica la última agregación.",
        "explanation": (
            "En Python, un diccionario no puede tener claves duplicadas.\n"
            "Al repetir la clave 'salary', cada entrada sobrescribe la anterior y solo se ejecuta la última agregación (count)."
        ),
    },
    {
        "exam": 2,
        "id": "q42_audit_log_json_format",
        "question": (
            "¿Cuál de las siguientes opciones representa un ejemplo correcto de un audit log en Databricks para un evento createMetastoreAssignment?"
        ),
        "options": [
            "[2021-08-24 13:46:24] INFO unityCatalog – createMetastoreAssignment – user@derar.cloud – workspace_id=30490590956351435170 metastore_id=abc123456-8398-4c25-91bb-b000b08739c7 default_catalog_name=main",
            "version: \"2.0\"\ntimestamp: 1629775584891\nserviceName: unityCatalog\nactionName: createMetastoreAssignment\nuserIdentity:\n  email: user@derar.cloud\nrequestParams:\n  workspace_id: \"30490590956351435170\"\n  metastore_id: abc123456-8398-4c25-91bb-b000b08739c7\n  default_catalog_name: main",
            "<log><version>2.0</version><timestamp>1629775584891</timestamp><serviceName>unityCatalog</serviceName><actionName>createMetastoreAssignment</actionName><userIdentity><email>user@derar.cloud</email></userIdentity><requestParams><workspace_id>30490590956351435170</workspace_id><metastore_id>abc123456-8398-4c25-91bb-b000b08739c7</metastore_id><default_catalog_name>main</default_catalog_name></requestParams></log>",
            "{ \"version\": \"2.0\", \"timestamp\": 1629775584891, \"serviceName\": \"unityCatalog\", \"actionName\": \"createMetastoreAssignment\", \"userIdentity\": { \"email\": \"user@derar.cloud\" }, \"requestParams\": { \"workspace_id\": \"30490590956351435170\", \"metastore_id\": \"abc123456-8398-4c25-91bb-b000b08739c7\", \"default_catalog_name\": \"main\" } }"
        ],
        "answer": "{ \"version\": \"2.0\", \"timestamp\": 1629775584891, \"serviceName\": \"unityCatalog\", \"actionName\": \"createMetastoreAssignment\", \"userIdentity\": { \"email\": \"user@derar.cloud\" }, \"requestParams\": { \"workspace_id\": \"30490590956351435170\", \"metastore_id\": \"abc123456-8398-4c25-91bb-b000b08739c7\", \"default_catalog_name\": \"main\" } }",
        "explanation": (
            "Los audit logs de Databricks se entregan en formato JSON.\n"
            "Los campos serviceName y actionName identifican el evento siguiendo la convención del Databricks REST API."
        ),
    },
    {
        "exam": 2,
        "id": "q43_streaming_trigger_available_now",
        "question": (
            "Dada la siguiente query de Structured Streaming:\n\n"
            "(spark.table(\"orders\")\n"
            "    .withColumn(\"total_after_tax\", col(\"total\")+col(\"tax\"))\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .outputMode(\"append\")\n"
            "    .__________\n"
            "    .table(\"new_orders\") )\n\n"
            "Rellena el espacio en blanco para que la query ejecute múltiples micro-batches para procesar todos los datos disponibles y luego detenga el trigger."
        ),
        "options": [
            'trigger(processingTime="0 seconds")',
            'trigger(micro-batches=True)',
            'trigger(availableNow=True)',
            'trigger(once=True)'
        ],
        "answer": 'trigger(availableNow=True)',
        "explanation": (
            "trigger(availableNow=True) procesa todos los datos disponibles en múltiples micro-batches y se detiene automáticamente.\n"
            "Se utiliza para ejecutar streaming en modo batch incremental."
        ),
    },
    {
        "exam": 2,
        "id": "q44_delta_sharing_identifier",
        "question": (
            "Durante la configuración de Delta Sharing con un partner externo, un data engineer pide al partner su sharing identifier.\n\n"
            "¿Cuál de las siguientes opciones describe mejor el sharing identifier en el contexto de Databricks-to-Databricks Sharing?"
        ),
        "options": [
            "Actúa como el authentication token para llamadas API con el endpoint del receptor",
            "Proporciona una referencia única para el Unity Catalog metastore del receptor",
            "Sirve como una public encryption key usada durante escrituras de datos en las tablas del partner",
            "Identifica la dirección IP de red del partner para firewall whitelisting"
        ],
        "answer": "Proporciona una referencia única para el Unity Catalog metastore del receptor",
        "explanation": (
            "El sharing identifier es un identificador único del metastore de Unity Catalog del receptor.\n"
            "Permite al proveedor conceder acceso a los datos compartidos en Databricks-to-Databricks Delta Sharing."
        ),
    },
    {
        "exam": 2,
        "id": "q45_liquid_clustering_multi_column_filters",
        "question": (
            "Un equipo de data engineering está trabajando con una tabla de user activity events almacenada en Unity Catalog. Las queries a menudo incluyen filtros sobre múltiples columnas como user_id y event_date.\n\n"
            "¿Qué técnica de data layout debería implementar el equipo para evitar table scans costosos?"
        ),
        "options": [
            "Usar partitioning en la columna event_date.",
            "Usar liquid clustering en la combinación de user_id y event_date",
            "Usar partitioning en la columna user_id, junto con Z-order indexing en la columna event_date.",
            "Usar Z-order indexing en user_id"
        ],
        "answer": "Usar liquid clustering en la combinación de user_id y event_date",
        "explanation": (
            "Liquid clustering optimiza progresivamente el layout físico basado en múltiples columnas de filtrado.\n"
            "Permite data skipping eficiente cuando se filtra por user_id y event_date evitando full table scans."
        ),
    },
    {
        "exam": 3,
        "id": "q01_cloudflare_r2_delta_sharing_egress",
        "question": (
            "Una organización planea usar Delta Sharing para habilitar acceso a datasets grandes por múltiples clientes a través de AWS, Azure y GCP. "
            "Un senior data engineer ha recomendado migrar el dataset a Cloudflare R2 object storage antes de iniciar el proceso de data sharing.\n\n"
            "¿Qué beneficio ofrece Cloudflare R2 en esta configuración de Delta Sharing?"
        ),
        "options": [
            "Elimina el coste de egress del proveedor cloud para transferencias de datos de salida",
            "Proporciona una API estándar para evitar cloud vendor lock-in",
            "Proporciona soporte nativo para dynamic data masking",
            "Ofrece soporte integrado para streaming data con checkpointing automático"
        ],
        "answer": "Elimina el coste de egress del proveedor cloud para transferencias de datos de salida",
        "explanation": (
            "Cloudflare R2 elimina los costes de egress, reduciendo significativamente el coste al compartir datos entre clouds mediante Delta Sharing."
        ),
    },
    {
        "exam": 3,
        "id": "q01_cloudflare_r2_delta_sharing_egress",
        "question": (
            "Una organización planea usar Delta Sharing para habilitar acceso a grandes datasets por múltiples clientes a través de AWS, Azure y GCP. "
            "Un senior data engineer ha recomendado migrar el dataset a Cloudflare R2 object storage antes de iniciar el proceso de data sharing.\n\n"
            "¿Qué beneficio ofrece Cloudflare R2 en esta configuración de Delta Sharing?"
        ),
        "options": [
            "Elimina el coste de egress del proveedor cloud para transferencias de datos de salida",
            "Proporciona una API estándar para evitar cloud vendor lock-in",
            "Proporciona soporte nativo para dynamic data masking",
            "Ofrece soporte integrado para streaming data con checkpointing automático"
        ],
        "answer": "Elimina el coste de egress del proveedor cloud para transferencias de datos de salida",
        "explanation": (
            "Cloudflare R2 elimina los costes de egress, reduciendo significativamente el coste al compartir datos entre clouds mediante Delta Sharing."
        ),
    },
    {
        "exam": 3,
        "id": "q02_automatic_liquid_clustering_when_to_use",
        "question": (
            "Un equipo de data engineering está discutiendo la estrategia óptima de data layout en una tabla Delta gestionada en Unity Catalog en crecimiento. "
            "Están considerando partitioning, Z-ordering y Liquid Clustering para mejorar el rendimiento de las consultas.\n\n"
            "¿Qué escenario indica mejor que Automatic Liquid Clustering es la opción recomendada?"
        ),
        "options": [
            "El equipo ha identificado clustering keys estables para la tabla.",
            "La tabla experimenta filtros de consulta diversos y que cambian con frecuencia en múltiples columnas, con patrones de acceso impredecibles.",
            "Ninguna de las opciones listadas es correcta. Automatic Liquid Clustering no puede aplicarse en tablas gestionadas.",
            "La tabla se filtra intensivamente por un conjunto pequeño y consistente de rangos de fechas."
        ],
        "answer": "La tabla experimenta filtros de consulta diversos y que cambian con frecuencia en múltiples columnas, con patrones de acceso impredecibles.",
        "explanation": (
            "Automatic Liquid Clustering se adapta dinámicamente a patrones de consulta cambiantes reorganizando los datos según los filtros recientes. "
            "Es especialmente útil cuando los predicados cambian frecuentemente entre múltiples columnas y no existe una clave de partición estable.\n\n"
            "Partitioning funciona mejor con filtros predecibles (por ejemplo fechas), y ZORDER con columnas conocidas de alta cardinalidad. "
            "Cuando el acceso es impredecible, Liquid Clustering evita tener que ajustar manualmente el layout."
        ),
    },
    {
        "exam": 3,
        "id": "q03_autoloader_incremental_technology",
        "question": (
            "¿Cuál de las siguientes tecnologías utiliza Auto Loader para cargar datos de forma incremental?"
        ),
        "options": [
            "Spark Structured Streaming",
            "Arquitectura multi-hop",
            "COPY INTO",
            "DEEP CLONE"
        ],
        "answer": "Spark Structured Streaming",
        "explanation": (
            "Auto Loader está basado en Spark Structured Streaming y expone una fuente de streaming llamada cloudFiles. "
            "Gracias a ello puede detectar automáticamente nuevos archivos en el storage e ingerirlos incrementalmente."
        ),
    },
    {
        "exam": 3,
        "id": "q04_autoloader_schema_evolution_mode",
        "question": (
            "Un data engineer está configurando un stream de Databricks Auto Loader para ingerir datos JSON desde un bucket de S3. "
            "El pipeline debe fallar cuando se detecten nuevas columnas en los datos entrantes, pero esas nuevas columnas deben seguir "
            "añadiéndose al esquema para que las ejecuciones posteriores puedan reanudarse correctamente con el esquema actualizado. "
            "Las columnas existentes deben conservar sus tipos de datos.\n\n"
            "spark.readStream \\\n"
            "    .format(\"cloudFiles\") \\\n"
            "    .option(\"cloudFiles.format\", \"json\") \\\n"
            "    .option(\"cloudFiles.schemaLocation\", \"s3://checkpoints/orders\") \\\n"
            "    .option(\"cloudFiles.schemaEvolutionMode\", \"__________\") \\\n"
            "    .load(\"s3://shop/raw/orders/json/\") \\\n"
            ".writeStream \\\n"
            "    .option(\"checkpointLocation\", \"s3://checkpoints/orders\") \\\n"
            "    .start(\"orders_table\")\n\n"
            "¿Qué opción completa correctamente el hueco para cumplir el requisito especificado?"
        ),
        "options": [
            "addNewColumns",
            "none",
            "rescue",
            "failOnNewColumns"
        ],
        "answer": "addNewColumns",
        "explanation": (
            "El modo addNewColumns es el comportamiento por defecto de Auto Loader. "
            "Cuando aparece una nueva columna el stream falla, pero la columna se añade al esquema permitiendo reiniciar "
            "y continuar el procesamiento sin cambiar tipos existentes."
        ),
    },
    {
        "exam": 3,
        "id": "q05_sql_warehouse_private_network",
        "question": (
            "Un data architect está diseñando una plataforma de datos híbrida que debe conectarse de forma segura a bases de datos on-premises. "
            "Estas bases de datos deben consultarse usando SQL desde Databricks, y la plataforma debe permanecer dentro "
            "de la red definida por la empresa por motivos de compliance.\n\n"
            "¿Qué tipo de SQL Warehouse debería elegir el arquitecto para soportar esta arquitectura?"
        ),
        "options": [
            "Pro SQL Warehouse",
            "Serverless compute for notebooks",
            "Classic SQL Warehouse",
            "Serverless SQL Warehouse"
        ],
        "answer": "Pro SQL Warehouse",
        "explanation": (
            "Pro SQL Warehouses permiten conectividad segura a redes privadas definidas por la empresa (VPC/VNet) "
            "y soportan arquitecturas híbridas con bases de datos on-premises. "
            "Las opciones serverless están totalmente gestionadas por Databricks y no operan dentro de redes privadas del cliente."
        ),
    },
    {
        "exam": 3,
        "id": "q06_jobs_task_dependency",
        "question": (
            "En Databricks Jobs, ¿cuál de los siguientes enfoques puede usar un data engineer para configurar una dependencia lineal "
            "entre Task A y Task B?"
        ),
        "options": [
            "Pueden asignar a Task A un número de orden 1 y a Task B un número de orden 2",
            "Pueden seleccionar Task A en el campo Depends On de la configuración de Task B",
            "Pueden arrastrar y soltar visualmente una flecha desde Task A a Task B en el canvas del Job",
            "Pueden configurar la dependencia a nivel de notebook usando la utilidad dbutils.jobs"
        ],
        "answer": "Pueden seleccionar Task A en el campo Depends On de la configuración de Task B",
        "explanation": (
            "El orden de ejecución de tareas en Databricks Jobs se define mediante el campo 'Depends On'. "
            "Task B depende de Task A si se selecciona Task A dentro de este campo, estableciendo así la dependencia."
        ),
    },
    {
        "exam": 3,
        "id": "q07_declarative_etl_framework",
        "question": (
            "Databricks proporciona un framework ETL declarativo para construir pipelines de procesamiento de datos fiables y mantenibles, "
            "manteniendo dependencias entre tablas y calidad de datos.\n\n"
            "¿Cuál de las siguientes tecnologías se describe arriba?"
        ),
        "options": [
            "Databricks Jobs",
            "Unity Catalog Lineage",
            "Delta Lake",
            "Delta Live Tables"
        ],
        "answer": "Delta Live Tables",
        "explanation": (
            "Delta Live Tables es un framework declarativo para construir pipelines de datos fiables y mantenibles. "
            "Gestiona automáticamente la orquestación de tareas, el clúster, el monitoreo, la calidad de datos y el manejo de errores."
        ),
    },
    {
        "exam": 3,
        "id": "q08_python_function_multiply",
        "question": (
            "¿Cuál de los siguientes bloques de código puede usar un data engineer para crear una función Python que multiplique dos enteros y devuelva el resultado?"
        ),
        "options": [
            "fun multiply_numbers(num1, num2):\n    return num1 * num2",
            "def multiply_numbers(num1, num2):\n    print(num1 * num2)",
            "def multiply_numbers(num1, num2):\n    return num1 * num2",
            "def fun: multiply_numbers(num1, num2):\n    return num1 * num2"
        ],
        "answer": "def multiply_numbers(num1, num2):\n    return num1 * num2",
        "explanation": (
            "En Python, una función se define usando la palabra clave 'def'. "
            "Para devolver el resultado debe utilizarse 'return' y no 'print'.\n\n"
            "Sintaxis:\n"
            "def function_name(params):\n"
            "    return params"
        ),
    },
    {
        "exam": 3,
        "id": "q09_dab_resources_key",
        "question": (
            "Un data engineer está preparando un Databricks asset bundle para definir un job que ejecuta un notebook ubicado en "
            "./src/my_notebook.py. Mientras escribe la configuración YAML, el engineer redacta lo siguiente:\n\n"
            "bundle:\n"
            "  name: my_bundle\n\n"
            "__________:\n"
            "  jobs:\n"
            "    my_job:\n"
            "      name: my_notebook_job\n"
            "      tasks:\n"
            "        - task_key: test_task\n"
            "          existing_cluster_id: 1234-911320-xyzwpm999\n"
            "          notebook_task:\n"
            "            notebook_path: './src/my_notebook.py'\n\n"
            "Para asegurar que el bundle sea válido y desplegable, ¿qué clave debería reemplazar correctamente el hueco anterior?"
        ),
        "options": [
            "settings",
            "workflows",
            "resources",
            "pipelines"
        ],
        "answer": "resources",
        "explanation": (
            "La sección 'resources' es donde se definen los objetos desplegables: jobs, pipelines, notebooks, clusters, etc. "
            "Es el núcleo de lo que se despliega cuando se aplica el bundle.\n\n"
            "Las otras opciones son incorrectas:\n"
            "- settings no es una sección válida en Databricks Asset Bundles\n"
            "- pipelines se usa para DLT pipelines\n"
            "- workflows no se usa en la estructura databricks.yml"
        ),
    },
    {
        "exam": 3,
        "id": "q10_use_schema_privilege",
        "question": (
            "Un data engineer usa la siguiente consulta SQL:\n\n"
            "GRANT USE SCHEMA ON SCHEMA sales_db TO finance_team\n\n"
            "¿Cuál de las siguientes opciones describe el beneficio del privilegio USE SCHEMA?"
        ),
        "options": [
            "Es un prerrequisito para poder realizar cualquier acción en la base de datos",
            "Otorga permisos completos sobre toda la base de datos",
            "Otorga acceso de lectura a la base de datos",
            "Otorga la capacidad de ver los objetos de la base de datos y su metadata"
        ],
        "answer": "Es un prerrequisito para poder realizar cualquier acción en la base de datos",
        "explanation": (
            "El privilegio USE SCHEMA (antes USAGE) no otorga permisos directos. "
            "Sirve como prerrequisito para poder ejecutar acciones sobre objetos dentro del schema. "
            "Además, es necesario otorgar USE CATALOG en el catálogo padre para permitir acceso en Unity Catalog."
        ),
    },
    {
        "exam": 3,
        "id": "q11_groupby_multiple_aggregations",
        "question": (
            "Un data engineer debe calcular tanto el salario total como el salario promedio de empleados, agrupados por su departamento. "
            "Usan groupBy de PySpark y quieren aplicar múltiples agregaciones a cada grupo.\n\n"
            "Completa el siguiente snippet de código:\n\n"
            "result_df = df.groupBy(\"department\").__________(\n"
            "    sum(\"salary\").alias(\"total_salary\"),\n"
            "    avg(\"salary\").alias(\"average_salary\")\n"
            ")\n\n"
            "¿Qué función debe usarse para completar el código?"
        ),
        "options": [
            "agg",
            "filter",
            "withColumn",
            "select"
        ],
        "answer": "agg",
        "explanation": (
            "La función agg() se utiliza después de groupBy() cuando se aplican una o más funciones de agregación a datos agrupados. "
            "En este caso se calcula suma y media, por lo que agg() es la correcta."
        ),
    },
    {
        "exam": 3,
        "id": "q12_grant_permissions_catalog_explorer",
        "question": (
            "¿Qué parte de la plataforma Databricks puede usar un data engineer para otorgar permisos sobre tablas a usuarios?"
        ),
        "options": [
            "Data Studio",
            "Workspace Admin Console",
            "Account console",
            "Catalog Explorer"
        ],
        "answer": "Catalog Explorer",
        "explanation": (
            "Catalog Explorer permite gestionar permisos sobre objetos de datos en Unity Catalog, "
            "incluyendo otorgar privilegios sobre tablas y bases de datos a usuarios o grupos."
        ),
    },
    {
        "exam": 3,
        "id": "q13_git_folders_not_supported_task",
        "question": (
            "¿Cuál de las siguientes tareas no está soportada por Git folders y debe realizarse en tu proveedor Git?"
        ),
        "options": [
            "Crear y hacer checkout de ramas para trabajo de desarrollo.",
            "Comparar visualmente diferencias al hacer commit.",
            "Eliminar ramas",
            "Clonar, hacer push o hacer pull desde un repositorio Git remoto."
        ],
        "answer": "Eliminar ramas",
        "explanation": (
            "Databricks Repos (Git folders) no soporta eliminar ramas ni crear pull requests; "
            "esas operaciones deben realizarse en el proveedor Git (GitHub, GitLab, Bitbucket...). "
            "Las demás operaciones básicas sí pueden hacerse desde Databricks."
        ),
    },
    {
        "exam": 3,
        "id": "q14_merge_into_avoid_duplicates",
        "question": (
            "Un junior data engineer suele usar el comando INSERT INTO para escribir datos en una tabla Delta. "
            "Un senior data engineer sugiere usar otro comando que evita escribir registros duplicados.\n\n"
            "¿Cuál de los siguientes comandos es el sugerido por el senior data engineer?"
        ),
        "options": [
            "UPDATE",
            "COPY INTO",
            "MERGE INTO",
            "INSERT OR OVERWRITE"
        ],
        "answer": "MERGE INTO",
        "explanation": (
            "MERGE INTO permite combinar inserciones, actualizaciones y borrados basados en una tabla origen hacia una tabla Delta destino. "
            "Con MERGE INTO puedes evitar insertar registros duplicados al escribir en tablas Delta."
        ),
    },
    {
        "exam": 3,
        "id": "q15_ctas_not_true_statement",
        "question": (
            "¿Cuál de las siguientes afirmaciones NO es cierta sobre sentencias CTAS?"
        ),
        "options": [
            "Las sentencias CTAS soportan declaración manual del schema",
            "CTAS significa CREATE TABLE AS SELECT",
            "Con CTAS, los datos se insertan durante la creación de la tabla",
            "CTAS infiere automáticamente el schema a partir de los resultados de la query"
        ],
        "answer": "Las sentencias CTAS soportan declaración manual del schema",
        "explanation": (
            "CREATE TABLE AS SELECT (CTAS) crea y rellena tablas Delta usando el resultado de una SELECT. "
            "CTAS infiere automáticamente el esquema desde los resultados de la query y no soporta la declaración manual del schema."
        ),
    },
    {
        "exam": 3,
        "id": "q16_dlt_triggered_development_mode",
        "question": (
            "El equipo de data engineer tiene un pipeline de DLT que actualiza todas las tablas una vez y luego se detiene. "
            "Los recursos de cómputo del pipeline continúan ejecutándose para permitir pruebas rápidas.\n\n"
            "¿Cuál de las siguientes opciones describe mejor los modos de ejecución de este pipeline de DLT?"
        ),
        "options": [
            "El pipeline de DLT se ejecuta en modo Triggered Pipeline bajo Production mode.",
            "El pipeline de DLT se ejecuta en modo Continuous Pipeline bajo Production mode.",
            "El pipeline de DLT se ejecuta en modo Continuous Pipeline bajo Development mode.",
            "El pipeline de DLT se ejecuta en modo Triggered Pipeline bajo Development mode."
        ],
        "answer": "El pipeline de DLT se ejecuta en modo Triggered Pipeline bajo Development mode.",
        "explanation": (
            "Triggered pipelines se ejecutan una vez y luego se detienen. "
            "En Development mode, el clúster permanece activo durante un periodo de tiempo para permitir testing e iteración rápida, "
            "y desactiva los reintentos automáticos para ayudar a detectar errores rápidamente."
        ),
    },
    {
        "exam": 3,
        "id": "q17_medallion_silver_to_gold_streaming",
        "question": (
            "Dada la siguiente query de Structured Streaming:\n\n"
            "(spark.readStream\n"
            "    .table(\"cleanedOrders\")\n"
            "    .groupBy(\"productCategory\")\n"
            "    .agg(sum(\"totalWithTax\"))\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .outputMode(\"complete\")\n"
            "    .table(\"aggregatedOrders\")\n"
            ")\n\n"
            "¿Cuál de las siguientes opciones describe mejor el propósito de esta query en una Medallion Architecture?"
        ),
        "options": [
            "La query está realizando un salto de una tabla Bronze a una tabla Silver",
            "La query está realizando un salto de la capa Silver a una tabla Gold",
            "La query está realizando ingesta de datos raw en una tabla Bronze",
            "La query está realizando transferencia de datos desde una tabla Gold hacia una aplicación de producción"
        ],
        "answer": "La query está realizando un salto de la capa Silver a una tabla Gold",
        "explanation": (
            "La query de Structured Streaming agrega datos limpios de la tabla silver 'cleanedOrders' "
            "y escribe resultados agregados a nivel de negocio en la tabla gold 'aggregatedOrders'. "
            "Esto representa una transformación Silver → Gold en la arquitectura Medallion."
        ),
    },
    {
        "exam": 3,
        "id": "q18_vacuum_retention_threshold",
        "question": (
            "Un data engineer observó que hay data files sin uso en el directorio de una tabla Delta. "
            "Ejecutaron el comando VACUUM sobre esta tabla; sin embargo, solo algunos de esos data files sin uso han sido eliminados.\n\n"
            "¿Cuál de las siguientes opciones podría explicar por qué solo algunos de los data files sin uso han sido eliminados tras ejecutar el comando VACUUM?"
        ),
        "options": [
            "Los data files eliminados eran más grandes que el umbral de tamaño por defecto. Mientras que los archivos restantes son más pequeños que el umbral de tamaño por defecto y no pueden eliminarse.",
            "Los data files eliminados eran más antiguos que el umbral de retención por defecto. Mientras que los archivos restantes son más recientes que el umbral de retención por defecto y no pueden eliminarse.",
            "Los data files eliminados eran más pequeños que el umbral de tamaño por defecto. Mientras que los archivos restantes son más grandes que el umbral de tamaño por defecto y no pueden eliminarse.",
            "Los data files eliminados eran más recientes que el umbral de retención por defecto. Mientras que los archivos restantes son más antiguos que el umbral de retención por defecto y no pueden eliminarse."
        ],
        "answer": "Los data files eliminados eran más antiguos que el umbral de retención por defecto. Mientras que los archivos restantes son más recientes que el umbral de retención por defecto y no pueden eliminarse.",
        "explanation": (
            "Ejecutar el comando VACUUM en una tabla Delta elimina los data files sin uso que sean más antiguos que un periodo de retención de datos especificado. "
            "Los archivos sin uso que sean más recientes que el umbral de retención por defecto se mantienen intactos."
        ),
    },
    {
        "exam": 3,
        "id": "q19_lakehouse_definition",
        "question": (
            "¿Cuál de las siguientes opciones describe mejor un Data Lakehouse?"
        ),
        "options": [
            "Sistema de gestión de datos fiable con garantías transaccionales para los datos estructurados de la organización.",
            "Sistema único, flexible y de alto rendimiento que soporta cargas de trabajo de data engineering, analítica y machine learning.",
            "Plataforma que escala workloads de data lake para organizaciones sin invertir en hardware on-premises.",
            "Plataforma que ayuda a reducir los costes de almacenar ficheros de datos en formatos abiertos de la organización en la nube."
        ],
        "answer": "Sistema único, flexible y de alto rendimiento que soporta cargas de trabajo de data engineering, analítica y machine learning.",
        "explanation": (
            "La Databricks Intelligence Platform es una plataforma unificada de analítica lakehouse que combina los mejores elementos de data lakes y data warehouses. "
            "En el lakehouse, puedes trabajar en data engineering, analytics y AI en una única plataforma."
        ),
    },
    {
        "exam": 3,
        "id": "q20_autoloader_vs_copyinto_when_use",
        "question": (
            "Un data engineer necesita determinar si usar Auto Loader o el comando COPY INTO para cargar ficheros de datos de entrada de forma incremental.\n\n"
            "¿En cuál de los siguientes escenarios debería el data engineer usar Auto Loader en lugar del comando COPY INTO?"
        ),
        "options": [
            "Si van a ingerir ficheros en el orden de millones o más a lo largo del tiempo",
            "Si van a cargar un subconjunto de ficheros re-subidos",
            "Si el schema de los datos no va a evolucionar con frecuencia",
            "Si van a ingerir un número pequeño de ficheros en el orden de miles"
        ],
        "answer": "Si van a ingerir ficheros en el orden de millones o más a lo largo del tiempo",
        "explanation": (
            "If you're going to ingest files in the order of thousands, you can use COPY INTO. "
            "If you are expecting files in the order of millions or more over time, use Auto Loader.\n"
            "If your data schema is going to evolve frequently, Auto Loader provides better primitives around schema inference and evolution."
        ),
    },
    {
        "exam": 3,
        "id": "q21_create_schema_usage",
        "question": "¿Cuál de las siguientes afirmaciones describe mejor el uso del comando CREATE SCHEMA?",
        "options": [
            "Se usa para crear una base de datos",
            "Se usa para inferir y almacenar el schema en \"cloudFiles.schemaLocation\"",
            "Se usa para crear el schema de una tabla (nombres de columnas y datatype)",
            "Se usa para hacer merge del schema al escribir datos en una tabla destino"
        ],
        "answer": "Se usa para crear una base de datos",
        "explanation": (
            "CREATE SCHEMA es un alias de la sentencia CREATE DATABASE. "
            "Aunque el uso de SCHEMA y DATABASE es intercambiable, se prefiere SCHEMA."
        ),
    },
    {
        "exam": 3,
        "id": "q22_global_temp_view",
        "question": (
            "Un data engineer quiere crear un objeto relacional a partir de datos de dos tablas. "
            "El objeto relacional debe ser usado por otros data engineers en otras sesiones únicamente en el mismo clúster. "
            "Para ahorrar en costes de almacenamiento, el data engineer quiere evitar copiar y almacenar datos físicos.\n\n"
            "¿Cuál de los siguientes objetos relacionales debería crear el data engineer?"
        ),
        "options": [
            "Global Temporary view",
            "Temporary view",
            "External table",
            "View"
        ],
        "answer": "Global Temporary view",
        "explanation": (
            "Para evitar copiar y almacenar datos físicos, el data engineer debe crear un objeto view. "
            "Una view en Databricks es una tabla virtual sin datos físicos, solo una SQL query guardada.\n"
            "Una Global Temporary view puede accederse entre sesiones dentro del mismo clúster y se almacena "
            "en la base de datos especial global_temp."
        ),
    },
    {
        "exam": 3,
        "id": "q23_optimize_idempotent",
        "question": (
            "Un data engineer tiene una tabla Delta grande sin particionar que estaba experimentando problemas de rendimiento. "
            "Ejecutan el comando OPTIMIZE, que compactó correctamente muchos archivos pequeños en archivos más grandes. "
            "Sin embargo, a pesar de esta compactación, las consultas posteriores sobre la tabla continúan siendo lentas. "
            "Para mejorar el rendimiento aún más, vuelven a ejecutar el comando OPTIMIZE pero observan que no hay cambios en los tamaños de los data files.\n\n"
            "¿Qué explica más probablemente el comportamiento durante la segunda ejecución?"
        ),
        "options": [
            "La tabla Delta no está particionada, por lo que OPTIMIZE no tiene efecto al ejecutarse dos veces",
            "Se debe ejecutar un comando VACUUM antes de volver a ejecutar el comando OPTIMIZE.",
            "El comando OPTIMIZE tiene un periodo de retención de 7 días entre ejecuciones consecutivas.",
            "El comando OPTIMIZE no modifica archivos ya compactados"
        ],
        "answer": "El comando OPTIMIZE no modifica archivos ya compactados",
        "explanation": (
            "OPTIMIZE es idempotente en Delta Lake. Una vez que los small files ya han sido compactados en archivos de tamaño óptimo, "
            "ejecutar OPTIMIZE de nuevo no los modificará a menos que aparezcan nuevos small files. "
            "Por lo tanto, la segunda ejecución no produce cambios en los tamaños de los archivos."
        ),
    },
    {
        "exam": 3,
        "id": "q24_sql_warehouse_cluster_size",
        "question": (
            "Un data engineer quiere aumentar el cluster size de un Databricks SQL warehouse existente.\n\n"
            "¿Cuál de las siguientes opciones es el beneficio de incrementar el cluster size de Databricks SQL warehouses?"
        ),
        "options": [
            "Reduce la latencia de la ejecución de las queries",
            "Acelera el start up time del SQL warehouse",
            "El cluster size de los SQL warehouses no es configurable. En su lugar, pueden incrementar el número de clusters",
            "Reduce el coste ya que los clusters grandes usan Spot instances"
        ],
        "answer": "Reduce la latencia de la ejecución de las queries",
        "explanation": (
            "Incrementar el cluster size del SQL Warehouse aumenta los recursos de cómputo (CPU/memoria), permitiendo que las queries "
            "se ejecuten más rápido y, por lo tanto, reduciendo la latencia de las queries."
        ),
    },
    {
        "exam": 3,
        "id": "q25_databricks_sql_alert_destinations",
        "question": (
            "¿Cuál de los siguientes destinos de alertas NO está soportado en Databricks SQL?"
        ),
        "options": [
            "SMS",
            "Slack",
            "Microsoft Teams",
            "Webhook"
        ],
        "answer": "SMS",
        "explanation": (
            "SMS no está soportado como destino de alertas en Databricks SQL. "
            "Los destinos de alertas soportados incluyen email, webhook, Slack y Microsoft Teams."
        ),
    },
    {
        "exam": 3,
        "id": "q26_delta_sharing_external_vendor",
        "question": (
            "Una organización sanitaria almacena datos sensibles de pacientes dentro de Databricks Unity Catalog. "
            "Necesitan compartir estos datos con un proveedor externo de analítica, que no usa Databricks.\n\n"
            "¿Cuál es el método más seguro y eficiente para habilitar este acceso a los datos?"
        ),
        "options": [
            "Descargar los datos como ficheros Excel protegidos y subirlos vía SFTP",
            "Crear una stored view en la base de datos para uso externo",
            "Almacenar los datos en una external table para acceso directo",
            "Usar Delta Sharing con el open sharing protocol"
        ],
        "answer": "Usar Delta Sharing con el open sharing protocol",
        "explanation": (
            "Delta Sharing comparte datos de forma segura entre plataformas usando un protocolo abierto. "
            "Permite que sistemas externos (incluso no-Databricks) consulten los datos en tiempo real sin copiar ni exportar ficheros."
        ),
    },
    {
        "exam": 3,
        "id": "q27_git_update_from_remote",
        "question": (
            "¿Cuál de las siguientes operaciones puede usar un data engineer para actualizar una carpeta Git desde su repositorio Git remoto?"
        ),
        "options": [
            "Pull",
            "Clone",
            "Commit",
            "Push"
        ],
        "answer": "Pull",
        "explanation": (
            "Git Pull hace fetch y descarga el contenido desde un repositorio remoto y actualiza el repositorio local para que coincida con él. "
            "Clone crea una copia nueva, Commit guarda localmente, y Push sube los cambios locales al repositorio remoto."
        ),
    },
    {
        "exam": 3,
        "id": "q28_dlt_stream_function_required",
        "question": (
            "Un data engineer tiene la siguiente query en un pipeline de Delta Live Tables:\n\n"
            "CREATE STREAMING TABLE sales_silver\n"
            "AS\n"
            "SELECT store_id, total + tax AS total_after_tax\n"
            "FROM sales_bronze\n\n"
            "El pipeline falla al iniciar debido a un error en esta query.\n\n"
            "¿Cuál de los siguientes cambios debería realizarse en esta query para iniciar correctamente el pipeline de DLT?"
        ),
        "options": [
            "CREATE STREAMING TABLE sales_silver AS SELECT store_id, total + tax AS total_after_tax FROM STREAMING(sales_bronze)",
            "CREATE STREAM TABLE sales_silver AS SELECT store_id, total + tax AS total_after_tax FROM STREAM(LIVE.sales_bronze)",
            "CREATE STREAMING TABLE sales_silver AS SELECT store_id, total + tax AS total_after_tax FROM STREAMING sales_bronze",
            "CREATE STREAMING TABLE sales_silver AS SELECT store_id, total + tax AS total_after_tax FROM STREAM(sales_bronze)"
        ],
        "answer": "CREATE STREAMING TABLE sales_silver AS SELECT store_id, total + tax AS total_after_tax FROM STREAM(sales_bronze)",
        "explanation": (
            "En Delta Live Tables, cuando se lee desde otra tabla streaming dentro del mismo pipeline, "
            "debes usar la función STREAM(). La sintaxis CREATE STREAMING TABLE define la tabla streaming destino "
            "y STREAM(source_table) habilita el procesamiento incremental."
        ),
    },
    {
        "exam": 3,
        "id": "q29_dlt_triggered_production_mode",
        "question": (
            "El equipo de data engineer tiene un pipeline de DLT que actualiza todas las tablas una vez y luego se detiene. "
            "Los recursos de cómputo del pipeline terminan cuando el pipeline se detiene.\n\n"
            "¿Cuál de las siguientes opciones describe mejor los modos de ejecución de este pipeline de DLT?"
        ),
        "options": [
            "El pipeline de DLT se ejecuta en modo Continuous Pipeline bajo Production mode.",
            "El pipeline de DLT se ejecuta en modo Triggered Pipeline bajo Development mode.",
            "El pipeline de DLT se ejecuta en modo Triggered Pipeline bajo Production mode.",
            "El pipeline de DLT se ejecuta en modo Continuous Pipeline bajo Development mode."
        ],
        "answer": "El pipeline de DLT se ejecuta en modo Triggered Pipeline bajo Production mode.",
        "explanation": (
            "Triggered pipelines se ejecutan una vez y luego se detienen. En Production mode, los recursos de cómputo se terminan "
            "cuando el pipeline se detiene y se habilitan la recuperación automática y los reintentos."
        ),
    },
    {
        "exam": 3,
        "id": "q30_notebook_variable_explorer",
        "question": (
            "Un data engineer quiere validar que todos los DataFrames de Spark y Pandas en su notebook están correctamente "
            "definidos. Quieren inspeccionar la estructura y los nombres de las columnas de un vistazo, con acceso inmediato al "
            "detalle completo del schema de cada DataFrame.\n\n"
            "¿Cuál de las siguientes funcionalidades soportaría mejor este objetivo?"
        ),
        "options": [
            "Notebook Environment",
            "Notebook Variable Explorer",
            "Spark UI",
            "dbutils.variables.summary()"
        ],
        "answer": "Notebook Variable Explorer",
        "explanation": (
            "El Variable Explorer en notebooks de Databricks muestra todas las variables incluyendo DataFrames de Spark y Pandas, "
            "su schema, nombres de columnas y metadata de un vistazo, con el detalle completo del schema disponible de forma interactiva."
        ),
    },
    {
        "exam": 3,
        "id": "q31_delta_primary_file_format",
        "question": (
            "En tablas Delta Lake, ¿cuál de los siguientes es el formato principal de los data files?"
        ),
        "options": [
            "JSON",
            "Parquet",
            "Delta",
            "Ambos, Parquet y JSON"
        ],
        "answer": "Parquet",
        "explanation": (
            "Delta Lake almacena los datos reales en ficheros Parquet, mientras que el transaction log (_delta_log) se almacena en formato JSON."
        ),
    },
    {
        "exam": 3,
        "id": "q32_medallion_bronze_layer",
        "question": (
            "En la Medallion Architecture, ¿cuál de las siguientes afirmaciones describe mejor la capa Bronze?"
        ),
        "options": [
            "Representa una versión filtrada, limpiada y enriquecida de los datos",
            "Mantiene datos raw ingeridos desde múltiples fuentes",
            "Mantiene datos que alimentan analítica, machine learning y aplicaciones de producción",
            "Proporciona una versión agregada de datos a nivel de negocio"
        ],
        "answer": "Mantiene datos raw ingeridos desde múltiples fuentes",
        "explanation": (
            "Las tablas Bronze almacenan datos raw, sin procesar, exactamente como se ingieren desde los sistemas origen (ficheros, streams, bases de datos, etc.). "
            "Silver contiene datos limpios/enriquecidos, y Gold contiene datos agregados a nivel de negocio."
        ),
    },
    {
        "exam": 3,
        "id": "q33_dlt_view_object_type",
        "question": (
            "Un data engineer está usando Databricks Delta Live Tables (DLT) para definir una view de pedidos recientes. "
            "El engineer ha escrito el siguiente código Python para crear la view:\n\n"
            "@dlt.view\n"
            "def recent_orders():\n"
            "    return spark.read.table(\"orders\").filter(\"year > 2025\")\n\n"
            "Basado en esta implementación, ¿qué tipo de objeto se creará cuando se ejecute este código?"
        ),
        "options": [
            "Temporary view",
            "Stored view",
            "Materialized view",
            "Streaming view"
        ],
        "answer": "Temporary view",
        "explanation": (
            "En Delta Live Tables, los objetos creados con @dlt.view son temporary pipeline views. "
            "Solo existen durante la ejecución del pipeline y no se persisten en el catálogo como las tablas."
        ),
    },
    {
        "exam": 3,
        "id": "q34_join_spill_memory_optimized",
        "question": (
            "Un equipo de data engineering está procesando un pipeline ETL a gran escala que implica hacer join entre múltiples datasets grandes, "
            "cada uno con cientos de columnas y miles de millones de registros. Durante la fase de join, observan que los Spark "
            "executors están haciendo spill repetidamente a disco, y el rendimiento se degrada significativamente debido a un shuffling excesivo.\n\n"
            "¿Qué tipo de optimización de recursos debería priorizar el equipo para mejorar el rendimiento de este job?"
        ),
        "options": [
            "GPU Optimized",
            "Storage Optimized",
            "Compute Optimized",
            "Memory Optimized"
        ],
        "answer": "Memory Optimized",
        "explanation": (
            "Los joins grandes en Spark son memory-intensive porque los datos del shuffle deben mantenerse en memoria. "
            "Si los executors hacen spill a disco, aumentar memoria (instancias memory-optimized) reduce los spills y el overhead de shuffle, "
            "mejorando significativamente el rendimiento."
        ),
    },
    {
        "exam": 3,
        "id": "q35_python_if_invalid_syntax",
        "question": (
            "Un data engineer ha desarrollado un bloque de código para reprocesar completamente los datos basándose en la siguiente condición if en Python:\n\n"
            "if process_mode = \"init\" and not is_table_exist:\n"
            "    print(\"Start processing ...\")\n\n"
            "Este bloque de código devuelve un error de sintaxis inválida.\n\n"
            "¿Cuál de los siguientes cambios debería hacerse en el bloque de código para corregir este error?"
        ),
        "options": [
            "if process_mode = \"init\" and is_table_exist = False:\n    print(\"Start processing ...\")",
            "if (process_mode = \"init\") and (not is_table_exist):\n    print(\"Start processing ...\")",
            "if process_mode == \"init\" and not is_table_exist:\n    print(\"Start processing ...\")",
            "if process_mode = \"init\" & not is_table_exist:\n    print(\"Start processing ...\")"
        ],
        "answer": "if process_mode == \"init\" and not is_table_exist:\n    print(\"Start processing ...\")",
        "explanation": (
            "En Python, las comparaciones requieren '==' y no '='. "
            "'=' es asignación y provoca un error de sintaxis dentro de condiciones. "
            "Los operadores lógicos usan 'and' y la negación usa 'not'."
        ),
    },
    {
        "exam": 3,
        "id": "q36_dlt_expect_on_violation_fail",
        "question": (
            "Un data engineer ha definido el siguiente data quality constraint en un pipeline de Delta Live Tables:\n\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) __________\n\n"
            "Rellena el hueco anterior para que los registros que violen este constraint hagan que el pipeline falle."
        ),
        "options": [
            "No es necesario añadir la cláusula ON VIOLATION. Por defecto, los registros que violan el constraint hacen fallar el pipeline.",
            "ON VIOLATION FAIL UPDATE",
            "ON VIOLATION FAIL PIPELINE",
            "ON VIOLATION FAIL"
        ],
        "answer": "ON VIOLATION FAIL UPDATE",
        "explanation": (
            "En las expectations de Delta Live Tables, ON VIOLATION FAIL UPDATE hace que el pipeline falle cuando aparecen registros inválidos. "
            "El pipeline se detiene y requiere corregir los datos o la lógica antes de volver a ejecutarlo."
        ),
    },
    {
        "exam": 3,
        "id": "q37_create_table_using_jdbc",
        "question": (
            "Rellena el hueco inferior para crear correctamente una tabla en Databricks usando datos de una base de datos PostgreSQL existente:\n\n"
            "CREATE TABLE employees\n"
            "USING __________\n"
            "OPTIONS (\n"
            "  url \"jdbc:postgresql:dbserver\",\n"
            "  dbtable \"employees\"\n"
            ")"
        ),
        "options": [
            "org.apache.spark.sql.jdbc",
            "dbserver",
            "postgresql",
            "DELTA"
        ],
        "answer": "org.apache.spark.sql.jdbc",
        "explanation": (
            "Spark SQL usa el data source JDBC (org.apache.spark.sql.jdbc) para conectarse a bases de datos relacionales externas "
            "como PostgreSQL, MySQL o SQL Server y crear tablas basadas en sus datos."
        ),
    },
    {
        "exam": 3,
        "id": "q38_query_spark_sql_table_pyspark",
        "question": (
            "En PySpark, ¿cuál de los siguientes comandos puedes usar para consultar la tabla Delta employees creada en Spark SQL?"
        ),
        "options": [
            "Las tablas de Spark SQL no pueden accederse desde PySpark",
            "spark.sql(\"employees\")",
            "pyspark.sql.read(SELECT * FROM employees)",
            "spark.table(\"employees\")"
        ],
        "answer": "spark.table(\"employees\")",
        "explanation": (
            "spark.table() devuelve una tabla de Spark SQL como un DataFrame de PySpark, permitiendo acceso directo a tablas creadas en Spark SQL."
        ),
    },
    {
        "exam": 3,
        "id": "q39_repair_failed_job_run",
        "question": (
            "Un data engineer tiene un Job con múltiples tasks que tarda más de 2 horas en completarse. "
            "En la última ejecución, la task final falló inesperadamente.\n\n"
            "¿Cuál de las siguientes acciones puede realizar el data engineer para completar este Job Run minimizando el tiempo de ejecución?"
        ),
        "options": [
            "Pueden hacer repair de este Job Run para que solo se re-ejecuten las tasks que fallaron",
            "Necesitan borrar el Run fallido y lanzar un nuevo Run para el Job",
            "Pueden mantener el Run fallido y simplemente iniciar un nuevo Run para el Job",
            "Pueden ejecutar el Job en Production mode que reintenta automáticamente la ejecución en caso de errores"
        ],
        "answer": "Pueden hacer repair de este Job Run para que solo se re-ejecuten las tasks que fallaron",
        "explanation": (
            "La funcionalidad Repair Run permite re-ejecutar solo las tasks fallidas y las tasks dependientes, evitando re-ejecutar las tasks exitosas y minimizando el tiempo de ejecución."
        ),
    },
    {
        "exam": 3,
        "id": "q40_job_cluster_production_recommendation",
        "question": (
            "Para jobs de producción, ¿cuál de los siguientes tipos de clúster se recomienda utilizar?"
        ),
        "options": [
            "Job clusters",
            "Production clusters",
            "All-purpose clusters",
            "On-premises clusters"
        ],
        "answer": "Job clusters",
        "explanation": (
            "Los Job Clusters son clústeres dedicados creados para una ejecución específica de un job y se terminan automáticamente cuando el job finaliza. "
            "Proporcionan aislamiento, fiabilidad y eficiencia de costes, por lo que son la opción recomendada para workloads de producción."
        ),
    },
    {
        "exam": 3,
        "id": "q41_job_cluster_notebook_output_limit",
        "question": (
            "Un data engineer está desarrollando un pipeline de datos automatizado en Databricks, que incluye varios notebooks ejecutados como parte de un job programado. "
            "Uno de estos notebooks realiza data profiling extensivo y genera una gran cantidad de output textual para propósitos de validación. "
            "Tras múltiples ejecuciones exitosas, el job de repente empieza a fallar sin cambios en la lógica del código ni en el volumen de datos de entrada.\n\n"
            "Tras investigar, el engineer sospecha que el problema puede estar relacionado con output excesivo generado durante la ejecución del notebook, "
            "lo que está haciendo que el job cluster alcance un límite impuesto por el sistema.\n\n"
            "Para prevenir fallos de ejecución del notebook debido al tamaño del output, ¿cuál es la cantidad máxima de output que puede manejar un job cluster?"
        ),
        "options": [
            "10 MB",
            "15 MB",
            "25 MB",
            "30 MB"
        ],
        "answer": "30 MB",
        "explanation": (
            "Los job clusters de Databricks tienen un límite máximo de tamaño de output del notebook de 30 MB. "
            "Si un notebook produce más output que este límite, el job run puede fallar aunque la lógica y los datos sean correctos."
        ),
    },
    {
        "exam": 3,
        "id": "q42_databricks_customer_data_location",
        "question": (
            "¿Cuál de las siguientes ubicaciones aloja completamente los datos del cliente?"
        ),
        "options": [
            "Control plane",
            "Databricks-managed cluster",
            "Customer's cloud account",
            "Databricks account"
        ],
        "answer": "Customer's cloud account",
        "explanation": (
            "En la arquitectura Databricks Lakehouse, los datos del cliente residen en el data plane dentro de la propia cuenta cloud del cliente "
            "(por ejemplo, AWS S3, Azure Data Lake Storage o Google Cloud Storage). El control plane gestiona metadata y servicios, "
            "pero no almacena los datos reales del cliente."
        ),
    },
    {
        "exam": 3,
        "id": "q43_spark_kafka_stream_read",
        "question": (
            "Se le ha asignado a un data engineer consumir un stream de eventos desde un topic de Kafka llamado events_topic, "
            "que está alojado en un Kafka broker remoto en host:port.\n\n"
            "¿Cuál de los siguientes snippets de código establece correctamente un streaming DataFrame en PySpark para leer desde este topic de Kafka?"
        ),
        "options": [
            """eventsStream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "host:port")
        .option("subscribe", "events_topic")
        .option("startingOffsets", "latest")
        .load()
    )""",
            """eventsStream = (spark.readStream
        .format("cloud_files")
        .option("cloudFiles.format", "kafka")
        .option("cloudFiles.bootstrap.servers", "host:port")
        .option("cloudFiles.subscribe", "events_topic")
        .option("cloudFiles.startingOffsets", "latest")
        .load()
    )""",
            """eventsStream = spark.readStream.kafka("host:port", "events_topic", "latest")""",
            """eventsStream = spark.readStream.format("kafka").load("host:port", "events_topic", "latest")"""
        ],
        "answer": """eventsStream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "host:port")
        .option("subscribe", "events_topic")
        .option("startingOffsets", "latest")
        .load()
    )""",
        "explanation": (
            "Para leer desde Kafka usando Structured Streaming en PySpark, debes usar readStream con format('kafka') y configurar "
            "las opciones kafka.bootstrap.servers, subscribe y startingOffsets. "
            "Auto Loader (cloudFiles) es solo para ingesta de ficheros, no para streams de Kafka."
        ),
    },
    {
        "exam": 3,
        "id": "q44_liquid_clustering_predictive_optimization",
        "question": (
            "¿Cómo determina Automatic Liquid Clustering qué columnas usar como clustering keys en una tabla Delta gestionada por Unity Catalog?"
        ),
        "options": [
            "Determina automáticamente las clustering keys óptimas basándose en el tipo y el orden de definición de columnas en el schema.",
            "Selecciona inteligentemente las clustering keys a partir de columnas de clustering predefinidas especificadas durante la creación de la tabla.",
            "Aprovecha estrategias avanzadas de sampling para aleatorizar la selección de columnas tras balancear uniformemente los datos entre todos los ficheros.",
            "Aprovecha Predictive Optimization para elegir clustering keys óptimas basándose en el comportamiento observado de las queries."
        ],
        "answer": "Aprovecha Predictive Optimization para elegir clustering keys óptimas basándose en el comportamiento observado de las queries.",
        "explanation": (
            "Automatic Liquid Clustering optimiza dinámicamente el data layout analizando patrones de acceso de las queries y estadísticas de metadata. "
            "A través de Predictive Optimization, Databricks selecciona y actualiza automáticamente las clustering keys para mejorar el rendimiento sin tuning manual."
        ),
    },
    {
        "exam": 3,
        "id": "q45_spot_instances_best_use_case",
        "question": (
            "¿Qué escenario es el más adecuado para usar spot instances en Databricks?"
        ),
        "options": [
            "Análisis interactivo de datos por parte de business analysts",
            "Workloads transaccionales con altas demandas de concurrencia",
            "Jobs de batch processing no críticos con capacidad de reintento",
            "Procesamiento de streams en tiempo real con requisitos estrictos de SLA"
        ],
        "answer": "Jobs de batch processing no críticos con capacidad de reintento",
        "explanation": (
            "Las spot instances pueden ser terminadas en cualquier momento por el proveedor cloud. "
            "Por ello, son más adecuadas para workloads fault-tolerant y reintentables como batch processing no crítico. "
            "No son apropiadas para workloads de baja latencia, interactivos o sensibles a SLA."
        ),
    },

    {
        "exam": 4,
        "id": "q01_allpurpose_vs_job_clusters",
        "question": (
            "¿Cuál de las siguientes describe mejor la diferencia entre all-purpose clusters y job clusters en Databricks?"
        ),
        "options": [
            "All-purpose clusters son para uso colaborativo e interactivo y persisten entre sesiones; job clusters se crean para una sola ejecución y terminan al finalizar",
            "All-purpose clusters almacenan datos, mientras job clusters gestionan metadata y triggers de jobs",
            "Job clusters solo soportan SQL, mientras all-purpose clusters soportan Python y Scala",
            "Job clusters permiten que múltiples usuarios editen notebooks de forma interactiva, mientras all-purpose clusters están limitados a jobs automatizados",
            "All-purpose clusters son serverless, mientras job clusters requieren configuración manual"
        ],
        "answer": "All-purpose clusters son para uso colaborativo e interactivo y persisten entre sesiones; job clusters se crean para una sola ejecución y terminan al finalizar",
        "explanation": (
            "Esta pregunta evalúa la diferencia conceptual entre tipos de compute en Databricks.\n\n"

            "All-purpose clusters son clusters compartidos y reutilizables diseñados para cargas interactivas como desarrollo, exploración y colaboración. "
            "Persisten entre sesiones y permiten a múltiples usuarios ejecutar notebooks de forma interactiva.\n\n"

            "Job clusters son clusters efímeros creados automáticamente para una ejecución específica de un job y terminan una vez el job finaliza. "
            "Proporcionan aislamiento y eficiencia de coste, haciéndolos ideales para pipelines automatizados de producción.\n\n"

            "Explicación de las opciones incorrectas:\n"
            "- Los clusters NO almacenan datos ni gestionan metadata; los datos se almacenan en cloud storage y la metadata en Unity Catalog/Hive metastore.\n"
            "- Ambos tipos de cluster soportan múltiples lenguajes (Python, Scala, SQL, R), no solo SQL.\n"
            "- La edición interactiva está soportada por all-purpose clusters, no por job clusters.\n"
            "- Serverless es un concepto de compute separado y no la diferencia entre estos dos tipos de cluster."
        ),
    },
    {
        "exam": 4,
        "id": "q02_sql_udf_format_name",
        "question": (
            "Un/a data engineer quiere crear una user-defined function (UDF) en Databricks usando SQL. "
            "La función debe concatenar un nombre y un apellido, capitalizar cada parte y devolver el nombre completo.\n\n"
            "¿Cuál de las siguientes sentencias define correctamente esta función?"
        ),
        "options": [
            "CREATE FUNCTION format_name(fname STRING, lname STRING)\nRETURNS STRING\nAS 'SELECT UPPER(fname) || \" \" || UPPER(lname)';",
            "CREATE FUNCTION format_name(fname STRING, lname STRING)\nRETURNS STRING\nUSING INITCAP(fname) || INITCAP(lname);",
            "CREATE OR REPLACE FUNCTION format_name(fname STRING, lname STRING)\nRETURNS STRING\nRETURN INITCAP(fname) || ' ' || INITCAP(lname);",
            "CREATE TEMPORARY FUNCTION format_name AS (fname STRING, lname STRING) -> INITCAP(fname) + ' ' + INITCAP(lname);",
            "CREATE OR REPLACE FUNCTION format_name(fname, lname)\nRETURN INITCAP(fname) || ' ' || INITCAP(lname);"
        ],
        "answer": "CREATE OR REPLACE FUNCTION format_name(fname STRING, lname STRING)\nRETURNS STRING\nRETURN INITCAP(fname) || ' ' || INITCAP(lname);",
        "explanation": (
            "Esta es la sintaxis correcta para crear una SQL UDF en Databricks. Usa CREATE OR REPLACE FUNCTION, "
            "define explícitamente los parámetros de entrada con sus tipos, especifica el tipo de retorno con RETURNS STRING, "
            "e incluye una expresión SQL válida con RETURN.\n\n"

            "Reglas clave para SQL UDFs en Databricks:\n"
            "- Debes declarar los nombres de parámetros y sus tipos.\n"
            "- Debes especificar el tipo de retorno con RETURNS <type>.\n"
            "- La función debe devolver una expresión SQL válida usando la palabra clave RETURN.\n"
            "- La concatenación de strings se realiza usando ||, no +.\n"
            "- Las SQL UDFs no pueden incluir sentencias SQL SELECT completas ni referenciar tablas.\n"
            "- La sintaxis temporal o estilo lambda no está soportada en declaraciones estándar de SQL UF.\n\n"

            "Por qué las otras opciones son incorrectas:\n"
            "- AS 'SELECT ...' es inválido porque los cuerpos de Spark SQL UDF no se definen usando strings con SELECT embebido.\n"
            "- USING no forma parte de la sintaxis de Spark SQL UDF (parece sintaxis legacy de Hive/lenguaje externo).\n"
            "- CREATE TEMPORARY FUNCTION ... -> es sintaxis tipo lambda de Scala/Python, no sintaxis de SQL UDF, y + es inválido para strings.\n"
            "- Omitir tipos de datos es inválido porque los parámetros deben declarar explícitamente sus tipos.\n\n"

            "La función toma dos entradas string, aplica INITCAP() para capitalizar cada nombre, las concatena con un espacio "
            "y devuelve el nombre completo formateado."
        ),
    },
    {
        "exam": 4,
        "id": "q03_dlt_triggered_mode_cost_optimization",
        "question": (
            "Una empresa ejecuta un pipeline de Delta Live Tables (DLT) para procesar datos de sensores IoT de equipos de fabricación. "
            "Los nuevos datos llegan a cloud storage aproximadamente cada 3 a 4 horas. El equipo necesita procesar estos datos de forma fiable "
            "pero no requiere insights en tiempo real. Además, buscan minimizar costes de compute evitando uso innecesario de clusters "
            "entre ejecuciones del pipeline.\n\n"
            "¿Qué modo de pipeline de DLT se ajusta mejor a este caso de uso?"
        ),
        "options": [
            "Triggered mode",
            "Real-time mode",
            "Streaming with Auto Loader mode",
            "Continuous mode",
            "Interactive mode"
        ],
        "answer": "Triggered mode",
        "explanation": (
            "Triggered mode ejecuta el pipeline en ejecuciones discretas solo cuando se inicia manualmente o según un schedule. "
            "Tras procesar los datos disponibles, los recursos de compute se detienen automáticamente. Esto lo convierte en la opción "
            "más coste-efectiva para escenarios de ingesta batch periódica donde no se requiere baja latencia.\n\n"

            "Modos de pipeline de DLT:\n"
            "- Continuous mode: se ejecuta indefinidamente y comprueba constantemente si hay nuevos datos, diseñado para procesamiento en tiempo real de baja latencia. "
            "Esto mantiene los clusters en ejecución y, por tanto, incrementa el coste de compute.\n"
            "- Triggered mode: procesa los datos disponibles en batches y apaga el compute al finalizar, minimizando coste.\n\n"

            "Por qué este escenario encaja con Triggered mode:\n"
            "- Los datos llegan solo cada 3–4 horas (no streaming continuo)\n"
            "- No se requieren insights en tiempo real\n"
            "- La optimización de coste es importante\n"
            "- Se necesita procesamiento batch fiable\n\n"

            "Por qué las otras opciones son incorrectas:\n"
            "- Real-time mode: no es una opción real de configuración en DLT; es solo un estilo conceptual de procesamiento.\n"
            "- Streaming with Auto Loader mode: Auto Loader es una técnica de ingesta, no un modo de ejecución del pipeline.\n"
            "- Continuous mode: está diseñado para streaming always-on y mantendría recursos de compute ejecutándose innecesariamente.\n"
            "- Interactive mode: se refiere a uso de desarrollo en notebooks, no a una estrategia de ejecución de pipeline de DLT.\n\n"

            "Por tanto, Triggered mode es ideal porque procesa datos periódicos de forma fiable mientras levanta compute solo cuando hace falta, "
            "reduciendo el coste operativo para esta carga de ingesta IoT."
        ),
    },
    {
        "exam": 4,
        "id": "q04_structured_streaming_trigger_processing_timee",
        "question": (
            "Un/a data engineer está construyendo un pipeline de Spark Structured Streaming para procesar datos de sensores IoT. "
            "El equipo quiere procesar cualquier dato nuevo de la fuente cada 60 segundos usando un enfoque de micro-batch. "
            "El/la engineer escribe el siguiente código:\n\n"
            "query = (spark.readStream\n"
            "    .format(\"delta\")\n"
            "    .load(\"/mnt/sensor_bronze\")\n"
            "    .writeStream\n"
            "    .trigger(_____)\n"
            "    .format(\"delta\")\n"
            "    .option(\"checkpointLocation\", \"/mnt/checkpoints/silver\")\n"
            "    .start(\"/mnt/sensor_silver\"))\n\n"
            "¿Cuál de las siguientes debería usarse para completar correctamente la configuración del trigger?"
        ),
        "options": [
            "Trigger.Continuous(\"60 seconds\")",
            "Trigger.ProcessingTime(\"60000\")",
            "Trigger.AvailableNow(\"60 seconds\")",
            "Trigger.ProcessingTime(\"1 minute\")",
            "Trigger.Once()"
        ],
        "answer": "Trigger.ProcessingTime(\"1 minute\")",
        "explanation": (
            "En Spark Structured Streaming, el trigger determina con qué frecuencia se procesan los datos.\n\n"

            "El requisito es ejecutar la query cada 60 segundos usando micro-batches. "
            "La configuración correcta es Trigger.ProcessingTime(\"1 minute\"), que programa un nuevo micro-batch "
            "a un intervalo fijo y procesa cualquier dato nuevo que haya llegado desde el batch anterior.\n\n"

            "Por qué esta opción es correcta:\n"
            "- Trigger.ProcessingTime(\"1 minute\") ejecuta un micro-batch cada 60 segundos.\n"
            "- Es el trigger estándar para cargas de streaming periódicas estilo batch.\n"
            "- Spark comprueba si hay nuevos datos en cada intervalo y los procesa si existen.\n"
            "- Soporta funcionalidad completa de Structured Streaming como aggregations, joins y watermarking.\n\n"

            "Por qué las otras opciones son incorrectas:\n"
            "- Trigger.Continuous(\"60 seconds\") usa modo de procesamiento continuo, no micro-batching, "
            "y está diseñado para latencia ultra-baja más que para ejecución programada.\n"
            "- Trigger.ProcessingTime(\"60000\") es inválido porque Spark espera un duration string "
            "como \"60 seconds\" o \"1 minute\", no un string numérico bruto.\n"
            "- Trigger.AvailableNow() procesa todos los datos disponibles en múltiples micro-batches y luego se detiene; "
            "no se ejecuta repetidamente cada 60 segundos.\n"
            "- Trigger.Once() procesa los datos disponibles una sola vez y termina, sin ejecución recurrente.\n\n"

            "El procesamiento continuo intenta ejecución casi en tiempo real y no sigue un schedule fijo, "
            "y además tiene soporte limitado para operaciones comparado con el modo micro-batch. "
            "Por tanto, para ingesta periódica programada cada minuto, ProcessingTime es el trigger correcto."
        ),
    },
    {
        "exam": 4,
        "id": "q05_git_branch_behind",
        "question": (
            "Dos data engineers están trabajando en el mismo notebook en un Databricks Repo. Un/a engineer hace commit y "
            "hace push de cambios a la rama remota. El/la otro/a engineer, sin saber de esta actualización, intenta hacer push de sus propios "
            "cambios sin hacer pull primero. Recibe un error de Git indicando que su rama está behind.\n\n"
            "¿Por qué ocurre este error y cómo debería resolverlo el/la engineer?"
        ),
        "options": [
            "Hicieron commit del notebook con los ajustes de usuario de Git incorrectos y deberían configurar de nuevo las credenciales de Git.",
            "La rama remota tiene nuevos commits que no han hecho pull; deben hacer un Git Pull para hacer merge antes de hacer push.",
            "Databricks no soporta edición simultánea en Repos, así que el notebook debe bloquearse para uso exclusivo.",
            "Su cluster local está fuera de sincronización con el control plane y deben reiniciarlo para aplicar cambios de Git.",
            "El/la engineer intentó hacer push a una rama read-only y debe solicitar acceso de escritura al propietario del repo."
        ],
        "answer": "La rama remota tiene nuevos commits que no han hecho pull; deben hacer un Git Pull para hacer merge antes de hacer push.",
        "explanation": (
            "Git impide hacer push si la rama local está behind respecto a la rama remota. Otro usuario hizo push de nuevos commits, "
            "así que el historial local está desactualizado. Permitir el push sobrescribiría el trabajo de otra persona.\n\n"

            "El workflow correcto es:\n"
            "1) Hacer pull de los últimos cambios del repositorio remoto\n"
            "2) Hacer merge de esos cambios en la rama local (y resolver conflictos si es necesario)\n"
            "3) Hacer push de la rama actualizada\n\n"

            "Por qué las otras opciones son incorrectas:\n"
            "- Credenciales de Git incorrectas causarían un error de autenticación, no un error de 'branch behind'.\n"
            "- Databricks Repos SÍ soporta desarrollo colaborativo; Git gestiona la consistencia mediante merges.\n"
            "- El estado del cluster no tiene nada que ver con el historial de Git ni con la sincronización del repositorio.\n"
            "- Una rama read-only produciría un error de permisos, no un error de divergencia.\n\n"

            "Git impone esta regla para evitar sobrescrituras accidentales y mantener la integridad del historial de commits. "
            "La solución siempre es sincronizar con la rama remota (git pull), resolver conflictos si aparecen, "
            "y solo entonces hacer push de los cambios."
        ),
    },
]