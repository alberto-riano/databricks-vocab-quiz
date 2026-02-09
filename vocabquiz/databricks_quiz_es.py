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
]