DATABRICKS_QUIZ = [
    # ---------------- EXAM 1 ----------------

    {
        "exam": 1,
        "id": "q01_delta_optimize_compaction",
        "question": "Which of the following commands can a data engineer use to compact small data files of a Delta table into larger ones?",
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
            "A Data Engineer is developing a data pipeline to process IoT sensor data stored in Parquet format within Google "
            "Cloud Storage. The goal is to create a Delta table using DLT (Delta Live Tables) that reads this data as its source. "
            "Given the context, which of the following options correctly implements this functionality in Python?"
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
        "question": "Which of the following code blocks can a data engineer use to create a user defined function (UDF)?",
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
        "question": "What is the primary purpose of Lakehouse Federation in data architecture?",
        "options": [
            "To create backups of data stored in Databricks",
            "To migrate all data into Databricks for centralized processing",
            "To optimize storage costs by compressing data",
            "To enable direct querying across multiple data sources without duplicating data",
        ],
        "answer": "To enable direct querying across multiple data sources without duplicating data",
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
            "A data engineer is analyzing a dataset of clickstream events from a high-traffic website. The dataset includes fields "
            "such as user_id, timestamp, event_type, and page_url. During a join operation between the clickstream logs and a user "
            "profile dataset (joined on user_id), the job’s performance is significantly hindered due to uneven data distribution. "
            "Further analysis confirms a data skew caused by a small subset of users generating a disproportionately large number of events.\n"
            "Which of the following approaches is NOT an appropriate solution to mitigate the skew in this scenario?"
        ),
        "options": [
            "Separate processing of skewed keys by handling high-frequency users in a dedicated job.",
            "Broadcast the skewed keys to all worker nodes to avoid shuffle during the join.",
            "Use salting by appending a random prefix to skewed user_id values to distribute the load across partitions.",
            "Repartition the clickstream dataset to increase the number of partitions before the join.",
        ],
        "answer": "Broadcast the skewed keys to all worker nodes to avoid shuffle during the join.",
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
        "question": "Which of the following compute resources is available in Databricks SQL?",
        "options": ["SQL warehouses", "Single-node clusters", "Multi-nodes clusters", "SQL engines"],
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
        "question": "Which of the following best describes the correct format of a Databricks Asset Bundle configuration file?",
        "options": [
            "A YAML file named bundle-config.yml with fields for environment variables and user roles",
            "A YAML file named databricks.yml that defines the bundle's structure, including targets, resources, and configurations.",
            "A JSON file named databricks_asset.json containing cluster definitions and job schedules",
            "A XML file named asset_bundle.xml specifying the workspace path, job IDs, and compute specifications",
        ],
        "answer": "A YAML file named databricks.yml that defines the bundle's structure, including targets, resources, and configurations.",
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
        "question": "How long is the default retention period of the VACUUM command?",
        "options": ["365 days", "0 days", "30 days", "7 days"],
        "answer": "7 days",
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
            "Given the following table faculties... Fill in the following blank to get the students enrolled in less than 3 courses from the array column students.\n"
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
        "question": "Which of the following will utilize Gold tables as their source?",
        "options": ["Bronze tables", "Dashboards", "Silver tables", "Auto loader"],
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
            "A data engineer is designing a Delta Live Tables pipeline. The source system generates files containing changes captured in the source data. "
            "Each change event has metadata indicating whether the specified record was inserted, updated, or deleted. In addition to a timestamp column "
            "indicating the order in which the changes happened. The data engineer needs to update a target table based on these change events.\n"
            "Which of the following commands can the data engineer use to best solve this problem?"
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
            "A data engineer wants to validate that all variables defined in their Python notebook are numerical values. "
            "Which of the following provides real-time inspection of the data structures and types in Databricks notebooks?"
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
            "A data engineering team is developing a complex ETL pipeline on Databricks. They want to ensure that their workflow configurations are version-controlled "
            "and can be deployed reliably across staging and production environments.\n"
            "What is the most appropriate solution to achieve this task?"
        ),
        "options": [
            "Leverage Notebook built-in version history for source control, and create jobs using Databricks UI",
            "Store the source code in Git Folders, and deploy jobs using Databricks REST API",
            "Leverage Notebook built-in version history for source control, and deploy jobs using serverless notebook's Environment",
            "Store the source code in Git folders, and deploy jobs using Databricks Asset Bundles (DAB)",
        ],
        "answer": "Store the source code in Git folders, and deploy jobs using Databricks Asset Bundles (DAB)",
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
            "A data engineer attempts to execute a Scala script on a serverless compute but encounters failure. "
            "Which of the following statements best explains this failure?"
        ),
        "options": [
            "Scala is not a supported language on the Databricks platform",
            "Scala requires a classic compute with runtime version 16.4 LTS or above",
            "The serverless compute supports only SQL",
            "The serverless compute supports only SQL and Python",
        ],
        "answer": "The serverless compute supports only SQL and Python",
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
            "Given the following Structured Streaming query:\n"
            "Python\n"
            "(spark.readStream\n"
            "  .format(\"cloudFiles\")\n"
            "  .option(\"cloudFiles.format\", \"json\")\n"
            "  .load(ordersLocation)\n"
            ".writeStream\n"
            "  .option(\"checkpointLocation\", checkpointPath)\n"
            "  .table(\"uncleanedOrders\")\n"
            ")\n"
            "Which of the following best describe the purpose of this query in a Medallion Architecture?"
        ),
        "options": [
            "The query is performing data transfer from a Gold table into a production application",
            "The query is performing raw data ingestion into a Bronze table",
            "The query is performing a hop from Silver table to a Gold table",
            "The query is performing a hop from a Bronze table to a Silver table",
        ],
        "answer": "The query is performing raw data ingestion into a Bronze table",
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
        "question": "Which of the following statements is Not true about Delta Lake?",
        "options": [
            "Delta Lake provides scalable data and metadata handling",
            "Delta Lake builds upon standard data formats: Parquet + XML",
            "Delta Lake provides ACID transaction guarantees",
            "Delta Lake provides audit history and time travel",
        ],
        "answer": "Delta Lake builds upon standard data formats: Parquet + XML",
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
            "A data engineer is analyzing customer transactions and needs to determine the maximum and minimum transaction amounts for each customer. "
            "They use the following code structure:\n"
            "Python\n"
            "result_df = df.________(\"customer_id\").agg(\n"
            "    F.max(\"transaction_amount\").alias(\"max_transaction\"),\n"
            "    F.min(\"transaction_amount\").alias(\"min_transaction\")\n"
            ")\n"
            "Which function correctly fills in the blank to meet the specified requirement?"
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
            "A data engineer is working on a complex data transformation pipeline in a Python notebook and encounters unexpected None values being returned in one of their functions. "
            "They want to pause the execution at a specific line, step through the code, and check the state of their variables in real-time to identify the source of the issue.\n"
            "Which feature should the data engineer use to achieve this task?"
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
            "A data engineer needs to execute a scheduled Python notebook on an hourly basis. This job involves processing relatively small volumes of data. "
            "The execution environment should be efficient, require no cluster pre-warming, and deliver consistent, reliable performance.\n"
            "Considering these requirements, which compute option is recommended for executing this job?"
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
        "question": (
            "If the default notebook language is SQL, which of the following options a data engineer can use to run a Python code in this SQL Notebook?"
        ),
        "options": [
            "This is not possible! They need to change the default language of the notebook to Python",
            "They can add %python at the start of a cell.",
            "They can add %language magic command at the start of a cell to force language detection.",
            "They need first to import the python module in a cell",
        ],
        "answer": "They can add %python at the start of a cell.",
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
            "The data engineer team has a DLT pipeline that updates all the tables at defined intervals until manually stopped. "
            "The compute resources terminate when the pipeline is stopped.\n"
            "Which of the following best describes the execution modes of this DLT pipeline?"
        ),
        "options": [
            "The DLT pipeline executes in Triggered Pipeline mode under Production mode.",
            "The DLT pipeline executes in Continuous Pipeline mode under Production mode.",
            "The DLT pipeline executes in Triggered Pipeline mode under Development mode.",
            "The DLT pipeline executes in Continuous Pipeline mode under Development mode.",
        ],
        "answer": "The DLT pipeline executes in Continuous Pipeline mode under Production mode.",
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
            "A data analytics firm uses Delta Sharing to collaborate with a partner company. The firm stores the data in AWS US-East-1, "
            "while the partner is hosted on Azure in Europe. After initiating large-scale data sharing, the firm notices unexpected costs in their billing dashboard.\n"
            "What is the most likely reason for the unexpected cost increase?"
        ),
        "options": [
            "Storage costs because Delta Sharing requires replication of data across clouds and regions",
            "Flat rate fee is applied for all shared data by Delta Sharing protocol",
            "DBU costs for running Delta Sharing servers across cloud boundaries",
            "Egress fees are incurred when data transferred between cloud providers and regions",
        ],
        "answer": "Egress fees are incurred when data transferred between cloud providers and regions",
        "explanation": (
            "Aunque Delta Sharing es un protocolo abierto y gratuito (no cobra licencias extra), la infraestructura de nube subyacente sí cobra por el movimiento de datos.\n"
            "• Egress Fees (Tarifas de salida): Los proveedores de nube (como AWS, Azure, Google Cloud) cobran cuando los datos \"salen\" de su red o región.\n"
            "• En este caso, los datos salen de AWS (EE.UU.) hacia Azure (Europa). Al cruzar tanto fronteras de proveedores como de regiones geográficas, se aplican tarifas de salida de datos (Network Egress) que pueden ser significativas si el volumen de datos es grande."
        ),
    },
    {
        "exam": 1,
        "id": "q23_sql_warehouse_auto_stop_benefit",
        "question": "Which of the following is the benefit of using the Auto Stop feature of Databricks SQL warehouses?",
        "options": [
            "Improves the performance of the warehouse by automatically stopping ideal services",
            "Minimizes the total running time of the warehouse",
            "Increases the availability of the warehouse by automatically stopping long-running SQL queries",
            "Provides higher security by automatically stopping unused ports of the warehouse",
        ],
        "answer": "Minimizes the total running time of the warehouse",
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
            "A data engineer is trying to use Delta time travel feature to rollback a table to a previous version, but the data engineer received an error that the data files are no longer present.\n"
            "Which of the following commands was run on the table that caused deleting the data files?"
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
            "The data engineering team has a Delta table called employees that contains the employees personal information including their gross salaries.\n"
            "Which of the following code blocks will keep only the employees having a salary greater than 3000 in the table?"
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
        "question": "Which of the following locations hosts the Databricks web application?",
        "options": ["Databricks-managed cluster", "Control plane", "Data plane", "Customer Cloud Account"],
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
        "question": "In the Medallion Architecture, which of the following statements best describes the Silver layer tables?",
        "options": [
            "The table structure in this layer resembles that of the source system table structure with any additional metadata columns like the load time, and input file name.",
            "They maintain data that powers analytics, machine learning, and production applications",
            "They maintain raw data ingested from various sources",
            "They provide a more refined view of raw data, where it’s filtered, cleaned, and enriched.",
        ],
        "answer": "They provide a more refined view of raw data, where it’s filtered, cleaned, and enriched.",
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
            "A data engineer has implemented the following streaming ingestion code using Databricks Auto Loader:\n"
            "Python\n"
            "spark.readStream \\\n"
            "    .format(\"cloudFiles\") \\\n"
            "    .option(\"cloudFiles.schemaEvolutionMode\", \"failOnNewColumns\") \\\n"
            "    .load(\"s3://vendor/raw/sales/json/\")\n"
            "What is the expected behavior of this streaming job if a new column appears in the incoming JSON files that is not part of the original schema?"
        ),
        "options": [
            "The stream fails, and all new columns are saved in a rescued data column for later processing.",
            "The stream fails temporarily but continues by ignoring the new columns without schema update.",
            "The stream fails and will not restart unless the schema is manually updated or the problematic data file is removed.",
            "The stream fails, but it automatically restarts after updating the schema with the new columns.",
        ],
        "answer": "The stream fails and will not restart unless the schema is manually updated or the problematic data file is removed.",
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
        "question": "What is the primary benefit of using spot instances in Databricks clusters?",
        "options": [
            "Significantly lower compute costs",
            "Increased security and compliance",
            "Guaranteed job execution time",
            "Reduced data storage latency",
        ],
        "answer": "Significantly lower compute costs",
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
        "question": "Which of the following commands can a data engineer use to create a new table along with a comment?",
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
            "A data engineer uses the following SQL query:\n"
            "SQL\n"
            "GRANT MODIFY ON TABLE employees TO hr_team\n"
            "Which of the following describes the ability given by the MODIFY privilege?"
        ),
        "options": [
            "It gives the ability to add, update, or delete data within the table",
            "It gives the ability to modify data in the table",
            "It gives the ability to delete data from the table",
            "It gives the ability to add data from the table",
        ],
        "answer": "It gives the ability to add, update, or delete data within the table",
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
        "question": "Which of the following commands can a data engineer use to grant full permissions to the HR team on the table employees?",
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
        "question": "According to the Databricks Lakehouse architecture, which of the following is located in the customer's cloud account?",
        "options": ["Databricks web application", "Cluster virtual machines", "Workflows", "Notebooks"],
        "answer": "Cluster virtual machines",
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
            "\"A feature that illustrates the relationship between different data assets including tables, queries, notebooks, and dashboards, enabling users to trace the origin and flow of data across the entire lakehouse platform.\"\n"
            "Which of the following is being described in the above statement?"
        ),
        "options": ["Delta Live Tables DAGs", "Databricks Lakeflow", "Databricks Jobs", "Unity Catalog Data Lineage"],
        "answer": "Unity Catalog Data Lineage",
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
            "A data engineering team has a long-running multi-tasks Job. The team members need to be notified when the run of this job completes.\n"
            "Which of the following approaches can be used to send emails to the team members when the job completes?"
        ),
        "options": [
            "Only Job owner can be configured to be notified when the job completes",
            "They can use Job API to programmatically send emails according to each task status",
            "They can configure email notifications settings in the job page",
            "There is no way to notify users when the job completes",
        ],
        "answer": "They can configure email notifications settings in the job page",
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
        "question": "Which of the following statements best describes Auto Loader?",
        "options": [
            "Auto loader monitors a source location, in which files accumulate, to identify and ingest only new arriving files with each command run. While the files that have already been ingested in previous runs are skipped.",
            "Auto loader allows cloning a source Delta table to a target destination at a specific version.",
            "Auto loader allows applying Change Data Capture (CDC) feed to update tables based on changes captured in source data.",
            "Auto loader enables efficient insert, update, deletes, and rollback capabilities by adding a storage layer that provides better data reliability to data lakes.",
        ],
        "answer": "Auto loader monitors a source location, in which files accumulate, to identify and ingest only new arriving files with each command run. While the files that have already been ingested in previous runs are skipped.",
        "explanation": (
            "• Qué es Auto Loader (cloudFiles): Es una herramienta optimizada para la ingestión incremental de archivos. Su función principal es \"escuchar\" una carpeta en la nube (S3/ADLS/GCS), detectar cuándo llega un archivo nuevo y procesarlo una sola vez.\n"
            "* Por qué tu respuesta fue incorrecta: Seleccionaste la opción que describe a Delta Lake (\"efficient insert, update, deletes, and rollback capabilities...\"). Delta Lake es el formato de almacenamiento; Auto Loader es el mecanismo para meter datos dentro de Delta Lake desde archivos externos.\n"
            "• Nota: La opción de CDC se refiere a Delta Live Tables (APPLY CHANGES INTO)."
        ),
    },
    {
        "exam": 1,
        "id": "q37_liquid_clustering_function",
        "question": "What is the primary function of Liquid Clustering in Databricks?",
        "options": [
            "To automate the creation of new data pipelines",
            "To improve the speed of network connectivity between nodes",
            "To incrementally optimize data layout for improved query performance",
            "To encrypt data stored in Delta Lake",
        ],
        "answer": "To incrementally optimize data layout for improved query performance",
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
            "Given the following Structured Streaming query:\n"
            "Python\n"
            "(spark.table(\"orders\")\n"
            "    .withColumn(\"total_after_tax\", col(\"total\") + col(\"tax\"))\n"
            "  .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .outputMode(\"append\")\n"
            "    .________\n"
            "    .table(\"new_orders\")\n"
            ")\n"
            "Fill in the blank to make the query executes a micro-batch to process data every 2 minutes."
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
            "A production environment has an S3 bucket receiving thousands of image files daily in different formats (.png, .jpg, .gif). "
            "A data engineer has been tasked with modifying the following streaming ingestion script to ensure only .jpg files are processed.\n"
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
            "A data engineer has defined the following data quality constraint in a Delta Live Tables pipeline:\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ________\n"
            "Fill in the above blank so records violating this constraint will be added to the target table, and reported in metrics."
        ),
        "options": [
            "ON VIOLATION FAIL UPDATE",
            "There is no need to add ON VIOLATION clause. By default, records violating the constraint will be kept, and reported as invalid in the event log",
            "ON VIOLATION NONE",
            "ON VIOLATION ADD ROW",
        ],
        "answer": "There is no need to add ON VIOLATION clause. By default, records violating the constraint will be kept, and reported as invalid in the event log",
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
            "When dropping a Delta table, which of the following explains why only the table's metadata will be deleted, while the data files will be kept in the storage?"
        ),
        "options": [
            "The table is external",
            "The user running the command has no permission to delete the data files",
            "The table is managed",
            "Delta prevents deleting files less than retention threshold, just to ensure that no long-running operations are still referencing any of the files to be deleted",
        ],
        "answer": "The table is external",
        "explanation": (
            "Esta es una diferencia fundamental en Databricks y Delta Lake:\n"
            "• Managed Tables (Tablas Gestionadas): Databricks gestiona tanto los metadatos como los archivos de datos físicos en su almacenamiento. Si haces DROP TABLE, se borra todo (metadatos + archivos).\n"
            "• External Tables (Tablas Externas o Unmanaged): Tú gestionas los archivos de datos en tu propio almacenamiento (S3, ADLS, GCS) y le dices a Databricks dónde están. Si haces DROP TABLE, Databricks solo borra el \"puntero\" (los metadatos en el catálogo). Los archivos físicos (.parquet) permanecen intactos en el almacenamiento en la nube para seguridad."
        ),
    },
    {
        "exam": 1,
        "id": "q42_cron_syntax_jobs",
        "question": "Which of the following describes Cron syntax in Databricks Jobs?",
        "options": [
            "It’s an expression to represent the run timeout of a job",
            "It’s an expression to represent complex job schedule that can be defined programmatically",
            "It’s an expression to represent the retry policy of a job",
            "It’s an expression to represent the maximum concurrent runs of a job",
        ],
        "answer": "It’s an expression to represent complex job schedule that can be defined programmatically",
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
            "Given the two tables students_course_1 and students_course_2. Which of the following commands can a data engineer use to get all the students "
            "from the above two tables without duplicate records?"
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
            "The engineering team is managing a large Delta Lake table with frequent updates and deletes. They notice that query performance is degrading over time due "
            "to an increase in small data files. To address this, they decide to run the OPTIMIZE command with Z-Order indexing on columns frequently used in filters.\n"
            "What type of resource optimization should the engineering team prioritize to ensure these commands execute efficiently?"
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
            "A retail company stores sales data in Delta tables within Databricks Unity Catalog. They need to securely share specific tables with an external auditing firm, "
            "who uses Databricks on a different cloud provider.\n"
            "Which of the following options enable achieving this task without data replication?"
        ),
        "options": ["Shallow clone", "Databricks Connect", "Databricks-to-Databricks Delta Sharing", "External schema in Unity Catalog"],
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
            "Which of the following operations can a data engineer use to save local changes of a Git folder to its remote repository?"
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
            "Which of the following SQL keywords can be used to rotate rows of a table by turning row values into multiple columns?"
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
            "Which of the following functionalities can be performed in Git folders?"
        ),
        "options": [
            "Create pull requests",
            "Delete branches",
            "Pull changes from a remote Git repository",
            "Create new remote Git repositories"
        ],
        "answer": "Pull changes from a remote Git repository",
        "explanation": (
            "Los Git folders soportan la operación git pull.\n"
            "Se utiliza para hacer fetch y descargar contenido desde un remote repository y actualizar inmediatamente el repositorio local para que coincida con ese contenido."
        ),
    },
    {
        "exam": 2,
        "id": "q04_databricks_data_warehousing_service",
        "question": (
            "Which of the following services provides a data warehousing solution in the Databricks Intelligence Platform?"
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
            "Fill in the following blank to successfully create a table using data from CSV files located at /path/input\n\n"
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
            "Which of the following techniques allows Auto Loader to track the ingestion progress and store metadata of the discovered files?"
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
            "A data engineer has a custom-location schema named db_hr, and they want to know where this schema was created in the underlying storage.\n\n"
            "Which of the following commands can the data engineer use to complete this task?"
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
            "A data scientist from the marketing department requires read-only access to the ‘customer_insights’ table located in the analytics schema, which is part of the BI catalog. "
            "The data will be used to generate quarterly customer engagement reports. In accordance with the principle of least privilege, only the minimum permissions necessary to perform the required tasks should be granted.\n\n"
            "Which SQL commands will correctly grant access with the least privileges?"
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
            "A data engineering team is using the Silver Layer in the Medallion Architecture to join customer data with external lookup tables and apply filters.\n\n"
            "A team member makes the following claims about the Silver Layer. Which of these claims is incorrect?"
        ),
        "options": [
            "The Silver Layer handles data de-duplication",
            "The Silver Layer integrates with other sources for data enrichment",
            "The Silver Layer stores raw data enriched with source file details and ingestion timestamps",
            "The Silver Layer is responsible for data cleansing and filtering"
        ],
        "answer": "The Silver Layer stores raw data enriched with source file details and ingestion timestamps",
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
            "When dropping a Delta table, which of the following explains why both the table's metadata and the data files will be deleted?"
        ),
        "options": [
            "The data files are older than the default retention period",
            "The table is shallow cloned",
            "The table is managed",
            "The table is external"
        ],
        "answer": "The table is managed",
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
            "Which of the following code blocks can a data engineer use to query the events table as a streaming source?"
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
            "An e-commerce company experiences rapid data growth due to seasonal traffic spikes. Their engineering team needs to ensure that batch processing jobs complete within a fixed timeframe, even during peak hours. "
            "The team has limited human resources for infrastructure management and seeks a solution with automated scaling and optimization.\n\n"
            "Which option best fulfills these conditions?"
        ),
        "options": [
            "Databricks Serverless compute",
            "All-purpose clusters with autoscaling enabled",
            "Dedicated clusters with Photon enabled",
            "Job clusters with maximum resource allocation"
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
            "The data engineer team has a DLT pipeline that updates all the tables at defined intervals until manually stopped. "
            "The compute resources of the pipeline continue running to allow for quick testing.\n\n"
            "Which of the following best describes the execution modes of this DLT pipeline ?"
        ),
        "options": [
            "The DLT pipeline executes in Triggered Pipeline mode under Production mode.",
            "The DLT pipeline executes in Continuous Pipeline mode under Development mode.",
            "The DLT pipeline executes in Continuous Pipeline mode under Production mode.",
            "The DLT pipeline executes in Triggered Pipeline mode under Development mode."
        ],
        "answer": "The DLT pipeline executes in Continuous Pipeline mode under Development mode.",
        "explanation": (
            "Los pipelines Continuous actualizan las tablas continuamente hasta que se detienen manualmente.\n"
            "En Development mode el cluster permanece activo para facilitar pruebas rápidas y evitar reinicios constantes."
        ),
    },
    {
        "exam": 2,
        "id": "q14_schedule_query_refresh",
        "question": (
            "From which of the following locations can a data engineer set a schedule to automatically refresh an SQL query in Databricks?"
        ),
        "options": [
            "There is no way to automatically refresh a query in Databricks. Schedules can be set only for dashboards and alerts to refresh their underlying queries.",
            "From the SQL Editor in Databricks SQL",
            "From the jobs UI",
            "From the Alerts page in Databricks SQL"
        ],
        "answer": "From the SQL Editor in Databricks SQL",
        "explanation": (
            "En Databricks SQL se puede programar la ejecución automática de una query directamente desde el SQL Editor."
        ),
    },
    {
        "exam": 2,
        "id": "q15_databricks_connect_purpose",
        "question": (
            "Which of the following best describes the purpose and functionality of Databricks Connect?"
        ),
        "options": [
            "Databricks Connect is a client library that allows engineers to develop Spark code locally using their IDE, while executing that code remotely on a Databricks cluster.",
            "Databricks Connect is a data visualization tool designed to create and render interactive dashboards outside the Databricks environment.",
            "Databricks Connect is a data ingestion solution that offers simple and efficient connectors to ingest data from various data sources into the lakehouse.",
            "Databricks Connect is an open protocol developed by Databricks for secure data sharing with other organizations regardless of the computing platforms they use."
        ],
        "answer": "Databricks Connect is a client library that allows engineers to develop Spark code locally using their IDE, while executing that code remotely on a Databricks cluster.",
        "explanation": (
            "Databricks Connect es una client library del Databricks Runtime que permite desarrollar Spark code localmente desde un IDE y ejecutarlo remotamente en un Databricks cluster.\n"
            "Facilita escribir, testear y debuggear código aprovechando la capacidad de cómputo del cluster remoto."
        ),
    },
    {
        "exam": 2,
        "id": "q16_change_table_owner_catalog_explorer",
        "question": (
            "In which of the following locations can a data engineer change the owner of a table?"
        ),
        "options": [
            "In the Catalog Explorer, under the Permissions tab of the table's page",
            "In the Catalog Explorer, from the Owner field in the database's page, since owners are set at database-level",
            "In the Catalog Explorer, under the Permissions tab of the database's page, since owners are set at database-level",
            "In the Catalog Explorer, from the Owner field in the table's page"
        ],
        "answer": "In the Catalog Explorer, from the Owner field in the table's page",
        "explanation": (
            "Desde el Catalog Explorer se puede navegar a la página de la tabla y cambiar su propietario directamente en el campo Owner.\n"
            "El ownership se gestiona a nivel de objeto (tabla), no únicamente a nivel de database."
        ),
    },
    {
        "exam": 2,
        "id": "q17_git_folders_vs_notebook_versioning",
        "question": (
            "A junior data engineer uses the built-in Databricks Notebooks versioning for source control. A senior data engineer recommended using Git folders instead.\n\n"
            "Which of the following could explain why Git folders is recommended instead of Databricks Notebooks versioning?"
        ),
        "options": [
            "Git folders store source code files in Unity Catalog for centralized security and governance",
            "Git folders automatically sync all notebook changes to the remote Git repository in real time",
            "Git folders support automatic conflict resolution when multiple users edit the same notebook",
            "Git folders support creating and managing branches for development work."
        ],
        "answer": "Git folders support creating and managing branches for development work.",
        "explanation": (
            "Una ventaja clave de Git folders frente al versionado nativo de Notebooks es que permiten trabajar con branches.\n"
            "Esto facilita workflows de desarrollo como feature branches, pull requests y colaboración entre developers."
        ),
    },
    {
        "exam": 2,
        "id": "q18_notebook_interactive_debugger",
        "question": (
            "A data scientist wants to test a newly written Python function that parses and normalizes user input. Rather than relying solely on print statements or logs, they prefer a more dynamic way to track the flow of data and understand how different variables change as the function executes.\n\n"
            "Which tool should the data scientist use to gain these insights effectively within a Databricks notebook?"
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
            "What is a key benefit of Liquid Clustering for analytical workloads in Databricks?"
        ),
        "options": [
            "It reduces the volume of scanned data during query execution",
            "It encrypts sensitive data fields automatically during ingestion",
            "It prevents data duplication, thereby enhancing query performance",
            "It ensures real-time streaming from input datasource"
        ],
        "answer": "It reduces the volume of scanned data during query execution",
        "explanation": (
            "Liquid Clustering optimiza el layout físico de las tablas Delta organizando los datos por clustering keys.\n"
            "Esto permite data skipping y reduce la cantidad de datos escaneados durante la ejecución de queries, mejorando el rendimiento."
        ),
    },
    {
        "exam": 2,
        "id": "q20_repair_failed_job_task",
        "question": (
            "A large Databricks job fails at task 12 of 15 due to a missing configuration file. After resolving the issue, what is the most appropriate action to resume the workflow?"
        ),
        "options": [
            "Repair run from task 12",
            "Wait for the next scheduled run",
            "Run the notebook associated with task 12 manually",
            "Restart the job"
        ],
        "answer": "Repair run from task 12",
        "explanation": (
            "Databricks permite reparar jobs fallidos ejecutando solo las tareas que fallaron y sus dependencias.\n"
            "Las tareas completadas correctamente no se vuelven a ejecutar, reduciendo tiempo y consumo de recursos."
        ),
    },
    {
        "exam": 2,
        "id": "q21_insert_into_delta_table",
        "question": (
            "Which of the following SQL commands will append this new row to the existing Delta table users?\n\n"
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
            "If the default notebook language is Python, which of the following options a data engineer can use to run SQL commands in this Python Notebook?"
        ),
        "options": [
            "They can add %sql at the start of a cell.",
            "This is not possible! They need to change the default language of the notebook to SQL",
            "They can add %language magic command at the start of a cell to force language detection.",
            "They need first to import the SQL library in a cell"
        ],
        "answer": "They can add %sql at the start of a cell.",
        "explanation": (
            "En un notebook de Databricks se puede sobrescribir el lenguaje por celda usando magic commands.\n"
            "Añadiendo %sql al inicio de la celda se pueden ejecutar sentencias SQL dentro de un notebook Python."
        ),
    },
    {
        "exam": 2,
        "id": "q23_job_failure_email_notifications",
        "question": (
            "A data engineering team has a multi-tasks Job in production. The team members need to be notified in the case of job failure.\n\n"
            "Which of the following approaches can be used to send emails to the team members in the case of job failure ?"
        ),
        "options": [
            "Only Job owner can be configured to be notified in the case of job failure",
            "They can configure email notifications settings in the job page",
            "There is no way to notify users in the case of job failure",
            "They can use Job API to programmatically send emails according to each task status"
        ],
        "answer": "They can configure email notifications settings in the job page",
        "explanation": (
            "Databricks Jobs permite configurar notificaciones por email para eventos como start, success o failure desde la página del job.\n"
            "Se pueden añadir múltiples direcciones en la sección de notificaciones."
        ),
    },
    {
        "exam": 2,
        "id": "q24_autoloader_schema_evolution_rescue",
        "question": (
            "A data engineer is designing a streaming ingestion pipeline using Auto Loader. The requirement is that the pipeline should never fail on schema changes but must capture any new columns that arrive in the data for later inspection.\n\n"
            "Which of the following schema evolution modes should the engineer use to meet this requirement?"
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
            "A data engineer from a global logistics company needs to share specific datasets and analysis notebooks with an external analytics vendor, who is a Databricks client. "
            "The data is stored as Delta tables in Unity Catalog, and the vendor does not have access to the company Databricks account.\n\n"
            "What is the most effective and secure way to share the data and notebooks with the external vendor?"
        ),
        "options": [
            "Share the Delta tables using Delta Sharing, and send all the notebooks together in a single DBC file",
            "Share the Delta tables using Delta Sharing, and publish the notebooks as HTML pages programmatically",
            "Share the Delta tables using Delta Sharing, and grant access to each notebook via its built-in collaboration feature",
            "Share the Delta tables and notebooks using Delta Sharing"
        ],
        "answer": "Share the Delta tables and notebooks using Delta Sharing",
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
            "Which part of the Databricks platform can a data engineer use to revoke permissions from users on tables ?"
        ),
        "options": [
            "Catalog Explorer",
            "Workspace Admin Console",
            "Account Console",
            "There is no way to revoke permissions in the Databricks platform. The data engineer needs to clone the table with the updated permissions"
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
            "Which of the following services can a data engineer use for task orchestration in the Databricks platform?"
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
            "Given the following Structured Streaming query:\n\n"
            "(spark.readStream\n"
            "    .table(\"orders\")\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .table(\"Output_Table\")\n"
            ")\n\n"
            "Which of the following is the trigger Interval for this query ?"
        ),
        "options": [
            "Every half second",
            "The query will run in batch mode to process all available data at once, then the trigger stops.",
            "Every half minute",
            "Every half hour"
        ],
        "answer": "Every half second",
        "explanation": (
            "Si no se especifica un trigger en Structured Streaming, Spark usa el valor por defecto.\n"
            "El processingTime por defecto es 500 ms, equivalente a ejecutar un micro-batch cada medio segundo."
        ),
    },
    {
        "exam": 2,
        "id": "q29_storage_optimized_for_ad_hoc_queries",
        "question": (
            "An analyst runs frequent ad hoc queries on a large Delta Lake dataset using Databricks. However, the analyst is experiencing slow query performance. "
            "They need to achieve quick, interactive responses for exploratory analysis by relying on cached data.\n\n"
            "Which instance type is best suited for this workload?"
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
            "A data engineer at a global e-commerce enterprise is tasked with building a near real-time analytics pipeline using Delta Live Tables (DLT). "
            "The goal is to continuously process clickstream data that records user interactions across multiple regional websites.\n\n"
            "The raw event data is ingested from edge services into two primary locations:\n"
            "1- A Delta table registered in Unity Catalog, located at: ecommerce.analytics.raw_clickstream\n"
            "2- An S3 bucket, where incoming data lands as Parquet files, at the following path: s3://ecommerce/analytics/clickstream/\n\n"
            "To support near real-time use cases such as personalized product recommendations, fraud detection, and live performance dashboards, the data engineer needs to define a streaming table within a DLT pipeline. "
            "This table must continuously ingest new records as they arrive in the data source.\n\n"
            "The engineer has drafted the following candidate code blocks using the @dlt.table decorator and needs to identify which ones correctly define a streaming DLT table.\n\n"
            "Which of the following code blocks correctly creates a streaming table named clickstream_events?"
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
            "Which of the following commands can a data engineer use to purge stale data files of a Delta table?"
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
            "In Delta Lake tables, which of the following is the primary format for the transaction log files?"
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
            "\"One of the foundational technologies provided by the Databricks Intelligence Platform is an open-source, file-based storage format that brings reliability to data lakes\"\n\n"
            "Which of the following technologies is being described in the above statement?"
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
            "A data engineer has defined the following data quality constraint in a Delta Live Tables pipeline:\n\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ______\n\n"
            "Fill in the above blank so records violating this constraint will be dropped, and reported in metrics"
        ),
        "options": [
            "ON VIOLATION DROP ROW",
            "ON VIOLATION DELETE ROW",
            "There is no need to add ON VIOLATION clause. By default, records violating the constraint will be discarded, and reported as invalid in the event log",
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
            "What is the primary purpose of the targets section in a Databricks Asset Bundle's databricks.yml file?"
        ),
        "options": [
            "To list the external libraries required by the bundle",
            "To specify different deployment environments with their respective configurations",
            "To define user roles and access policies for the workspace",
            "To configure the version of the Databricks Runtime"
        ],
        "answer": "To specify different deployment environments with their respective configurations",
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
            "In the Medallion Architecture, which of the following statements best describes the Gold layer tables?"
        ),
        "options": [
            "They provide business-level aggregations that power analytics, machine learning, and production applications",
            "The table structure in this layer resembles that of the source system table structure with any additional metadata columns like the load time, and input file name.",
            "They represent a filtered, cleaned, and enriched version of data",
            "They maintain raw data ingested from various sources"
        ],
        "answer": "They provide business-level aggregations that power analytics, machine learning, and production applications",
        "explanation": (
            "La Gold layer contiene agregaciones a nivel de negocio preparadas para reporting, BI y machine learning.\n"
            "Es la capa final consumida por usuarios y aplicaciones."
        ),
    },
    {
        "exam": 2,
        "id": "q37_left_join_students_enrollments",
        "question": (
            "Given the following 2 tables:\n\n"
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
            "Fill in the blank to make the following query return the below result:\n\n"
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
            "Fill in the following blank to increment the number of courses by 1 for each student in the array column students:\n\n"
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
            "A data engineer wants to create a relational object by pulling data from two tables. The relational object will only be used in the current session. "
            "In order to save on storage costs, the data engineer wants to avoid copying and storing physical data.\n\n"
            "Which of the following relational objects should the data engineer create?"
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
            "The data engineering team has a Delta table called products that contains products’ details including the net price.\n\n"
            "Which of the following code blocks will apply a 50% discount on all the products where the price is greater than 1000 and save the new price to the table?"
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
            "A data engineer at an HR analytics company is developing a PySpark pipeline to analyze salary metrics across departments. "
            "They wrote the following line of code to compute the total, average, and count of salaries per department:\n\n"
            "result_df = df.groupBy(\"department\").agg({\"salary\": \"sum\", \"salary\": \"avg\", \"salary\": \"count\"})\n\n"
            "After running the code, they observed that the resulting DataFrame only contains one aggregated value instead of the three expected metrics.\n\n"
            "What is the most probable cause of this issue?"
        ),
        "options": [
            "The agg() method must use a list of tuples rather than a dictionary when aggregating multiple functions.",
            "Python dictionaries do not allow duplicate keys, so only the last aggregation is applied.",
            "The groupBy() method must be preceded by a select() method for columns used in aggregation.",
            "The agg() method only supports one aggregation function at a time."
        ],
        "answer": "Python dictionaries do not allow duplicate keys, so only the last aggregation is applied.",
        "explanation": (
            "En Python, un diccionario no puede tener claves duplicadas.\n"
            "Al repetir la clave 'salary', cada entrada sobrescribe la anterior y solo se ejecuta la última agregación (count)."
        ),
    },
    {
        "exam": 2,
        "id": "q42_audit_log_json_format",
        "question": (
            "Which of the following represents a correct example of an audit log in Databricks for a createMetastoreAssignment event?"
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
            "Given the following Structured Streaming query:\n\n"
            "(spark.table(\"orders\")\n"
            "    .withColumn(\"total_after_tax\", col(\"total\")+col(\"tax\"))\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .outputMode(\"append\")\n"
            "    .__________\n"
            "    .table(\"new_orders\") )\n\n"
            "Fill in the blank to make the query executes multiple micro-batches to process all available data, then stops the trigger."
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
            "During the setup of Delta Sharing with an external partner, a data engineer asks the partner for their sharing identifier.\n\n"
            "Which of the following best describes the sharing identifier within the context of Databricks-to-Databricks Sharing?"
        ),
        "options": [
            "It acts as the authentication token for API calls with the recipient's endpoint",
            "It provides a unique reference for the recipient’s Unity Catalog metastore",
            "It serves as a public encryption key used during data writes to the partner’s tables",
            "It identify the partner’s network IP address for firewall whitelisting"
        ],
        "answer": "It provides a unique reference for the recipient’s Unity Catalog metastore",
        "explanation": (
            "El sharing identifier es un identificador único del metastore de Unity Catalog del receptor.\n"
            "Permite al proveedor conceder acceso a los datos compartidos en Databricks-to-Databricks Delta Sharing."
        ),
    },
    {
        "exam": 2,
        "id": "q45_liquid_clustering_multi_column_filters",
        "question": (
            "A data engineering team is working on a user activity events table stored in Unity Catalog. Queries often involve filters on multiple columns like user_id and event_date.\n\n"
            "Which data layout technique should the team implement to avoid expensive table scans?"
        ),
        "options": [
            "Use partitioning on the event_date column.",
            "Use liquid clustering on the combination of user_id and event_date",
            "Use partitioning on the user_id column, along with Z-order indexing on the event_date column.",
            "Use Z-order indexing on the user_id"
        ],
        "answer": "Use liquid clustering on the combination of user_id and event_date",
        "explanation": (
            "Liquid clustering optimiza progresivamente el layout físico basado en múltiples columnas de filtrado.\n"
            "Permite data skipping eficiente cuando se filtra por user_id y event_date evitando full table scans."
        ),
    },
    {
        "exam": 3,
        "id": "q01_cloudflare_r2_delta_sharing_egress",
        "question": (
            "An organization plans to use Delta Sharing for enabling large dataset access by multiple clients across AWS, Azure, and GCP. "
            "A senior data engineer has recommended migrating the dataset to Cloudflare R2 object storage prior to initiating the data sharing process.\n\n"
            "Which benefit does Cloudflare R2 offer in this Delta Sharing setup?"
        ),
        "options": [
            "Eliminates cloud provider egress cost for outbound data transfers",
            "Provides standard API to avoid cloud vendor lock-in",
            "Provides native support for dynamic data masking",
            "Offer built-in support for streaming data with automatic checkpointing"
        ],
        "answer": "Eliminates cloud provider egress cost for outbound data transfers",
        "explanation": (
            "Cloudflare R2 elimina los costes de egress, reduciendo significativamente el coste al compartir datos entre clouds mediante Delta Sharing."
        ),
    },
]