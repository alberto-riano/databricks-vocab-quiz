DATABRICKS_PROFESSIONAL_QUIZ = [
    # ---------------- EXAM 1 ----------------

    {
        "exam": 1,
        "id": "q01_delta_partition_candidate_activity_date",
        "question": (
            "A data engineer wants to create a Delta Lake table for storing user activities of a website. "
            "The table has the following schema:\n\n"
            "user_id LONG, page STRING, activity_type LONG, ip_address STRING, activity_time TIMESTAMP,\n"
            "activity_date DATE\n\n"
            "Based on the above schema, which column is a good candidate for partitioning the Delta Table?"
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
        "question": "Which of the following is the benefit of Delta Lake File Statistics?",
        "options": [
            "They are leveraged for data skipping when executing selective queries.",
            "They are leveraged for data compression in order to improve Delta Caching.",
            "They are used as checksums to check data corruption in parquet files.",
            "They are leveraged for process time forecasting when executing selective queries."
        ],
        "answer": "They are leveraged for data skipping when executing selective queries.",
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
        "question": "Which statement regarding Delta Lake File Statistics is correct?",
        "options": [
            "By default, Delta Lake captures statistics in the transaction log on the first 16 columns of each table.",
            "By default, Delta Lake captures statistics in the transaction log on the first 32 columns of each table.",
            "By default, Delta Lake captures statistics in the Hive metastore on the first 32 columns of each table.",
            "By default, Delta Lake captures statistics in the Hive metastore on the first 16 columns of each table."
        ],
        "answer": "By default, Delta Lake captures statistics in the transaction log on the first 32 columns of each table.",
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
        "question": "The data engineering team has a singleplex bronze table called ‘orders_raw’ where new orders data is appended every night. They created a new Silver table called ‘orders_cleaned’ in order to provide a more refined view of the orders data.\n\nThe team wants to create a batch processing pipeline to process all new records inserted in the orders_raw table and propagate them to the orders_cleaned table.\n\nWhich solution minimizes the compute costs to propagate this batch of data?",
        "options": [
            "Use time travel capabilities in Delta Lake to compare the latest version of orders_raw with one version prior, then write the difference to the orders_cleaned table.",
            "Use Spark Structured Streaming's foreachBatch logic to process the new records from orders_raw using trigger(processingTime=\"24 hours\")",
            "Use batch overwrite logic to reprocess all records in orders_raw and overwrite the orders_cleaned table",
            "Use Spark Structured Streaming to process the new records from orders_raw in batch mode using the trigger availableNow option"
        ],
        "answer": "Use Spark Structured Streaming to process the new records from orders_raw in batch mode using the trigger availableNow option",
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
        "question": "The data engineering team maintains a Type 1 table that is overwritten each night with new data received from the source system.\n\nA junior data engineer has suggested enabling the Change Data Feed (CDF) feature on the table in order to identify those rows that were updated, inserted, or deleted.\n\nWhich response to the junior data engineer's suggestion is correct?",
        "options": [
            "Table’s data changes captured by CDF can only be read in streaming mode",
            "CDF is useful when only a small fraction of records are updated in each batch",
            "CDF can not be enabled on existing tables. It can only be enabled on newly created tables.",
            "CDF is useful when the table is a Slowly Changing Dimension (SCD) of Type 2"
        ],
        "answer": "CDF is useful when only a small fraction of records are updated in each batch",
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
        "question": "The data engineering team has a Delta Lake table created with following query:\n\nCREATE TABLE target\nAS SELECT * FROM source\n\nA data engineer wants to drop the source table with the following query:\n\nDROP TABLE source\n\nWhich statement describes the result of running this drop command ?",
        "options": [
            "Only the source table will be dropped, but the target table will be no more queryable",
            "Only the source table will be dropped, while the target table will not be affected",
            "An error will occur indicating that other tables are based on this source table",
            "Both the target and source tables will be dropped"
        ],
        "answer": "Only the source table will be dropped, while the target table will not be affected",
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
        "question": "A junior data engineer is testing the following code block to get the newest entry for each item added in the ‘sales’ table since the last table update.\n\nfrom pyspark.sql import functions as F\nfrom pyspark.sql.window import Window\n\nwindow = Window.partitionBy(\"item_id\").orderBy(F.col(\"item_time\").desc())\n\nranked_df = (spark.readStream\n    .table(\"sales\")\n    .withColumn(\"rank\", F.rank().over(window))\n    .filter(\"rank == 1\")\n    .drop(\"rank\")\n)\n\ndisplay(ranked_df)\n\nHowever, the command fails when executed.\n\nWhich statement explains the cause of this failure?",
        "options": [
            "Watermarking is missing. It should be added to allow tracking state information for the window of time.",
            "The query output can not be displayed. They should use spark.writeStream to persist the query result.",
            "Non-time-based window operations are not supported on streaming DataFrames. They need to be implemented inside a foreachBatch logic instead.",
            "The item_id field is not unique. Records must be de-duplicated on the item_id using dropDuplicates function"
        ],
        "answer": "Non-time-based window operations are not supported on streaming DataFrames. They need to be implemented inside a foreachBatch logic instead.",
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
            "A scheduled job failed due to an upstream data source issue. After resolving the issue, "
            "the data engineer wants to use the Jobs API to trigger the same job again without waiting "
            "for its next scheduled run.\n\n"
            "Which of the following REST API calls achieves this requirement?"
        ),
        "options": [
            "Send GET request to the endpoint '/api/2.2/jobs/run'",
            "Send POST request to the endpoint '/api/2.2/jobs/run'",
            "Send POST request to the endpoint '/api/2.2/jobs/run-now'",
            "Send POST request to the endpoint '/api/2.2/jobs/start'",
        ],
        "answer": "Send POST request to the endpoint '/api/2.2/jobs/run-now'",
        "explanation": (
            "Sending POST requests to the endpoint '/api/2.2/jobs/run-now' allows you to trigger a job run "
            "using its job_id without waiting for the next scheduled execution.\n"
            "• GET requests are read-only and cannot trigger actions.\n"
            "• '/jobs/run' and '/jobs/start' are not valid Jobs API endpoints."
        ),
    },
    {
        "exam": 2,
        "id": "q02_dynamic_file_pruning",
        "question": "Which of the following statements best describes dynamic file pruning in Apache Spark?",
        "options": [
            "An optimization technique that duplicates data files across worker nodes to improve data locality and query performance.",
            "An optimization technique that dynamically repartitions files into smaller chunks at runtime to balance workload across executors.",
            "An optimization technique that skips reading irrelevant data files during query execution based on runtime filter information.",
            "An optimization technique that automatically compresses large files during Spark job execution to prune storage usage.",
        ],
        "answer": "An optimization technique that skips reading irrelevant data files during query execution based on runtime filter information.",
        "explanation": (
            "Dynamic file pruning is an optimization technique that skips reading irrelevant data files during query execution "
            "by leveraging runtime filter information. Spark avoids scanning files that do not match the query predicates, "
            "thereby improving performance and reducing I/O.\n"
            "• Duplicating files increases storage, not performance.\n"
            "• Repartitioning at runtime describes adaptive query execution, not file pruning.\n"
            "• Compressing files is a storage optimization, unrelated to file pruning."
        ),
    },
    {
        "exam": 2,
        "id": "q03_repair_job_run",
        "question": (
            "A data engineer repaired a failed multi-task job run in Databricks. Before clicking Repair run, "
            "they changed a task parameter value in the Repair run dialog.\n\n"
            "Which of the following best describes the effect of this change?"
        ),
        "options": [
            "The change is ignored because the job parameters always override the run's parameters.",
            "The repair run will fail because this feature only supports adding new parameters, not updating existing ones.",
            "The updated parameter applies only to the current repair run and does not modify the job's stored parameters.",
            "The updated parameter value is permanently saved to the job configuration.",
        ],
        "answer": "The updated parameter applies only to the current repair run and does not modify the job's stored parameters.",
        "explanation": (
            "When using 'Repair run' for a failed job, the dialog allows tweaking parameters for that specific run. "
            "These changes do not overwrite the job's original configuration — they only apply to this repair run. "
            "This is useful for testing a fix without permanently altering the job definition."
        ),
    },
    {
        "exam": 2,
        "id": "q04_retrieve_job_metadata",
        "question": (
            "A data engineer wants to use Databricks REST API to retrieve the metadata of a job run using its run_id.\n\n"
            "Which of the following REST API calls achieves this requirement?"
        ),
        "options": [
            "Send POST request to the endpoint '/api/2.2/jobs/runs/get'",
            "Send GET request to the endpoint '/api/2.2/jobs/runs/get-metadata'",
            "Send GET request to the endpoint '/api/2.2/jobs/runs/get-output'",
            "Send GET request to the endpoint '/api/2.2/jobs/runs/get'",
        ],
        "answer": "Send GET request to the endpoint '/api/2.2/jobs/runs/get'",
        "explanation": (
            "Sending GET requests to the endpoint '/api/2.2/jobs/runs/get' allows retrieving the metadata of a job run "
            "using its run_id.\n"
            "• POST is used to create/trigger actions, not to retrieve data.\n"
            "• '/runs/get-metadata' and '/runs/get-output' are not valid endpoint names."
        ),
    },
    {
        "exam": 2,
        "id": "q05_spark_ui_metrics",
        "question": (
            "A data engineer is analyzing a Spark job via the Spark UI. They have the following summary metrics "
            "for 27 completed tasks in a particular stage:\n\n"
            "Metric         | Min              | 25th pct         | Median           | 75th pct         | Max\n"
            "Duration       | 311 ms           | 311 ms           | 311 ms           | 311 ms           | 311 ms\n"
            "GC Time        | 0 ms             | 0 ms             | 0 ms             | 0 ms             | 0 ms\n"
            "Shuffle Read   | 10.0 MB / 51     | 105.1 MB / 188   | 120.3 MB / 217   | 140.5 MB / 257   | 167.9 MB / 270\n"
            "Shuffle Write  | 9.5 MB / 49      | 101.4 MB / 191   | 115.5 MB / 203   | 138.1 MB / 241   | 160.2 MB / 289\n\n"
            "Which conclusion can the data engineer draw from the above statistics?"
        ),
        "options": [
            "All tasks are operating over partitions with even amounts of data",
            "A number of tasks are operating over near empty partitions",
            "All tasks are operating over near empty partitions",
            "A number of tasks are operating over partitions with larger skewed amounts of data.",
        ],
        "answer": "A number of tasks are operating over near empty partitions",
        "explanation": (
            "If computation were completely symmetric across tasks, all statistics would cluster tightly around the median. "
            "Here, the distribution looks reasonable except for the 'Min' values (10 MB vs. 120 MB median). "
            "This large gap at the minimum suggests a subset of tasks are processing near-empty partitions, "
            "not that all tasks have the problem."
        ),
    },
    {
        "exam": 2,
        "id": "q06_dynamic_reference_timezone",
        "question": (
            "A data engineer uses the dynamic reference {{job.start_time_iso_datetime}} to configure the value "
            "of a task parameter in a job.\n\n"
            "Which of the following statements correctly describes the timezone of the returned timestamp?"
        ),
        "options": [
            "The timestamp is in UTC.",
            "The timestamp is based on the user's local time who triggered the job.",
            "The timestamp is based on the workspace cloud region's local time.",
            "The timestamp is based on the cluster virtual machine's local time.",
        ],
        "answer": "The timestamp is in UTC.",
        "explanation": (
            "In Databricks jobs, all time-based dynamic references — including {{job.start_time_iso_datetime}}, "
            "{{job.start_time}}, and {{run_date}} — are always in UTC. "
            "This ensures consistency across regions, clusters, and users regardless of their local time zones."
        ),
    },
    {
        "exam": 2,
        "id": "q07_liquid_clustering_auto",
        "question": 'Which of the following commands can a data engineer use to create a Delta table "orders" with Automatic Liquid Clustering enabled?',
        "options": [
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY (id, updated_date);",
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY NONE;",
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY AUTO;",
            "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY ALL;",
        ],
        "answer": "CREATE OR REPLACE TABLE orders(id int, updated_date DATE, value double)\nCLUSTER BY AUTO;",
        "explanation": (
            "Automatic Liquid Clustering is enabled with CLUSTER BY AUTO. "
            "Delta automatically manages clustering based on query patterns and data distribution without manually specifying columns.\n"
            "• CLUSTER BY (id, updated_date) manually specifies columns — not automatic.\n"
            "• CLUSTER BY NONE explicitly disables liquid clustering.\n"
            "• CLUSTER BY ALL is not valid Delta Lake syntax."
        ),
    },
    {
        "exam": 2,
        "id": "q08_window_function_tier",
        "question": (
            "A data engineer has a PySpark DataFrame with columns: employee_name, department, and salary. "
            "They want to assign a unique tier to each employee within their department based on salary (descending), "
            "even if they have the same salary.\n\n"
            "Window spec defined as:\nwindow_spec = Window.partitionBy('department').orderBy(col('salary').desc())\n\n"
            "Which of the following functions correctly calculates the tier column?"
        ),
        "options": [
            'df.withColumn("tier", rank().over(window_spec))',
            'df.withColumn("tier", dense_rank().over(window_spec))',
            'df.withColumn("tier", percent_rank().over(window_spec))',
            'df.withColumn("tier", row_number().over(window_spec))',
        ],
        "answer": 'df.withColumn("tier", row_number().over(window_spec))',
        "explanation": (
            "row_number() generates a sequential, unique number for each row within the window, "
            "guaranteeing uniqueness even when multiple employees share the same salary.\n"
            "• rank() assigns the same rank to ties and skips numbers (e.g., 1, 1, 3).\n"
            "• dense_rank() assigns the same rank to ties without skipping (e.g., 1, 1, 2).\n"
            "• percent_rank() returns a value between 0 and 1, not a tier number."
        ),
    },
    {
        "exam": 2,
        "id": "q09_over_partitioned_table",
        "question": (
            "The data engineering team wants to know if the tables they maintain in the Lakehouse are over-partitioned.\n\n"
            "Which of the following is an indicator that a Delta Lake table is over-partitioned?"
        ),
        "options": [
            "If most partitions in the table have more than 1 GB of data",
            "If the data in the table continues to arrive indefinitely",
            "If most partitions in the table have less than 1 GB of data",
            "If the number of partitions in the table are too low",
        ],
        "answer": "If most partitions in the table have less than 1 GB of data",
        "explanation": (
            "Over-partitioned tables suffer significant performance degradation: files cannot be combined across "
            "partition boundaries, increasing storage costs and the number of files to scan.\n"
            "The general guideline is that each partition should contain at least 1 GB of data. "
            "If most partitions are below 1 GB, the table is likely over-partitioned and should be repartitioned or "
            "migrated to Liquid Clustering."
        ),
    },
    {
        "exam": 2,
        "id": "q10_cluster_init_script",
        "question": (
            "A data engineering team wants to ensure that a specific Python library is available every time a cluster starts.\n\n"
            "Which approach best achieves this goal?"
        ),
        "options": [
            "Use an init script to install the library during cluster startup",
            "Install the library when running a Python notebook",
            "Manually install the library on the driver node each time the cluster starts",
            "Use Databricks CLI to upload the library files to the cluster after startup",
        ],
        "answer": "Use an init script to install the library during cluster startup",
        "explanation": (
            "Init scripts run automatically on every node whenever the cluster starts, ensuring the library is "
            "consistently available before any jobs or notebooks execute.\n"
            "• Per-notebook installation only applies to that notebook's session.\n"
            "• Manual installation requires human intervention each restart.\n"
            "• CLI uploads post-startup are error-prone and not guaranteed to run before workloads start."
        ),
    },
    # ---------------- EXAM 3 ----------------
    # TODO: añadir preguntas del examen 3 (EN)
]