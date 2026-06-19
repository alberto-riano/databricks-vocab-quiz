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
    {
        "exam": 2,
        "id": "q11_delta_check_constraint",
        "question": (
            "The data engineering team has a Delta table named 'users'. A recent CHECK constraint has been added to the table using the following command:\n\n"
            "ALTER TABLE users\n"
            "ADD CONSTRAINT valid_age CHECK (age >= 0);\n\n"
            "The team attempted to insert a batch of new records to the table, but there were some records with negative age values which caused the write to fail because of the constraint violation.\n\n"
            "Which statement describes the outcome of this batch insert?"
        ),
        "options": [
            "None of the records have been inserted into the table.",
            "All records except those that violate the table constraint have been inserted in the table. Records violating the constraint have been ignored.",
            "All records except those that violate the table constraint have been inserted in the table. Records violating the constraint have been recorded into the transaction log.",
            "Only records processed before reaching the first violating record have been inserted in the table",
        ],
        "answer": "None of the records have been inserted into the table.",
        "explanation": "Write operations failed because of the constraint violation. However, ACID guarantees on Delta Lake ensure that all transactions are atomic. As told, they will either succeed or fail completely. So in this case, none of these records have been inserted into the table, even the ones that don't violate the constraint.",
    },
    {
        "exam": 2,
        "id": "q12_unit_testing_definition",
        "question": "Which of the following statements correctly describes Unit Testing?",
        "options": [
            "It's an approach to verify if each feature of the application works as per the business requirements.",
            "It's an approach to test individual units of code to determine whether they still work as expected if new changes are made to them in the future",
            "It's an approach to test the interaction between subsystems of an application to ensure that modules work properly as a group.",
            "It's an approach to measure the reliability, speed, scalability, and responsiveness of an application",
        ],
        "answer": "It's an approach to test individual units of code to determine whether they still work as expected if new changes are made to them in the future",
        "explanation": "Unit testing is an approach to testing units of code, such as functions. So, if you make any changes to them in the future, you can use tests to determine whether they still work as you expect them to. Assertions are used in unit tests to check if certain assumptions remain true while you're developing code.",
    },
    {
        "exam": 2,
        "id": "q13_ldp_pipeline_permissions",
        "question": "Which of the following correctly orders the Lakeflow Declarative pipeline permissions from least privilege to most privilege?",
        "options": [
            "CAN VIEW < CAN MANAGE < CAN RUN",
            "CAN RUN < CAN VIEW < CAN MANAGE",
            "CAN VIEW < CAN RUN < CAN MANAGE",
            "CAN MANAGE < CAN VIEW < CAN RUN",
        ],
        "answer": "CAN VIEW < CAN RUN < CAN MANAGE",
        "explanation": "In permission hierarchies, least privilege to most privilege means starting with the minimal access and ending with full control. CAN VIEW allows only viewing pipeline details, Spark UI, and driver logs. CAN RUN allows executing the pipeline but not modifying it. CAN MANAGE allows full control, including executing, editing, deleting, and managing permissions.",
    },
    {
        "exam": 2,
        "id": "q14_ldp_expectation_functions",
        "question": (
            "A junior data engineer has been tasked with implementing data quality validation in a Lakeflow Declarative Pipeline (LDP). They added several expectation functions to ensure that incoming datasets meet certain conditions before being processed further. After the junior engineer submitted a pull request, a senior data engineer began reviewing the code and noticed that one of the function calls used for validation was not valid according to Databricks documentation.\n\n"
            "As part of the review, the senior engineer wants to ensure that all expectation functions used in the pipeline are valid according to Databricks documentation.\n\n"
            "Which of the following function calls is Not a valid expectation function in Lakeflow Declarative Pipelines?"
        ),
        "options": [
            "dlt.expect_or_fail",
            "dlt.expect_or_drop",
            "dlt.expect_or_warn",
            "dlt.expect",
        ],
        "answer": "dlt.expect_or_warn",
        "explanation": "dlt.expect_or_warn is not a supported expectation function in Lakeflow Declarative Pipelines (LDP). LDP supports the following expectation functions: dlt.expect (it writes invalid records to the target bearing semantics), dlt.expect_or_drop (drops invalid rows before writing to the target), and dlt.expect_or_fail (fails the update if violation occurs).",
    },
    {
        "exam": 2,
        "id": "q15_table_partitioning_benefits",
        "question": (
            "The data engineering team has a pipeline that ingest Kafka source data into a Multiples bronze table. This Delta table is partitioned based on the topic and month columns.\n\n"
            "A data engineer notices that the 'user_activity' topic contains Personally Identifiable Information (PII) that needs to be deleted every two months based on the company's Service-Level Agreement (SLA).\n\n"
            "Which statement describes how table partitioning can help to meet this requirement?"
        ),
        "options": [
            "Table partitioning allows delete queries to leverage partition boundaries.",
            "Table partitioning does not allow time travel to the PII data after deletion",
            "Table partitioning allows immediate files deletion without running VACUUM command",
            "Table partitioning reduces query latency when deleting large data files",
        ],
        "answer": "Table partitioning allows delete queries to leverage partition boundaries.",
        "explanation": "Partitioning on datetime columns can be leveraged when removing data older than a certain age from the table. For example, you can decide to delete previous months data. In this case, file deletion will be cleanly along partition boundaries. Similarly, data could be archived and backed up at partition boundaries to a cheaper storage tier. This drives a huge savings on cloud storage.",
    },
    {
        "exam": 2,
        "id": "q16_scd_type_identification",
        "question": (
            "Given the following two versions of a Delta Lake table before and after an update:\n\n"
            "Before:\n"
            "user_id | name | address | city | current | start_date | end_date\n"
            "001 | John | 4, Oxford Street | London | True | 1/1/2022 | NULL\n"
            "002 | Sarah | 99, Victor Hugo Street | Paris | True | 1/1/2022 | NULL\n\n"
            "After:\n"
            "user_id | name | address | city | current | start_date | end_date\n"
            "001 | John | 4, Oxford Street | London | False | 1/1/2022 | 25/1/2023\n"
            "002 | Sarah | 99, Victor Hugo Street | Paris | True | 1/1/2022 | NULL\n"
            "001 | John | 25, King's Road | London | True | 25/1/2023 | NULL\n\n"
            "Which SCD Type is this table?"
        ),
        "options": [
            "SCD Type 0",
            "SCD Type 2",
            "SCD Type 1",
            "It's a combination of Type 0 and Type 2 SCDs",
        ],
        "answer": "SCD Type 2",
        "explanation": "In a Type 2 SCD table, a new record is added with the changed data values, and this new record becomes the current active record, while the old record is marked as no longer active. So, Type 2 SCD retains the full history of values.",
    },
    {
        "exam": 2,
        "id": "q17_ldp_quarantine_pattern",
        "question": (
            "A data engineering team is building a LDP pipeline to clean and validate stream data products streaming from multiple sources. While handling the records in the bronze_products table, products table contain invalid price values, specifically some prices are zero or negative, which violates business rules.\n\n"
            "To handle this issue, they implemented the following LDP code:\n\n"
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
            "Which of the following correctly describes the result of running this pipeline?"
        ),
        "options": [
            'All records are loaded into the silver_products table, with a flag "quarantine_products" indicating whether the price is valid or not.',
            "Records with positive prices are loaded into the silver_products table, while records with zero or negative prices are loaded into the quarantine_products table.",
            'All records are updated in the bronze_products table with a flag "quarantine_products" that indicates whether the price is valid or not.',
            "Records with positive prices are loaded into the silver_products table, while records with zero or negative prices are deleted from the bronze_products table.",
        ],
        "answer": "Records with positive prices are loaded into the silver_products table, while records with zero or negative prices are loaded into the quarantine_products table.",
        "explanation": "This LDP pipeline uses a common pattern for quarantining records by creating a second table that stores the invalid records. The silver_products table only filters for records meeting the condition price > 0, while records with zero or negative prices are selected into the quarantine_products table. The original bronze_products table remains unchanged, and no records are deleted from it.",
    },
    {
        "exam": 2,
        "id": "q18_query_profile_panels",
        "question": "Which of the following panels is Not included in the Query Profile view within Databricks SQL?",
        "options": [
            "Details",
            "Query text",
            "Top operators",
            "Query source",
        ],
        "answer": "Query source",
        "explanation": "The Query Profile provides three panels: Details, Top operators, and Query text, which give insights into query execution metrics, the main operations involved, and the actual SQL code.",
    },
    {
        "exam": 2,
        "id": "q19_databricks_objects_for_analytics",
        "question": (
            "A data analyst at a retail company is responsible for generating daily reports on sales performance across multiple regions and product categories. The company ingests transaction data continuously from its online stores using Lakeflow Declarative Pipelines. The analyst needs to create a relational object that can efficiently precompute business-level aggregations, such as total revenue, average order value, and units sold per category, so that downstream reporting and dashboards can access the data quickly without recalculating it every time.\n\n"
            "Which of the following objects is most suitable for this use case?"
        ),
        "options": [
            "Streaming table",
            "Materialized view",
            "Standard view",
            "Temporary view",
        ],
        "answer": "Materialized view",
        "explanation": "The most suitable object for this use case is a materialized view because it allows the data analyst to precompute and store business-level aggregations, such as total revenue, average order value, and category-level store sales, so that downstream reports and dashboards can access the results quickly without recalculating them every time, unlike a temporary or standard view, which either exist only for the session or require repeated computation, and unlike a streaming table, which is designed for processing raw, real-time event streams rather than pre-aggregated dashboards.",
    },
    {
        "exam": 2,
        "id": "q20_lakehouse_federation",
        "question": (
            "A financial services firm manages highly sensitive client investment portfolios in Oracle databases and maintains transactional market data in Microsoft SQL Server. Due to HIPAA regulations, data must remain in place and not be duplicated or exported unnecessarily. However, internal audit teams need to generate unified reports across both systems in Databricks while maintaining tight access control.\n\n"
            "What solution should the data team use to enable direct querying to these databases without duplicating the data?"
        ),
        "options": [
            "Shallow clone",
            "Lakehouse Federation",
            "Delta Sharing",
            "Partner Connect",
        ],
        "answer": "Lakehouse Federation",
        "explanation": "Lakehouse Federation is a feature in Databricks that enables users to query data in external databases directly, such as Oracle and SQL Server, without the need for data replication, ingestion, or movement. It provides a unified analytics layer on top of multiple data sources and allows for federated queries, where data from various platforms can be combined into a single logical view. This aligns perfectly with the firm's needs.",
    },
    {
        "exam": 2,
        "id": "q21_multi_task_job_creation",
        "question": "Which of the following methods does not allow data engineers to create a multi-task job in Databricks?",
        "options": [
            "Databricks Asset Bundles (DABs)",
            "Lakeflow Declarative Pipelines",
            "REST API",
            "Workspace UI",
        ],
        "answer": "Lakeflow Declarative Pipelines",
        "explanation": "The method that does not allow data engineers to create a multi-task job in Databricks is Lakeflow Declarative Pipelines. Lakeflow Declarative Pipelines are meant for defining transformation logic in a declarative way within a pipeline, and can function as a single task within a job rather than creating multi-task jobs themselves. While the Workspace UI and REST API let you define jobs with multiple tasks, and Databricks Asset Bundles (DABs) can package and deploy multi-task job definitions.",
    },
    {
        "exam": 2,
        "id": "q22_dbutils_secrets_get",
        "question": (
            "A data engineer is working on a project that requires integrating data from an external API endpoint into a Databricks workspace. For security reasons, they decided not to hardcode the API key directly in their notebooks. Instead, they used Databricks Secrets to securely store and manage sensitive credentials, as follows:\n\n"
            "databricks secrets create-scope api_scope\n"
            "databricks secrets put-secret api_scope api_key\n\n"
            "They now want to read the API key in order to external API endpoint from a Databricks notebook.\n\n"
            "Which of the following code lines allows the data engineer to achieve this task?"
        ),
        "options": [
            'api_key = dbutils.secrets.get("api_scope", "api_key")',
            'api_key = dbutils.secrets.get("api_key", "api_scope")',
            'api_key = dbutils.secrets.read("api_scope", "api_key")',
            'api_key = dbutils.secrets.read("api_key", "api_scope")',
        ],
        "answer": 'api_key = dbutils.secrets.get("api_scope", "api_key")',
        "explanation": "dbutils.secrets.get(scope, key) is used to securely retrieve a secret where the first argument is the scope name (api_scope) and the second argument is the secret key (api_key).",
    },
    {
        "exam": 2,
        "id": "q23_streaming_deduplication_issue",
        "question": (
            "A junior data engineer is using the following code to de-duplicate raw streaming data and insert them in a target Delta table:\n\n"
            "(\n"
            "    spark.readStream\n"
            '    .table("raw_data")\n'
            '    .dropDuplicates(["order_id", "order_timestamp"])\n'
            "    .writeStream\n"
            '    .option("checkpointLocation", "/data/checkpoints")\n'
            '    .toTable("orders")\n'
            ")\n\n"
            "A senior data engineer pointed out that this approach is not enough for having distinct records in the target table when there are late-arriving, duplicate records.\n\n"
            "Which of the following could explain the senior data engineer's remark?"
        ),
        "options": [
            "A window function is also needed to apply deduplication for each non-overlapping interval.",
            "The new records also need to be deduplicated against previously inserted data into the table.",
            "Watermarking is also needed to only track state information for a window of time in which we expect records could be delayed.",
            "A ranking function is also needed to ensure processing only the most recent records",
        ],
        "answer": "The new records also need to be deduplicated against previously inserted data into the table.",
        "explanation": "To perform stream deduplication, we use dropDuplicates() function to eliminate duplicate records within each new micro-batch. In addition, we need to ensure that records to be inserted are not already present in the target Delta table. We can achieve this using insert-only merge.",
    },
    {
        "exam": 2,
        "id": "q24_structured_streaming_retry_policy",
        "question": "For production Structured Streaming jobs, which of the following retry policies is recommended to use?",
        "options": [
            "No Retries, with Unlimited Concurrent Runs",
            "No Retries, with 1 Maximum Concurrent Run",
            "Unlimited Retries, with Unlimited Concurrent Runs",
            "Unlimited Retries, with 1 Maximum Concurrent Run",
        ],
        "answer": "Unlimited Retries, with 1 Maximum Concurrent Run",
        "explanation": "In order to restart streaming queries on failure, it's recommended to configure Structured Streaming jobs with the following job configuration: Retries: Set to Unlimited. Maximum concurrent runs: Set to 1. There must be only one instance of each query concurrently active.",
    },
    {
        "exam": 2,
        "id": "q25_autoloader_merge_schema",
        "question": (
            "A data engineer wants to use Auto Loader to ingest input data into a target table, and automatically evolve the schema of the table when new fields are detected.\n\n"
            "They use the query below with spark.readStream:\n\n"
            "spark.readStream\n"
            '    .format("cloudFiles")\n'
            '    .option("cloudFiles.format", "json")\n'
            '    .option("cloudFiles.schemaLocation", checkpointPath)\n'
            "    .load(source_path)\n"
            "    .writeStream\n"
            '    .option("checkpointLocation", checkpointPath)\n'
            "    .option(_________________)\n"
            '    .start("target_table")\n\n'
            "Which option correctly fills in the blank to meet the specified requirement?"
        ),
        "options": [
            'option("cloudFiles.schemaEvolutionMode","addNewColumns")',
            "schema(schema_definition, mergeSchema=True)",
            'option("mergeSchema","True")',
            'option("cloudFiles.mergeSchema","True")',
        ],
        "answer": 'option("mergeSchema","True")',
        "explanation": "Schema evolution is a feature that allows adding new detected fields to the table. It's activated by adding option(\"mergeSchema\",\"True\") to your write or writeStream Spark command.",
    },
    {
        "exam": 2,
        "id": "q26_autoloader_schema_rescue_mode",
        "question": (
            "A data engineer is designing a streaming ingestion pipeline using Auto Loader. "
            "The requirement is that the pipeline should never fail on schema changes but must capture "
            "any new columns that arrive in the data for later inspection.\n\n"
            "Which configuration should the engineer use?"
        ),
        "options": [
            "rescue",
            "none",
            "addNewColumns",
            "failOnNewColumns",
        ],
        "answer": "rescue",
        "explanation": "The 'rescue' mode ensures that the schema does not evolve, so the stream will not fail if new columns are added. Instead, any new columns are stored in the rescued data column, allowing later inspection without interrupting the stream. This meets the requirement to keep the stream running without failures and still capture new schema elements.",
    },
    {
        "exam": 2,
        "id": "q27_cdf_change_data_folder",
        "question": (
            "The data engineering team maintains a Delta Lake table of SCD Type 1. A data engineer noticed a folder named '_change_data' in the table directory, and wants to understand what this folder is used for.\n\n"
            "Which of the following describes the purpose of this folder?"
        ),
        "options": [
            "Optimized Writes feature is enabled on the table. The '_change_data' folder location is where the optimized data is stored",
            "CDF feature is enabled on the table. The '_change_data' folder location is where CDF data is stored",
            "All SCD Type 1 tables have the '_change_data' folder to track the updates applied on the table's data.",
            "The '_change_data' folder is the default directory to track the evolution in schema definition",
        ],
        "answer": "CDF feature is enabled on the table. The '_change_data' folder location is where CDF data is stored",
        "explanation": "Databricks records change data for UPDATE, DELETE, and MERGE operations in the _change_data folder under the table directory. The files in the _change_data folder follow the retention policy of the table. Therefore, if you run the VACUUM command, change data feed data is also deleted.",
    },
    {
        "exam": 2,
        "id": "q28_stream_static_join_behavior",
        "question": (
            "A data engineer has a streaming job that updates a Delta table named 'user_activities' by the results of a join between a streaming Delta table 'activity_logs' and a static Delta table 'users'.\n\n"
            "They noticed that adding new users into the 'users' table does not automatically trigger updates to the 'user_activities' table, even when there were activities for those users in the 'activity_logs' table.\n\n"
            "Which of the following likely explains this issue?"
        ),
        "options": [
            "The static portion of the stream-static join drives this join process only in batch mode.",
            "The users table must be refreshed with REFRESH TABLE command for each microbatch of this join",
            "The streaming portion of this stream-static join drives the join process. Only new data appearing on the streaming side of the join will trigger the processing.",
            "This stream-static join is not stateful by default unless they set the spark configuration delta.statefulStreamStaticJoin to true.",
        ],
        "answer": "The streaming portion of this stream-static join drives the join process. Only new data appearing on the streaming side of the join will trigger the processing.",
        "explanation": "In stream-static join, the streaming portion of this join drives the join process. So, only new data appearing on the streaming side of the join will trigger the processing. While, adding new records into the static table will not automatically trigger updates to the results of the stream-static join.",
    },
    {
        "exam": 2,
        "id": "q29_databricks_run_target_flag",
        "question": (
            "A data engineer is responsible for managing and orchestrating data workflows in their organization's Databricks environment. They have deployed a job called events_process_job using Databricks Asset Bundles. To execute this job, the engineer runs the following command from their terminal:\n\n"
            "databricks bundle run events_process_job\n\n"
            "After observing the command, a senior data engineer suggests that they could improve the execution process by adding the -t option when running the command.\n\n"
            "Which of the following explain the primary purpose of this option?"
        ),
        "options": [
            "To trigger dry run of the job without actually processing data",
            "To enable temporary logging during job execution",
            "To select the target environment for the job run",
            "To specify the target cluster size for the job run",
        ],
        "answer": "To select the target environment for the job run",
        "explanation": "The primary purpose of the -t option in the databricks bundle run command is to select the target environment for the job run. When a data engineer runs a job using Databricks Asset Bundles, the -t (or --target) flag allows them to specify which environment—such as development, staging, or production—the job should execute in. This helps ensure that jobs run against the correct resources and datasets for that environment, avoiding accidental changes in processing in the wrong context, and streamlines deployment workflows across multiple environments.",
    },
    {
        "exam": 2,
        "id": "q30_pandas_udf_apache_arrow",
        "question": "Which of the following formats is used by Pandas UDFs to improve execution performance in Apache Spark?",
        "options": [
            "Apache Iceberg",
            "Apache Arrow",
            "Delta Lake",
            "Apache Kafka",
        ],
        "answer": "Apache Arrow",
        "explanation": "Apache Arrow provides an efficient columnar in-memory data format that allows Spark to transfer data between the JVM and Python processes without serialization overhead. This significantly speeds up data processing compared to standard row-based formats.",
    },
    {
        "exam": 2,
        "id": "q31_delta_sharing_cloudflare_r2",
        "question": "An organization plans to use Delta Sharing for enabling large dataset access by multiple clients across AWS, Azure, and GCP. A senior data engineer has recommended migrating the dataset to Cloudflare R2 object storage prior to initiating the data sharing process.\n\nWhich benefit does Cloudflare R2 offer in this Delta Sharing setup?",
        "options": [
            "Provides standard API to avoid cloud vendor lock-in",
            "Offer built-in support for streaming data with automatic checkpointing",
            "Eliminates cloud provider egress cost for outbound data transfers",
            "Provides native support for dynamic data masking",
        ],
        "answer": "Eliminates cloud provider egress cost for outbound data transfers",
        "explanation": "Cloudflare R2 completely removes egress costs, which drastically minimizes the expenses usually incurred when sharing data across multiple cloud vendors and external analytical teams via the Delta Sharing protocol.",
    },
    {
        "exam": 2,
        "id": "q32_delta_sharing_supported_assets",
        "question": "Which of the following Delta Sharing implementations support sharing Unity Catalog Volumes, Unity Catalog Models, and notebooks in addition to static Delta tables?",
        "options": [
            "Customer-managed implementation of the open source Delta Sharing server",
            "Databricks-to-Databricks sharing protocol",
            "Databricks open sharing protocol",
            "None of the listed options support sharing these assets",
        ],
        "answer": "Databricks-to-Databricks sharing protocol",
        "explanation": "The Databricks-to-Databricks sharing protocol natively supports sharing advanced Unity Catalog assets such as Volumes, Models, and Notebooks. In contrast, the open sharing protocol and customer-managed implementations are restricted to static Delta tables.",
    },
    {
        "exam": 2,
        "id": "q33_sql_grant_all_privileges",
        "question": "Which of the following commands can a data engineer use to grant full permissions to the HR team on the table employees?",
        "options": [
            "GRANT SELECT, MODIFY, CREATE, READ_METADATA ON TABLE employees TO hr_team",
            "GRANT ALL PRIVILEGES ON TABLE employees TO hr_team",
            "GRANT ALL PRIVILEGES ON TABLE hr_team TO employees",
            "GRANT FULL PRIVILEGES ON TABLE employees TO hr_team",
        ],
        "answer": "GRANT ALL PRIVILEGES ON TABLE employees TO hr_team",
        "explanation": "In Databricks SQL, the keyword ALL PRIVILEGES is utilized to grant all available permissions on an object to a user or a group. FULL PRIVILEGES is grammatically incorrect syntax.",
    },
    {
        "exam": 2,
        "id": "q34_ldp_violation_clause_default",
        "question": "A data engineer has defined the following data quality constraint in a LDP pipeline:\n\nCONSTRAINT valid_id EXPECT (id IS NOT NULL) _______________\n\nWhich clause correctly fills in the blank so records violating this constraint will be written to the target table, but reported in metrics?",
        "options": [
            "ON VIOLATION ADD ROW",
            "ON VIOLATION NULL",
            "There is no need to add the ON VIOLATION clause. By default, records violating the constraint will be kept, and reported as invalid in the pipeline metrics.",
            "ON VIOLATION WARNING",
        ],
        "answer": "There is no need to add the ON VIOLATION clause. By default, records violating the constraint will be kept, and reported as invalid in the pipeline metrics.",
        "explanation": "By default, if no additional clause like ON VIOLATION DROP ROW or ON VIOLATION FAIL UPDATE is supplied, an EXPECT constraint records the violation in metrics but safely transmits the row directly to the target destination.",
    },
    {
        "exam": 2,
        "id": "q35_table_partitioning_pii_security",
        "question": "The data engineering team wants to create a multiphase bronze Delta table from a Kafka source. The Delta table has the following schema:\n\nkey BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG\n\nSince the 'value' column contains Personal Identifiable Information (PII) for some topics, the team wants to apply Access Control Lists (ACLs) at partition boundaries to restrict access to this PII data.\n\nBased on the above schema and the specified requirement, which column is a good candidate for partitioning?",
        "options": [
            "key",
            "timestamp",
            "topic",
            "partition",
        ],
        "answer": "topic",
        "explanation": "Partitioning the table on the 'topic' column isolates the relevant records containing PII data into specific directories, enabling granular access control limits and selective management of historical info.",
    },
    {
        "exam": 2,
        "id": "q36_delta_shallow_clone_vacuum",
        "question": "A junior data engineer has created the table 'orders_backup' as a copy of the table 'orders'. Recently, the team started getting an error when querying the orders_backup indicating that some data files are no longer present. The transaction logs for the orders table show a recent run of VACUUM command.\n\nWhich of the following explains how the data engineer created the orders_backup table?",
        "options": [
            "The orders_backup table was created via Delta Lake's SHALLOW CLONE functionality from the orders table",
            "The orders_backup table was created via Delta Lake's DEEP CLONE functionality from the orders table",
            "The orders_backup table was created using CTAS statement from orders table",
            "The orders_backup table was created using CRAS statement from orders table",
        ],
        "answer": "The orders_backup table was created via Delta Lake's SHALLOW CLONE functionality from the orders table",
        "explanation": "A SHALLOW CLONE only copies the Delta transaction logs without duplicating the underlying data files. If a VACUUM operation is executed on the parent source table, historical files referenced by the shallow clone are pruned, resulting in missing data errors when querying the clone.",
    },
    {
        "exam": 2,
        "id": "q37_pyspark_assert_data_frame_equal",
        "question": "A data engineer is testing a transformation pipeline that adds a new column to an existing DataFrame. They want to ensure the resulting DataFrame matches the expected output.\n\nWhich of the following functions can a data engineer use to verify equality?",
        "options": [
            "assertDataFrameEqual(actual_df, expected_df)",
            "assertEqual(actual_df, expected_df)",
            "verifyEquality(actual_df, expected_df)",
            "assert(actual_df == expected_df)",
        ],
        "answer": "assertDataFrameEqual(actual_df, expected_df)",
        "explanation": "In PySpark, assertDataFrameEqual is the native utility function designed to compare schemas and row-level data between two DataFrames within testing frameworks.",
    },
    {
        "exam": 2,
        "id": "q38_merge_into_limitation_multiple_matches",
        "question": "Which of the following is considered a limitation when using the MERGE INTO command?",
        "options": [
            "Merge does not support records deletion. It supports only upsert operations.",
            "Merge can not be performed if single source row matched and attempted to modify the multiple target rows in the table",
            "Merge can not be performed if multiple source rows matched and attempted to modify the same target row in the table",
            "Merge can not be performed in streaming jobs unless it uses Watermarking",
        ],
        "answer": "Merge can not be performed if multiple source rows matched and attempted to modify the same target row in the table",
        "explanation": "A MERGE operation will fail with an error if multiple records from the incoming source match a single row in the target table, as it creates ambiguity regarding which source row should take precedence for the update.",
    },
    {
        "exam": 2,
        "id": "q39_alter_table_file_retention_properties",
        "question": "A data engineering team is managing a Delta table called orders. They want to ensure that they can access the table's historical data using time travel for the same duration as Delta Lake's default transaction log retention.\n\nWhich of the following commands meets this requirement?",
        "options": [
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 365 days")',
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 30 days")',
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 7 days")',
            'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 90 days")',
        ],
        "answer": 'ALTER TABLE orders SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "interval 30 days")',
        "explanation": "By default, Delta log files are preserved for 30 days. To match this timeline and allow time travel lookup on physical rows before permanent cleanup via VACUUM, the delta.deletedFileRetentionDuration configuration property must be set to 30 days.",
    },
    {
        "exam": 2,
        "id": "q40_autoloader_binary_format_glob",
        "question": "A data engineer is tasked with ingesting x-ray image files of type .JPEG into a Delta table using Auto Loader.\n\nWhich of the following code snippets can the data engineer use to achieve this task?",
        "options": [
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'image') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'binaryFile') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'binary') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
            "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'files') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
        ],
        "answer": "df = spark.readStream.format('cloudFiles') \\n  .option('cloudFiles.format', 'binaryFile') \\n  .option('pathGlobFilter', '*.jpeg') \\n  .load('/source/xray')",
        "explanation": "When handling unstructured image data with Auto Loader, the cloudFiles.format option must be set to 'binaryFile'. The pathGlobFilter option extracts only files ending with the specified extension.",
    },
    {
        "exam": 2,
        "id": "q41_drop_table_managed_outcome",
        "question": (
            "The data engineering team has a Delta Lake table created with following query:\n\n"
            "CREATE TABLE customers_clone\n"
            "AS SELECT * FROM customers;\n\n"
            "A data engineer wants to drop the table with the following query:\n\n"
            "DROP TABLE customers_clone;\n\n"
            "Which statement describes the result of running this drop command?"
        ),
        "options": [
            "Only the table's metadata will be deleted from the catalog, while the data files will be kept in the storage.",
            "An error will occur as the table is deep cloned from the customers table",
            "The table will not be dropped until VACUUM command is run",
            "Both the table's metadata and the data files will be deleted",
        ],
        "answer": "Both the table's metadata and the data files will be deleted",
        "explanation": "The table was created without defining an explicit external LOCATION clause, meaning it is registered as a managed table. Dropping a managed table removes both its metadata definition in the catalog and all physical raw data files from underlying storage.",
    },
    {
        "exam": 2,
        "id": "q42_predictive_optimization_benefits",
        "question": "Which two of the following describe benefits of enabling predictive optimization on managed tables in Unity Catalog?\n\nChoose 2 answers.",
        "options": [
            "It boosts data privacy by automatically encrypting data on write and masking sensitive columns.",
            "It enhances query performance by collecting statistics as data is written to the table.",
            "It reduces overall cost by forecasting storage Usage and reallocating data across tiers.",
            "It improves data profiling by automatically predicting missing values in the table columns.",
            "It simplifies maintenance by automatically running maintenance operations on the table.",
        ],
        "answer": [
            "It enhances query performance by collecting statistics as data is written to the table.",
            "It simplifies maintenance by automatically running maintenance operations on the table.",
        ],
        "explanation": "Predictive Optimization automatically executes operational tasks such as OPTIMIZE and VACUUM to ensure optimal storage layout. It also collects data statistics on write, boosting query optimization without data engineering manual labor.",
    },
    {
        "exam": 2,
        "id": "q43_delta_sharing_with_history",
        "question": (
            "A data analyst at a retail company shared a large Delta table with an external analytics company using Delta Sharing without history. However, the company noticed execution delays when querying the shared data.\n\n"
            "A senior data engineer suggested using the following command to share the data with history in order to improve query performance:\n\n"
            "ALTER SHARE sales_share ADD TABLE products WITH HISTORY;\n\n"
            "Which benefit is achieved by using WITH HISTORY?"
        ),
        "options": [
            "It leverages disk caching of the Delta Sharing server, resulting in performance that is comparable to direct access to source tables.",
            "It replicates the table to balance vacancy requests through the Delta Sharing server.",
            "It leverages temporary security credentials from the cloud storage, scoped-down to the root directory of the provider's shared Delta table.",
            "It performs a shallow clone of the table to share only the table's transaction log.",
        ],
        "answer": "It leverages temporary security credentials from the cloud storage, scoped-down to the root directory of the provider's shared Delta table.",
        "explanation": "Sharing a table WITH HISTORY allows recipients to perform time-travel queries. It optimizes execution pathways by enabling Databricks to safely leverage pre-authenticated, scoped-down cloud credentials directly from the provider's underlying object storage layer.",
    },
    {
        "exam": 2,
        "id": "q44_alter_table_row_filter",
        "question": (
            "A data engineering team manages a Delta Lake table in Unity Catalog called employees, with columns id, name, salary, and region. They want to apply row filtering on this table so that only members of the HR team can access all records. If the table is queried by a non-HR team member, it should only show records in the France (FR) region. To achieve this, they implemented the following user-defined function:\n\n"
            "CREATE FUNCTION fn_filter(region STRING)\n"
            "RETURN IS_ACCOUNT_GROUP_MEMBER('hr_team') OR region = 'FR';\n\n"
            "Which of the following commands can the team use to apply this function as a row filter to the table?"
        ),
        "options": [
            "ALTER TABLE employees SET ROW FILTER fn_filter;",
            "ALTER TABLE employees ALTER COLUMN region SET ROW FILTER fn_filter;",
            "SET ROW FILTER fn_filter ON TABLE employees COLUMN region",
            "ALTER TABLE employees SET ROW FILTER fn_filter ON (region);",
        ],
        "answer": "ALTER TABLE employees SET ROW FILTER fn_filter ON (region);",
        "explanation": "The proper SQL syntax to attach an existing row filter function to a target table in Unity Catalog is ALTER TABLE <table_name> SET ROW FILTER <function_name> ON (<column_names>).",
    },
    {
        "exam": 2,
        "id": "q45_cdf_batch_read_behavior",
        "question": (
            "Given the following query on the Delta table 'customers' on which Change Data Feed is enabled:\n\n"
            "spark.read\n"
            '  .option("readChangeFeed", "true")\n'
            '  .option("startingVersion", 1)\n'
            '  .table("customers")\n'
            "  .write\n"
            '  .option("append")\n'
            '  .saveAsTable("customers_orders")\n\n'
            "Which statement describes the result of this query each time it is executed?"
        ),
        "options": [
            "The entire history of updated records will overwrite the target table at each execution.",
            "Newly updated records will overwrite the target table.",
            "Newly updated records will be appended to the target table.",
            "The entire history of updated records will be appended to the target table at each execution, which leads to duplicate entries.",
        ],
        "answer": "The entire history of updated records will be appended to the target table at each execution, which leads to duplicate entries.",
        "explanation": "Because this is executed as a batch read (spark.read) with a fixed startingVersion of 1 rather than a streaming read, every single execution scans and processes the entire historical ledger of changes from version 1 onward, continuously appending redundant records to the target table.",
    },
    
    {
        "exam": 2,
        "id": "q46_window_tumbling_streaming",
        "question": (
            "A data engineer has the following streaming query with a blank:\n\n"
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
            "They want to calculate the orders count and average quantity for each non-overlapping 15-minute interval.\n\n"
            "Which option correctly fills in the blank to meet this requirement?"
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
        "question": "A data engineering team is working on a user activity events table stored in Unity Catalog. Queries often involve filters on multiple columns like user_id and event_date.\n\nWhich data layout technique should the team implement to avoid expensive table scans?",
        "options": [
            "Use partitioning on the event_date column.",
            "Use Z-order indexing on the user_id.",
            "Use partitioning on the user_id column, along with Z-order indexing on the event_date column.",
            "Use liquid clustering on the combination of user_id and event_date.",
        ],
        "answer": "Use liquid clustering on the combination of user_id and event_date.",
        "explanation": "Liquid Clustering es la técnica moderna de optimización del diseño de datos en Databricks que reemplaza al particionamiento clásico y a Z-Order. Permite realizar agrupaciones de datos dinámicas y eficientes basadas en múltiples columnas (como combinaciones de alta y baja cardinalidad), evitando escaneos completos de tabla innecesarios y facilitando la optimización incremental.",
    },
    {
        "exam": 2,
        "id": "q48_ctas_statement_delta",
        "question": (
            "A data engineer has the following CTAS statement in a SQL notebook attached to an all-purpose cluster:\n\n"
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
            "Which statement describes the resulting course_students table?"
        ),
        "options": [
            "It's a session scoped table. The SELECT statement will be executed at the table creation, but its output will be stored in the cache of the current active Spark session.",
            "It's a Delta Lake table. The SELECT statement will be executed at the table creation, and its output will be stored in Delta format on the underlying storage.",
            "It's a virtual table that has no physical data. The SELECT statement will be executed each time the course_students table is queried.",
            "It's a cluster-scoped table. The SELECT statement will be executed at the table creation, but its output will be stored in the memory of the currently active cluster.",
        ],
        "answer": "It's a Delta Lake table. The SELECT statement will be executed at the table creation, and its output will be stored in Delta format on the underlying storage.",
        "explanation": "En Databricks SQL, las sentencias CREATE TABLE AS SELECT (CTAS) crean por defecto tablas persistentes basadas en el formato estructurado Delta Lake. Al ejecutarse la instrucción, la consulta SELECT se procesa por completo para guardar físicamente el resultado en el almacenamiento subyacente asignado.",
    },
    {
        "exam": 2,
        "id": "q49_delta_deletion_vectors",
        "question": "Which of the following statements best describes deletion vectors in Delta Lake?",
        "options": [
            "Metadata structures that track which rows in a data file have been logically deleted without physically rewriting the file.",
            "Temporary files that store deleted rows until they are archived in a separate table partition called \"_deletion_log\".",
            "Data structures that permanently removed deleted rows from all data files in a Delta Lake table.",
            "Indexes that accelerate queries on deleted rows by storing their physical locations directly in Unity Catalog volumes.",
        ],
        "answer": "Metadata structures that track which rows in a data file have been logically deleted without physically rewriting the file.",
        "explanation": "Los deletion vectors son estructuras de metadatos integradas en Delta Lake que registran de forma lógica las filas eliminadas o modificadas dentro de un archivo de datos de tipo Parquet. Esto optimiza el rendimiento al evitar la necesidad inmediata de reescribir físicamente todo el archivo durante operaciones UPDATE, DELETE o MERGE.",
    },
    {
        "exam": 2,
        "id": "q50_multitask_job_partial_failure",
        "question": "Given a multi-task job where Task 2 and Task 3 depend on Task 1:\n\nIf there is an error in the notebook associated with Task 1, which statement describes the run result of this job?",
        "options": [
            "Task 1 will completely fail. Tasks 2 and 3 will be skipped",
            "Task 1 will completely fail. Tasks 2 and 3 will run and succeed",
            "Task 1 will partially fail. Tasks 2 and 3 will run and succeed",
            "Task 1 will partially fail. Tasks 2 and 3 will be skipped",
        ],
        "answer": "Task 1 will partially fail. Tasks 2 and 3 will be skipped",
        "explanation": "En Databricks Workflows, el fallo de una tarea basada en un notebook se cataloga como un fallo parcial debido a que las celdas y operaciones previas al error se completan y confirman correctamente en el almacenamiento. No obstante, por integridad del flujo secuencial, todas las tareas dependientes (Tareas 2 y 3) se omitirán automáticamente (skipped).",
    },
    {
        "exam": 2,
        "id": "q51_streaming_default_trigger",
        "question": (
            "Given the following Structured Streaming query:\n\n"
            "spark.readStream\n"
            "    .table(\"orders\")\n"
            "    .writeStream\n"
            "    .option(\"checkpointLocation\", checkpointPath)\n"
            "    .table(\"output_table\")\n\n"
            "Which of the following is the trigger interval for this query?"
        ),
        "options": [
            "The query will run in batch mode to process all available data at once, then the trigger stops.",
            "Every half min",
            "Every half hour",
            "Every half second",
        ],
        "answer": "Every half second",
        "explanation": "Por defecto, si no se define explícitamente ninguna política de Trigger en una consulta de Structured Streaming en Apache Spark, el motor procesa micro-lotes de forma continua con un tiempo de espera de procesamiento mínimo de 500 milisegundos, lo que equivale a procesar datos 'cada medio segundo'.",
    },
    {
        "exam": 2,
        "id": "q52_change_data_feed_cdc",
        "question": "Which of the following best describes this feature?\n\n\"A feature built into Delta Lake that allows to automatically generate CDC feeds about Delta Lake tables\"",
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
            "A data engineer is using a foreachBatch logic to upsert data in a target Delta table.\n\n"
            "The function to be called at each new microbatch processing is displayed below with a blank:\n\n"
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
            "Which option correctly fills in the blank to execute the sql query in the function on a cluster with recent Databricks Runtime above 10.5?"
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
            "A data engineer wants to optimize the following join operation by allowing the smaller dataFrame to be sent to all executor nodes in the cluster:\n\n"
            "target_df = left_df.join(right_df, \"user_id\")\n\n"
            "Which of the following functions can be used to mark a dataFrame as small enough to fit in memory on all executors?"
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
        "question": "Which of the following statements regarding the retention policy of Delta Lake CDF is correct?",
        "options": [
            "CDF data files can be purged by running VACUUM CHANGES command",
            "Running the VACUUM command on the table does not deletes CDF data unless CASCADE clause is set to true",
            "Running the VACUUM command on the table deletes CDF data as well",
            "Running the VACUUM command on the table does not deletes CDF data",
        ],
        "answer": "Running the VACUUM command on the table deletes CDF data as well",
        "explanation": "Los archivos generados por Change Data Feed (CDF) se almacenan en el directorio interno '_change_data' dentro de la estructura de la tabla Delta. Al ejecutar el comando convencional VACUUM sobre la tabla, los archivos CDF que queden fuera del periodo de retención configurado también se purgarán de manera automática junto con los archivos de datos obsoletos.",
    },
    {
        "exam": 2,
        "id": "q56_delta_sharing_cross_cloud",
        "question": "A retail company stores sales data in Delta tables within Databricks Unity Catalog. They need to securely share specific tables with an external auditing firm, who uses Databricks on a different cloud provider.\n\nWhich of the following options enable achieving this task without data replication?",
        "options": [
            "External schema in Unity Catalog",
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
            "A data engineer wants to store passwords securely in a Unity Catalog managed table. They need to hash user passwords using sha2(password, 256) before storing them. To ensure proper storage, the engineer must also set a constraint on the column length to accommodate the full hash value.\n\n"
            "The engineer tests hashing the passwords \"sparkV23\" and \"ApacheSpark117\".\n\n"
            "What will the engineer notice about the resulting hash length?"
        ),
        "options": [
            "The hash of \"sparkV23\" will be shorter than the hash of \"ApacheSpark117\"",
            "Both hashes will have the same length because hash length depends on the number of numeric characters",
            "The hash of \"ApacheSpark117\" will be shorter than the hash of \"sparkV23\"",
            "Both hashes will have the same length, regardless of input size",
        ],
        "answer": "Both hashes will have the same length, regardless of input size",
        "explanation": "Las funciones criptográficas de la familia SHA-256 de longitud fija (empleadas con sha2(..., 256)) generan siempre un resultado final con un tamaño constante de 256 bits (representado comúnmente en formato de cadena de texto hexadecimal fija de 64 caracteres), con total independencia de las dimensiones o volumen del texto original que se introduzca en la entrada.",
    },
    {
        "exam": 2,
        "id": "q58_cron_syntax_job_scheduling",
        "question": "Which of the following describes Cron syntax in Databricks Jobs?",
        "options": [
            "It's an expression to represent complex job schedule that can be defined programmatically",
            "It's an expression to represent the maximum concurrent runs of a job",
            "It's an expression to represent the run timeout of a job",
            "It's an expression to represent the retry policy of a job",
        ],
        "answer": "It's an expression to represent complex job schedule that can be defined programmatically",
        "explanation": "La sintaxis estándar Cron dentro de Databricks Workflows se utiliza como mecanismo formal para modelar programáticamente planificaciones y esquemas de ejecución de tareas complejas e intermitentes (por ejemplo, definir ejecuciones repetitivas en periodos horarios alternos o días específicos de la semana).",
    },
    {
        "exam": 2,
        "id": "q59_minimal_permissions_attach_notebook",
        "question": "Which of the following describes the minimal permissions a data engineer needs to start an existing cluster, and attach a notebook to it?",
        "options": [
            "\"Can Manage\" privilege on the cluster",
            "\"Can Attach To\" privilege on the cluster",
            "\"Can Restart\" privilege on the cluster",
            "Cluster creation allowed + \"Can Restart\" privileges on the cluster",
        ],
        "answer": "\"Can Restart\" privilege on the cluster",
        "explanation": "El privilegio o permiso 'Can Restart' (Puede reiniciar) engloba y hereda implícitamente todas las capacidades básicas de nivel inferior como 'Can Attach To' (Puede asociar a). En consecuencia, para arrancar un clúster ya existente que se encuentra detenido y vincular un notebook con el fin de trabajar, el permiso mínimo necesario e indispensable es 'Can Restart'.",
    },


    # ---------------- EXAM 3 ----------------
    {
        "exam": 3,
        "id": "q01_dlt_expect_or_drop",
        "question": (
            "A data engineer defines the following function in their LDP pipeline:\n\n"
            "@dlt.table\n"
            '@dlt.expect_or_drop("quantity_within_range", "quantity BETWEEN 0 AND 1000")\n'
            '@dlt.expect_or_drop("recent_transaction", "transaction_date >= \'2025-01-01\'")\n'
            '@dlt.expect_or_drop("valid_transaction", "transaction_id IS NOT NULL")\n'
            "def silver_sales():\n"
            '    return dlt.read_stream("bronze_sales")\n\n'
            "Which of the following correctly describes the result of running this pipeline?"
        ),
        "options": [
            "Rows that violate the defined expectations are deleted from both tables.",
            "Rows that violate the defined expectations are filtered out, and only valid rows are written to silver_sales.",
            "Rows that violate the defined expectations are deleted from the bronze_sales table.",
            "Rows that violate the defined expectations are streamed into the silver_sales table."
        ],
        "answer": "Rows that violate the defined expectations are filtered out, and only valid rows are written to silver_sales.",
        "explanation": (
            "The expect_or_drop function is a data quality enforcement rule in LOP (previously known as DLT).\n"
            "The expect part defines the quality constraint (e.g., 'quantity BETWEEN 0 AND 1000').\n"
            "The or_drop part dictates the action to take when the expectation is violated: meaning the violating row is discarded (filtered out) and will not be written to the target table (silver_sales).\n\n"
            "In this example, only rows that successfully pass all three defined expectations (quantity_within_range, recent_transaction, and valid_transaction) will be included in the silver_sales table. Rows failing any of them are discarded."
        ),
    },
    {
        "exam": 3,
        "id": "q02_multitask_job_notifications",
        "question": (
            "A data engineering team manages a multi-task job where each task may be retried multiple times. They noticed that job notifications are not sent when failed tasks are retried.\n\n"
            "Which of the following configurations will ensure that a failure notification is received for every failed task?"
        ),
        "options": [
            "Implement custom notification logic within each task",
            "Create a separate job for each task with job-level retries",
            "Disable all task retries to rely on job-level notifications",
            "Use task-level notifications in the job definition"
        ],
        "answer": "Use task-level notifications in the job definition",
        "explanation": (
            "In a multi-task job, notifications can be configured at two levels:\n\n"
            "1. Job-level notifications: Trigger only when the entire job succeeds or fails. This means if an individual task fails but is retried successfully, no notification is sent until the overall job completes or fails.\n"
            "2. Task-level notifications: Trigger for each task event, including failures, or successful completions.\n\n"
            "Configuring task-level notifications ensures a notification is sent for every failed task, even if it's later retried."
        ),
    },
    {
        "exam": 3,
        "id": "q03_delta_sharing_external_vendor",
        "question": (
            "A data engineer from a global logistics company needs to share specific datasets and analysis notebooks with an external analytics vendor, who is a Databricks client. The data is stored as Delta tables in Unity Catalog, and the vendor does not have access to the company Databricks account.\n\n"
            "What is the most effective and secure way to share the data and notebooks with the external vendor?"
        ),
        "options": [
            "Share the Delta tables using Delta Sharing, and send all the notebooks together in a single DBC file.",
            "Share the Delta tables using Delta Sharing, and publish the notebooks as HTML pages programmatically.",
            "Share the Delta tables and notebooks using Delta Sharing.",
            "Share the Delta tables using Delta Sharing, and grant access to each notebook via its built-in collaboration feature."
        ],
        "answer": "Share the Delta tables and notebooks using Delta Sharing.",
        "explanation": (
            "Databricks-to-Databricks Delta Sharing enables secure, open, and real-time sharing of tables, notebooks, volumes, and ML Models with other Databricks clients. This does not require them to have access to the same Databricks account or workspace. With Unity Catalog, the company can ensure fine-grained access control and governance. This approach is efficient, scalable, and adheres to enterprise-grade security standards."
        ),
    },
    {
        "exam": 3,
        "id": "q04_dabs_github_authentication",
        "question": (
            "A data engineer wants to use Databricks Asset Bundles (DABs) in a fully automated CI/CD pipeline on GitHub.\n\n"
            "What is the recommended method for authenticating DABs to the target Databricks workspace in this scenario?"
        ),
        "options": [
            "Personal Access Token for a Databricks service principal",
            "OAuth client secret for a Databricks service principal",
            "OAuth token federation for a Databricks service principal",
            "Personal Access Token for an administrator user"
        ],
        "answer": "OAuth token federation for a Databricks service principal",
        "explanation": (
            "Databricks Asset Bundles are a feature of the Databricks CLI. To enable the CLI to authenticate to Databricks without managing Databricks secrets, it's recommended to use OAuth token federation for a Databricks service principal in the target workspace."
        ),
    },
    {
        "exam": 3,
        "id": "q05_rest_api_jobs_list",
        "question": (
            "A data engineering team wants to automate job monitoring and improve observability by retrieving available jobs in the production Databricks workspace using REST API.\n\n"
            "Which of the following REST API calls achieves this requirement?"
        ),
        "options": [
            "Send POST request to the endpoint '/api/2.0/jobs/list'",
            "Send POST request to the endpoint '/api/2.1/jobs/list'",
            "Send GET request to the endpoint '/api/2.0/jobs/list'",
            "Send GET request to the endpoint '/api/2.1/jobs/list'"
        ],
        "answer": "Send GET request to the endpoint '/api/2.1/jobs/list'",
        "explanation": (
            "Sending GET requests to the endpoint '/api/2.1/jobs/list' allows you to retrieve available jobs in a Databricks workspace."
        ),
    },
    {
        "exam": 3,
        "id": "q06_databricks_cli_jobs_list_runs",
        "question": (
            "Which of the following Databricks CLI commands allows a data engineer to list all runs of a job that started at or after a specific time?"
        ),
        "options": [
            "databricks jobs list-runs --job-id <job-id> --time-from <time-value>",
            "databricks jobs list-runs --job-id <job-id> --start <time-value>",
            "databricks jobs list-runs --job-id <job-id> --start-time <time-value>",
            "databricks jobs list-runs --job-id <job-id> --start-time-from <time-value>"
        ],
        "answer": "databricks jobs list-runs --job-id <job-id> --start-time-from <time-value>",
        "explanation": (
            "The correct Databricks CLI command that allows a data engineer to list all runs of a job that started at or after a specific time is: databricks jobs list-runs --job-id <job-id> --start-time-from <time-value>.\n\n"
            "'--start-time-from' is the proper parameter used to filter job runs based on their start time in the Databricks CLI."
        ),
    },
    {
        "exam": 3,
        "id": "q07_trigger_multitask_job_tools",
        "question": "Which of the following tools does not allow data engineers to programmatically trigger a multi-task job run?",
        "options": [
            "Command-line interface (CLI)",
            "Workspace Jobs UI",
            "REST API",
            "Databricks SDKs"
        ],
        "answer": "Workspace Jobs UI",
        "explanation": (
            "The Workspace Jobs UI does not allow data engineers to programmatically trigger a multi-task job run. It's a graphical interface that requires manual interaction and cannot be used for automated or code-based job execution.\n\n"
            "While the REST API, Command-line interface (CLI), and Databricks SDKs all provide programmatic ways to run jobs."
        ),
    },
    {
        "exam": 3,
        "id": "q08_databricks_secrets_plain_text",
        "question": (
            "A data engineer has heard recently that users who have access to Databricks Secrets could be able to display the values of secrets in notebooks.\n\n"
            "Which of the following could be a workaround to print the value of a Databricks secret in plain text?"
        ),
        "options": [
            'db_password = dbutils.secrets.get("prod-scope", "db-password")\ndisplay(db_password)',
            'db_password = dbutils.secrets.get("prod-scope", "db-password")\nprint(db_password, redacted=False)',
            'db_password = dbutils.secrets.get("prod-scope", "db-password", redacted=False)\nprint(db_password)',
            'db_password = dbutils.secrets.get("prod-scope", "db-password")\nfor char in db_password:\n    print(char)'
        ],
        "answer": 'db_password = dbutils.secrets.get("prod-scope", "db-password")\nfor char in db_password:\n    print(char)',
        "explanation": (
            "Databricks redacts secret values that are read using dbutils.secrets.get(). When displayed in notebook cell output, the secret values are replaced with [REDACTED] string.\n\n"
            "However, there is a workaround to print the values of Databricks secrets in plain text by iterating through the secret and printing each character."
        ),
    },
    {
        "exam": 3,
        "id": "q09_auto_compaction_zorder",
        "question": (
            "A data engineer is using the following spark configurations in a pipeline to enable Optimized Writes and Auto Compaction:\n\n"
            'spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)\n'
            'spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)\n\n'
            "They also want to enable Z-order indexing with Auto Compaction to leverage data skipping on all the pipeline's tables.\n\n"
            "Which of the following solutions allows the data engineer to complete this task ?"
        ),
        "options": [
            "There is no way to enable Z-order indexing with Auto Compaction since it does not support Z-Ordering",
            'Use spark.conf.set("spark.databricks.delta.autoZorder.enabled", True)',
            'Use spark.conf.set("spark.databricks.delta.autoCompact.zorder.enabled", True)',
            "Z-order indexing with Auto Compaction can only be enabled on each table separately using:\n\nALTER TABLE table_name\nSET TBLPROPERTIES (delta.autoOptimize.zorder.enabled = true)"
        ],
        "answer": "There is no way to enable Z-order indexing with Auto Compaction since it does not support Z-Ordering",
        "explanation": (
            "Auto Compaction does not support Z-Ordering as Z-Ordering is significantly more expensive than just compaction."
        ),
    },
    {
        "exam": 3,
        "id": "q10_lakehouse_declarative_pipelines_cdc",
        "question": (
            "A data engineer uses the following code to process CDC data in Lakehouse Declarative Pipelines:\n\n"
            "CREATE OR REFRESH STREAMING TABLE cdc_target;\n\n"
            "APPLY CHANGES INTO LIVE.cdc_target\n"
            "FROM stream(users_cdc)\n"
            "KEYS (user_id)\n"
            "APPLY AS DELETE WHEN operation = 'DELETE'\n"
            "SEQUENCE BY sequenceNum\n"
            "COLUMNS * ;\n\n"
            "After running this code, the data engineer noticed that two objects were created the metastore in addition to the users_target table:\n\n"
            "* A view named users_target.\n"
            "* A table named __apply_changes_storage_users_target.\n\n"
            "Which of the following correctly explains the purpose of these objects?"
        ),
        "options": [
            "The view users_target is a materialized snapshot of the raw data, while the table __apply_changes_storage_users_target stores user activity logs for auditing purposes.",
            "The view users_target and the table __apply_changes_storage_users_target are temporary objects created for write optimization and are deleted immediately after the pipeline runs.",
            "These objects are used for internal CDC processing leveraging sequence_by, along with extra information such as tombstones and versions required to handle out-of-order data.",
            "The users_target view is a virtual index on the target table to speed up queries, and __apply_changes_storage_users_target is a backup of the original users_target index."
        ],
        "answer": "These objects are used for internal CDC processing leveraging sequence_by, along with extra information such as tombstones and versions required to handle out-of-order data.",
        "explanation": (
            "In Lakehouse Declarative Pipelines, when you create a CDC (Change Data Capture) flow with operations like APPLY AS DELETE and SEQUENCE BY, the system needs a way to manage incremental updates, out-of-order events, updates, and deletions, while maintaining the correct order of events based on sequence keys.\n\n"
            "The '__apply_changes_storage_users_target' internal table is an internal storage table that keeps track of these changes along with 'tombstones' (records marked for deletion) and versioning metadata to ensure that out-of-order or late-arriving events are applied correctly. This internal table is not meant for direct querying but is essential for maintaining CDC consistency.\n\n"
            "The 'users_target' view, on the other hand, is a virtual layer over this internal table that presents the current state of the data as a clean, queryable snapshot. This allows the CDC logic to remain encapsulated and transparent."
        ),
    },
    {
        "exam": 3,
        "id": "q11_share_delta_table_sensitive_data",
        "question": (
            "The data engineering team is looking for a simple solution to share part of a large Delta Lake table with the data science team. Only department-specific columns in the table need to be shared, but with different names. In addition, there is some sensitive data that must be filtered out before sharing.\n\n"
            "Which of the following objects can be created to meet the specified requirements ?"
        ),
        "options": [
            "A new Delta Table created using SHALLOW CLONE from the existing table",
            "A new Delta Table created using DEEP CLONE from the existing table",
            "A new Delta Table created using CTAS statement on the existing table",
            "A stored view on the existing table"
        ],
        "answer": "A stored view on the existing table",
        "explanation": (
            "The solution in this case is to create a view on the table where the required columns can be renamed, and the sensitive data that can be filtered out with the WHERE clause."
        ),
    },
    {
        "exam": 3,
        "id": "q12_materializing_results_cost_latency",
        "question": (
            "The data engineering team has a Silver table called 'sales_cleaned' where raw sales data is appended in near real-time.\n\n"
            "They want to create a new Gold-layer entity against the 'sales_cleaned' table to calculate the year-to-date (YTD) of the sales amount. The new entity will have the following schema:\n\n"
            "country_code STRING, category STRING, ytd_total_sales FLOAT, updated TIMESTAMP\n\n"
            "It's enough for these metrics to be recalculated once daily. But since they will be queried very frequently by downstream business teams, the data engineering team wants to cut down the potential costs and latency associated with materializing the results.\n\n"
            "Which of the following solutions meets these requirements?"
        ),
        "options": [
            "Create multiple tables, one per business team so the metrics can be queried quickly and efficiently.",
            "Define the new entity as a global temporary view since it can be shared between notebooks or jobs that share computing resources.",
            "Configuring a nightly batch job to recalculate the metrics and store them as a table overwritten with each update.",
            "Define the new entity as a view to avoid penalizing the results each time the metrics are recalculated"
        ],
        "answer": "Configuring a nightly batch job to recalculate the metrics and store them as a table overwritten with each update.",
        "explanation": (
            "Data engineers must understand how materializing results is different between views and tables on Databricks, and how to reduce total compute and storage cost associated with each materialization.\n\n"
            "Consider using a view when:\n"
            "- Your query is not complex. Because views are computed on demand, the view is re-computed every time the view is queried. So, frequently querying complex queries with joins and subqueries increases compute costs.\n"
            "- You want to reduce storage costs. Views do not require additional storage space.\n\n"
            "Consider using a gold table when:\n"
            "- Multiple downstream queries consume the table, so you want to avoid re-computing complex ad-hoc queries every time.\n"
            "- Query results should be computed incrementally from a data source that is continuously or incrementally growing."
        ),
    },
    {
        "exam": 3,
        "id": "q13_structured_streaming_late_data",
        "question": "Which of the following techniques can a data engineer use to handle late-arriving data in Spark Structured Streaming?",
        "options": [
            "Windowing",
            "Checkpointing",
            "Watermarking",
            "Partitioning"
        ],
        "answer": "Watermarking",
        "explanation": (
            "In Structured Streaming, you can handle late-arriving data primarily using watermarking, which allows the system to track event time progress and specify how long to wait for late data before considering a window complete."
        ),
    },
    {
        "exam": 3,
        "id": "q14_cluster_minimal_permissions",
        "question": "Which of the following describes the minimal permissions a data engineer needs to start and terminate an existing cluster ?",
        "options": [
            'Cluster creation allowed + "Can Restart" privileges on the cluster',
            '"Can Manage" privilege on the cluster',
            '"Can Restart" privilege on the cluster',
            '"Can Attach To" privilege on the cluster'
        ],
        "answer": '"Can Restart" privilege on the cluster',
        "explanation": (
            "You can configure two types of cluster permissions:\n"
            "1. The 'Allow cluster creation' entitlement controls your ability to create clusters.\n"
            "2. Cluster-level permissions control your ability to use and modify a specific cluster. There are four permission levels for a cluster: No Permissions, Can Attach To, Can Restart, and Can Manage. The table lists the abilities for each permission."
        ),
    },
    {
        "exam": 3,
        "id": "q15_stream_static_joins_delta",
        "question": "Which statement regarding static Delta tables in Stream-Static joins is correct?",
        "options": [
            "Static Delta tables need to be refreshed with REFRESH TABLE command for each microbatch of a stream-static join",
            "The latest version of the static Delta table is returned only for the first microbatch of the stream-static join. Then, it will be cached to be used by any upcoming microbatch.",
            "Static Delta tables must be small enough to be broadcasted to all worker nodes in the cluster.",
            "The latest version of the static Delta table is returned each time it is queried by a microbatch of the stream-static join"
        ],
        "answer": "The latest version of the static Delta table is returned each time it is queried by a microbatch of the stream-static join",
        "explanation": (
            "Stream-static joins take advantage of Delta Lake guarantees that the latest version of the static delta table is returned each time it is queried in a join operation with a data stream."
        ),
    },
    {
        "exam": 3,
        "id": "q16_spark_ui_stage_metrics",
        "question": "In Spark UI, which of the following is Not part of the metrics displayed in a stage's details page ?",
        "options": [
            "Duration",
            "Spill (Disk and Memory)",
            "DBU Cost",
            "GC time"
        ],
        "answer": "DBU Cost",
        "explanation": (
            "In Spark UI, the stage's details page shows summary metrics for completed tasks. This includes:\n"
            "- Duration of tasks.\n"
            "- GC time is the total JVM garbage collection time.\n"
            "- Shuffle spill (memory) is the size of the deserialized form of the shuffled data in memory\n"
            "- Shuffle spill (disk) is the size of the serialized form of the data on disk.\n"
            "- and others.\n\n"
            "DBU Cost is not part of Spark UI. DBU stands for Databricks Unit and it is a unit of processing capability per hour for pricing purposes. This depends on your cluster configuration which tells you how much DBUs would be consumed if a virtual machine runs for an hour, and then pays for each DBU consumed."
        ),
    },
    {
        "exam": 3,
        "id": "q17_scheduling_notebooks_production",
        "question": "As a general rule, before scheduling notebooks in production, which of the following commands should be removed from the code ?",
        "options": [
            "Magic commands",
            "Import commands",
            "Markup language commands",
            "Display commands"
        ],
        "answer": "Display commands",
        "explanation": (
            "Before scheduling notebooks in production, you may need to refactor your code. As a general rule, Make sure you comment out:\n"
            "- Unwanted display or show commands added during development\n"
            "- Display actions or SQL queries added for debugging purposes"
        ),
    },
    {
        "exam": 3,
        "id": "q18_scd_type_0_definition",
        "question": "Which of the following definitions correctly describes a Slowly Changing Dimension of Type 0?",
        "options": [
            "It's a table where the new arriving data overwrites the existing one.",
            "It's a table that stores and manages both current and historical data over time.",
            "It's a table where history will be kept in the additional column",
            "It's a table where no changes are allowed."
        ],
        "answer": "It's a table where no changes are allowed.",
        "explanation": (
            "Type 0 SCD tables never change. Tables of this type are usually static. For example, static lookup tables."
        ),
    },
    {
        "exam": 3,
        "id": "q19_delta_lake_file_statistics",
        "question": "Which statement regarding Delta Lake File Statistics is Not correct?",
        "options": [
            "Nested fields do not count when determining the first 32 columns in the table.",
            "The statistics are leveraged for data skipping when executing selective queries.",
            "Delta Lake captures statistics in the transaction log for each added data file",
            "The statistics are generally uninformative for string fields with very high cardinality."
        ],
        "answer": "Nested fields do not count when determining the first 32 columns in the table.",
        "explanation": (
            "Delta Lake automatically captures statistics in the transaction log for each added data file of the table. By default, Delta Lake collects the statistics on the first 32 columns of each table. Nested fields count when determining the first 32 columns\n\n"
            "Example: 4 struct fields with 8 nested fields will total to the 32 columns."
        ),
    },
    {
        "exam": 3,
        "id": "q20_insert_only_merge",
        "question": "Which of the following commands allows data engineers to perform an insert-only merge?",
        "options": [
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN MATCHED\n    INSERT *",
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN MATCHED\n    INSERT *\nWHEN NOT MATCHED\n    INSERT *",
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN NOT MATCHED\n    INSERT *",
            "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN NOT MATCHED\n    UPDATE *"
        ],
        "answer": "MERGE INTO orders\nUSING new_orders\nON orders.orders_id = new_orders.orders_id\nWHEN NOT MATCHED\n    INSERT *",
        "explanation": (
            "The syntax for insert-only merge:\n\n"
            "MERGE INTO target_table\n"
            "USING source_table\n"
            "ON merge_condition\n"
            "WHEN NOT MATCHED\n"
            "    THEN INSERT *\n\n"
            "You just need to specify the NOT MATCHED clause, which inserts a row when a source row does not match any target row based on the merge_condition (merge keys). Records that have the same keys as an existing record in the table will be simply ignored."
        ),
    },
    {
        "exam": 3,
        "id": "q21_automatic_liquid_clustering",
        "question": "How does Automatic Liquid Clustering determine which columns to use as clustering keys in a Unity Catalog-managed Delta table?",
        "options": [
            "It leverages advanced sampling strategies to randomize column selection after uniformly balancing data across all files.",
            "It intelligently selects clustering keys from predefined clustering columns specified during table creation.",
            "It automatically determines optimal clustering keys based on the type and order of column definition in the schema.",
            "It leverages Predictive Optimization to choose optimal clustering keys based on observed query behavior."
        ],
        "answer": "It leverages Predictive Optimization to choose optimal clustering keys based on observed query behavior.",
        "explanation": "Automatic Liquid Clustering in Databricks is a feature designed to automatically optimize the physical layout of data in Delta tables based on the access patterns and metadata statistics. It leverages Predictive Optimization, which uses query behavior analytics to select clustering keys dynamically."
    },
    {
        "exam": 3,
        "id": "q22_delta_file_statistics_query",
        "question": (
            "Given a Delta table 'products' with the following schema:\n\n"
            "name STRING, category STRING, expiration_date DATE, price FLOAT\n\n"
            "When executing the below query:\n\n"
            "SELECT * FROM products\n"
            "WHERE price = 90.5\n\n"
            "Which of the following will be leveraged by the query optimizer to identify the data files to load?"
        ),
        "options": [
            "Files statistics in Unity Catalog metastore",
            "Columns statistics in the metadata of Parquet files",
            "Files statistics in the Delta transaction log",
            "Columns statistics in the Hive metastore"
        ],
        "answer": "Files statistics in the Delta transaction log",
        "explanation": (
            "In the transaction log, Delta Lake captures statistics for each data file of the table. These statistics indicate per file:\n"
            "- Total number of records\n"
            "- Minimum value in each column of the first 32 columns of the table\n"
            "- Maximum value in each column of the first 32 columns of the table\n"
            "- Null value counts for in each column of the first 32 columns of the table\n\n"
            "When a query with a selective filter is executed against the table, the query optimizer uses these statistics to generate the query result. It leverages them to identify data files that may contain records matching the conditional filter.\n\n"
            "For the SELECT query in the question, the transaction log is scanned for min and max statistics for the price column."
        )
    },
    {
        "exam": 3,
        "id": "q23_delta_table_over_partitioned",
        "question": (
            "The data engineering team noticed that a partitioned Delta Lake table is suffering greatly. They are experiencing slowish performance for most general queries on this table.\n\n"
            "The team tried to run an OPTIMIZE command on the table, but this did not help to resolve the issue.\n\n"
            "Which of the following likely explains the cause of these slowdowns?"
        ),
        "options": [
            "The table is over-partitioned or incorrectly partitioned. This requires a full rewrite of all data files to resolve the issue.",
            "They are applying the OPTIMIZE command without ZORDER. Z-ordering is needed on the partitioning columns",
            "They are applying the OPTIMIZE command on the whole table. It must be applied at each partition separately.",
            "The table has too many old data files that need to be purged. They need to run a VACUUM command instead."
        ],
        "answer": "The table is over-partitioned or incorrectly partitioned. This requires a full rewrite of all data files to resolve the issue.",
        "explanation": (
            "Data that is over-partitioned or incorrectly partitioned will suffer greatly. Files cannot be combined or compacted across partition boundaries, so partitioned small tables increase storage costs and total number of files to scan. This leads to slowdowns for most general queries. Such an issue requires a full rewrite of all data files to remedy."
        )
    },
    {
        "exam": 3,
        "id": "q24_lakeflow_streaming_table",
        "question": (
            "A data engineering team at a supply chain company uses Lakeflow Declarative Pipelines to manage inventory data. The team maintains an append-only streaming table, inventory_raw, that stores raw inventory status information, with columns product_id, quantity, and event_timestamp.\n\n"
            "A data engineer is tasked with creating a new table, inventory_latest, to capture near real-time changes in product inventory directly from inventory_raw. The new table will include the columns product_id, current_quantity, and updated_timestamp.\n\n"
            "Which of the following types of objects would be most suitable to implement the inventory_latest table?"
        ),
        "options": [
            "Live table",
            "Materialized view",
            "Temporary view",
            "Streaming table"
        ],
        "answer": "Streaming table",
        "explanation": (
            "The most suitable object to implement the inventory_latest table is a Streaming table, because it is designed to continuously capture and update near real-time changes from an append-only source like inventory_raw. Implementing inventory_latest as a Streaming table allows you to merge incoming changes or apply a CDC (Change Data Capture) feed from inventory_raw so that the table always reflects the most up-to-date state per product_id. Each new event—whether an update or insertion—can be applied in real time, updating current_quantity and updated_timestamp without rebuilding the entire table, which is the main advantage over a materialized view or temporary view.\n\n"
            "Materialized Views (formerly known as Live Tables) provide batch-oriented or scheduled incremental processing for precomputed queries rather than continuously updating individual records in real time. Temporary views, in contrast, are ephemeral and not suited for persistent state tracking."
        )
    },
    {
        "exam": 3,
        "id": "q25_liquid_clustering_prerequisites",
        "question": "Which two prerequisites are required to enable Automatic Liquid Clustering on a Delta table?\n\nChoose 2 answers:",
        "options": [
            "Table must be a Unity Catalog-managed table",
            "Table must have deletion vectors enabled",
            "Table must be a Unity Catalog-external table",
            "Table must have predictive optimization enabled",
            "Table must be partitioned by a date column"
        ],
        "answer": [
            "Table must be a Unity Catalog-managed table",
            "Table must have predictive optimization enabled"
        ],
        "explanation": (
            "To enable Automatic Liquid Clustering on a Delta table in Databricks, two prerequisites are required:\n\n"
            "1. Table must be a Unity Catalog-managed table\n"
            "- Automatic Liquid Clustering works only on tables managed by Unity Catalog.\n"
            "- External tables are currently not supported.\n\n"
            "2. Table must have predictive optimization enabled\n"
            "- Predictive optimization provides the system with insights on access patterns, which Liquid Clustering leverages to automatically optimize data layout."
        )
    },
    {
        "exam": 3,
        "id": "q26_python_wheels_databricks",
        "question": "Which of the following statements best describes the use of Python wheels in Databricks?",
        "options": [
            "A Python wheel is a binary distribution format for installing custom Python code packages on Databricks Clusters",
            "A Python wheel is package installer tool alternative to 'pip'",
            "A Python wheel is a repository for hosting, managing, and distributing Python binaries and artifacts in a Databricks workspace",
            "A Python wheel is a virtual environment for isolating the Python interpreter, libraries and modules in a notebook from other notebooks."
        ],
        "answer": "A Python wheel is a binary distribution format for installing custom Python code packages on Databricks Clusters",
        "explanation": (
            "Python wheel is a binary distribution format for installing custom Python code packages on Databricks Clusters.\n\n"
            "A wheel is a ZIP-format archive with the .whl extension."
        )
    },
    {
        "exam": 3,
        "id": "q27_cdf_overwrite_target",
        "question": (
            "Given the following query on the Delta table customers on which Change Data Feed is enabled :\n\n"
            "spark.read\n"
            '    .option("readChangeFeed", "true")\n'
            '    .option("startingVersion", 0)\n'
            '    .table("customers")\n'
            '    .filter(" _change_type=\'update_postimage\'")\n'
            "    .write\n"
            '    .mode("overwrite")\n'
            '    .table("customers_updates")\n\n'
            "Which statement describes the results of this query each time it is executed?"
        ),
        "options": [
            "The entire history of updated records will overwrite the target table at each execution.",
            "Newly updated records will overwrite the target table.",
            "Newly updated records will be appended to the target table.",
            "The entire history of updated records will be appended to the target table at each execution, which leads to duplicate entries."
        ],
        "answer": "The entire history of updated records will overwrite the target table at each execution.",
        "explanation": (
            "When querying the table's changes, captured by CDF, using spark.read means that you are reading them as a static source. So, each time you run the query, all table's changes (starting from the specified startingVersion) will be read.\n\n"
            "The query in the question then writes the data in mode \"overwrite\" to the target table, which completely overwrites the table at each execution."
        )
    },
    {
        "exam": 3,
        "id": "q28_optimize_default_file_size",
        "question": "Which of the following is the default target file size when compacting small files of a Delta table by manually running OPTIMIZE command ?",
        "options": [
            "1024 MB",
            "256 MB",
            "512 MB",
            "128 MB"
        ],
        "answer": "1024 MB",
        "explanation": "The OPTIMIZE command compact small data files into larger ones. The default value is 1073741824, which sets the size to 1 GB."
    },
    {
        "exam": 3,
        "id": "q29_autoloader_pathglobfilter",
        "question": (
            "A production environment has an S3 bucket receiving thousands of image files daily in different formats (png, .jpg, .gif). A data engineer has been tasked with modifying the following streaming ingestion script to ensure only .png files are processed.\n\n"
            'df = spark.readStream.format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "binaryFile") \\\n'
            '    .option(__________________, "*.png") \\\n'
            '    .load("s3://mybucket/incoming/")\n\n'
            "Which option correctly fills in the blank to meet the specified requirement ?"
        ),
        "options": [
            "fileExtension",
            "cloudFiles.pathGlobFilter",
            "pathGlobFilter",
            "cloudFiles.fileExtension"
        ],
        "answer": "pathGlobFilter",
        "explanation": (
            "The pathGlobFilter option allows you to filter input files based on a glob pattern, such as \"*.png\", when using Auto Loader."
        )
    },
    {
        "exam": 3,
        "id": "q30_cluster_permissions_attach",
        "question": "Which of the following describes the minimal permissions a data engineer needs to attach a notebook to an existing cluster ?",
        "options": [
            'Cluster creation allowed + "Can Attach To" privileges on the cluster',
            '"Can Attach To" privilege on the cluster',
            '"Can Restart" privilege on the cluster',
            '"Can Manage" privilege on the cluster'
        ],
        "answer": '"Can Attach To" privilege on the cluster',
        "explanation": (
            "You can configure two types of cluster permissions:\n"
            "1- The 'Allow cluster creation' entitlement controls your ability to create clusters.\n"
            "2- Cluster-level permissions control your ability to use and modify a specific cluster. There are four permission levels for a cluster: No Permissions, Can Attach To, Can Restart, and Can Manage. The table lists the abilities for each permission:\n"
            "Attach notebook to compute is permitted under \"Can Attach To\"."
        )
    },
    {
        "exam": 3,
        "id": "q31_mitigate_data_skew_join",
        "question": (
            "A data engineer is analyzing a dataset of clickstream events from a high-traffic website. The dataset includes fields such as user_id, timestamp, event_type, and page_url. During a join operation between the clickstream logs and a user profile dataset (joined on user_id), the job's performance is significantly hindered due to uneven data distribution. Further analysis confirms a data skew caused by a small subset of users generating a disproportionately large number of events.\n\n"
            "Which of the following approaches is NOT an appropriate solution to mitigate the skew in this scenario?"
        ),
        "options": [
            "Broadcast the skewed keys to all worker nodes to avoid shuffle during the join.",
            "Separate processing of skewed keys by handling high-frequency users in a dedicated job.",
            "Use salting by appending a random prefix to skewed user_id values to distribute the load across partitions.",
            "Repartition the clickstream dataset on user_id to increase the number of partitions before the join."
        ],
        "answer": "Broadcast the skewed keys to all worker nodes to avoid shuffle during the join.",
        "explanation": (
            "Broadcasting a small table is a great way to share small lookup datasets with all executors to avoid joins that cause shuffles. However, broadcasting skewed keys, especially if the associated data is large, does not solve the skew problem and may actually increase memory pressure on each executor.\n\n"
            "Other options are appropriate solutions:\n"
            "- Use salting by appending a random prefix to skewed user_id values to distribute the load across partitions. Salting is an effective technique to mitigate skew by artificially spreading out hot keys across multiple partitions. This approach reduces bottlenecks caused by skewed keys during shuffles.\n\n"
            "- Repartition the clickstream dataset to increase the number of partitions before the join. While increasing the number of partitions via repartition() helps balance the data load and enhances parallelism, it can help mitigate skew by distributing keys more evenly.\n\n"
            "- Separate processing of skewed keys by handling high-frequency users in a dedicated job. Isolating skewed keys for specialized processing prevents them from affecting the entire join operation. This targeted approach can improve performance by tailoring resources to problematic keys."
        )
    },
    {
        "exam": 3,
        "id": "q32_lakeflow_data_quality_expect_all",
        "question": (
            "A data engineer is building a Lakeflow Declarative Pipeline to process product sales data. The pipeline needs to enforce the following data quality rules:\n\n"
            'valid_products = "product_id IS NOT NULL", "recent_sales": "date >= \'2023-01-01\'", "quantity_within_range": "quantity BETWEEN 0 AND 1000"\n\n'
            "Any invalid records should still be written to the target, while metrics about these violations are captured by the pipeline.\n\n"
            "Which of the following configurations would satisfy these requirements?"
        ),
        "options": [
            "@dlt.table\n@dlt.expect_all(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
            "@dlt.table\n@dlt.expect_all_fail(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
            "@dlt.table\n@dlt.expect_all_drop(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
            "@dlt.table\n@dlt.expect(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")"
        ],
        "answer": "@dlt.table\n@dlt.expect_all(valid_products)\ndef silver_sales():\n    return dlt.read_stream(\"bronze_sales\")",
        "explanation": (
            "dlt.expect_all() enforces all the specified data quality rules, writes both valid and invalid records to the target table, and captures metrics about any rule violations.\n\n"
            "dlt.expect() would not fully meet the requirements because it applies expectations individually but doesn't automatically enforce all of them together as a group. Similarly, dlt.expect_or_drop() removes individual invalid records, and dlt.expect_or_fail() stops the pipeline on individual rule violations. To evaluate multiple expectations together and specify collective actions using the functions dlt.expect_all(), dlt.expect_all_drop(), and dlt.expect_all_or_fail().\n\n"
            "Note: Databricks has recently open-sourced this solution, integrating it into the Apache Spark ecosystem under the name Spark Declarative Pipelines (SDP)."
        )
    },
    {
        "exam": 3,
        "id": "q33_query_profiler_total_wall_clock",
        "question": (
            "A data engineer executed a query that took a long time. To investigate, they use the Query Profiler associated with this query to check the Total wall-clock duration metric.\n\n"
            "Which of the following statements correctly describe what this metric measures?"
        ),
        "options": [
            "The time spent on actual query execution",
            "The total time spent on query optimization and file pruning",
            "The time spent on query scheduling",
            "The total time from the start of scheduling to the end of query execution"
        ],
        "answer": "The total time from the start of scheduling to the end of query execution",
        "explanation": (
            "The Total wall-clock duration metric measures the total time from the start of scheduling to the end of query execution, covering the entire period the query takes to run, including scheduling, optimization and file pruning, and actual execution."
        )
    },
    {
        "exam": 3,
        "id": "q34_delta_check_constraint_failure",
        "question": (
            "The data engineering team has a large Delta table named 'users'. A recent query on the table returned some entries with negative values in the 'age' column.\n\n"
            "To avoid this issue and enforce data quality, a junior data engineer decided to add a CHECK constraint to the table with the following command:\n\n"
            "ALTER TABLE users ADD CONSTRAINT valid_age CHECK (age> 0);\n\n"
            "However, the command fails when executed.\n\n"
            "Which statement explains the cause of this failure?"
        ),
        "options": [
            "The users table already contains rows that violate the new constraint; all existing rows must satisfy the constraint before adding it to the table.",
            "The users table already exists; CHECK constraints can only be added during table creation using CREATE TABLE command.",
            "The syntax for adding the CHECK constraint is incorrect. Instead, the command should be: ALTER TABLE users ADD CONSTRAINT ON COLUMN age (CHECK > 0)",
            "The users table already contains rows; CHECK constraints can only be added on empty tables"
        ],
        "answer": "The users table already contains rows that violate the new constraint; all existing rows must satisfy the constraint before adding it to the table.",
        "explanation": (
            "ADD CONSTRAINT command verifies that all existing rows in the table satisfy the constraint before adding it to the table. Otherwise, the command failed with an error that says some rows in the table violate the new CHECK constraint."
        )
    },
    {
        "exam": 3,
        "id": "q35_validate_button_declarative_pipeline",
        "question": (
            "The data engineering team at an analytics firm has started implementing Lakeflow Declarative Pipelines to process large-scale data transformations. During a routine code review session, the data engineering lead emphasizes a critical best practice: before performing any pipeline runs, the team must always click the \"Validate\" button in the notebook associated with the pipeline.\n\n"
            "What is the main benefit of this practice?"
        ),
        "options": [
            "It validates that the user has access permissions to create tables in the catalog.",
            "It checks for any syntax errors in the pipeline code without actually processing data",
            "It executes the pipeline on a small dataset to preview the transformation results.",
            "It runs unit tests on all pipeline components to verify their correctness."
        ],
        "answer": "It checks for any syntax errors in the pipeline code without actually processing data",
        "explanation": (
            "The \"Validate\" action in the notebook identifies syntax or configuration errors in the pipeline definition before execution, reducing the risk of runtime failures or writing partial data."
        )
    },
    {
        "exam": 3,
        "id": "q36_databricks_rest_api_run_id",
        "question": "When running an existing job via Databricks REST API, which of the following represents the globally unique identifier of the newly triggered run ?",
        "options": [
            "task_id",
            "job_id",
            "run_key",
            "run_id"
        ],
        "answer": "run_id",
        "explanation": (
            "Running an existing job via the endpoint '/api/2.0/jobs/run-now' returns the run_id of the triggered run. This represents the globally unique identifier of this newly triggered run."
        )
    },
    {
        "exam": 3,
        "id": "q37_pyspark_window_dense_rank",
        "question": (
            "A data engineer has a PySpark DataFrame with the following columns: employee_name, department, and salary. They want to assign a tier to each employee within their department based on salary, where employees earning the same salary share the same tier. The expected output is as follows:\n\n"
            "| employee_name | department | salary | tier |\n"
            "|---------------|------------|--------|------|\n"
            "| Eve           | HR         | 4000   | 1    |\n"
            "| Frank         | HR         | 4000   | 1    |\n"
            "| David         | HR         | 3900   | 2    |\n"
            "| Alice         | Sales      | 5000   | 1    |\n"
            "| Bob           | Sales      | 4500   | 2    |\n"
            "| Charlie       | Sales      | 4500   | 2    |\n\n"
            "To achieve this, they define a window by department and order by salary in descending order:\n\n"
            'window_spec = Window.partitionBy("department").orderBy(df["salary"].desc())\n\n'
            "Which of the following functions correctly use this window to calculate the tier column?"
        ),
        "options": [
            'df.withColumn("tier", percent_rank().over(window_spec))',
            'df.withColumn("tier", rank().over(window_spec))',
            'df.withColumn("tier", row_number().over(window_spec))',
            'df.withColumn("tier", dense_rank().over(window_spec))'
        ],
        "answer": 'df.withColumn("tier", dense_rank().over(window_spec))',
        "explanation": (
            "The correct function to use is dense_rank() because it assigns the same rank (or tier) to employees with identical salaries within each department while maintaining the correct order when salaries differ. In this case, employees with the same salary value share the same tier number, and the next unique salary value receives the next consecutive rank number. This matches the expected output, where, for example, Eve and Frank in the HR department both have a salary of 4000 and share tier 1, while David, with a lower salary of 3900, gets tier 2.\n\n"
            "Other functions such as row_number() would assign unique sequential numbers even for ties, rank() would skip numbers after ties, and percent_rank() would assign fractional ranks between 0 and 1, none of which align with the desired behavior."
        )
    },
    {
        "exam": 3,
        "id": "q38_udf_dynamic_data_masking",
        "question": (
            "A data engineer at a healthcare organization manages a Delta Lake table patient_records with columns: patient_id, name, department, and diagnosis. They want to create a user-defined function that masks the diagnosis column so that only doctors can view values in that column.\n\n"
            "Which of the following functions can the data engineer use to achieve this?"
        ),
        "options": [
            "CREATE FUNCTION patient_mask(doctors STRING)\nRETURN CASE WHEN is_account_group_member('doctors') THEN doctors ELSE 'CONFIDENTIAL' END;",
            "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN in_account_group_member('doctors') THEN diagnosis ELSE 'CONFIDENTIAL' END;",
            "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN is_account_group_member('doctors') THEN diagnosis ELSE 'CONFIDENTIAL' END;",
            "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN diagnosis IS NOT NULL THEN diagnosis ELSE 'CONFIDENTIAL' END;"
        ],
        "answer": "CREATE FUNCTION patient_mask(diagnosis STRING)\nRETURN CASE WHEN is_account_group_member('doctors') THEN diagnosis ELSE 'CONFIDENTIAL' END;",
        "explanation": (
            "This function properly implements role-based access control by verifying if the current user belongs to the 'doctors' group using the is_account_group_member('doctors') function. If the user is a doctor, the function returns the actual diagnosis; otherwise, it replaces the diagnosis with the string 'CONFIDENTIAL' to protect sensitive patient information.\n\n"
            "This approach ensures compliance with healthcare data privacy requirements while allowing authorized medical staff to access the necessary information."
        )
    },
    {
        "exam": 3,
        "id": "q39_dataframe_write_append",
        "question": (
            "A data engineer has been asked to develop a nightly batch job for workforce productivity analytics. The job will calculate points of employees productivity of the previous day, and store the performance of each employee in the Delta table \"employees_performance\". The table has the following schema:\n\n"
            "\"date DATE, employee_id STRING, rating DOUBLE\"\n\n"
            "The data engineering team wants data to be stored in the table with the ability to compare employees' performance across time.\n\n"
            "Which of the following code blocks accomplishes this task?"
        ),
        "options": [
            'performance_df.write.format("delta").saveAsTable("employees_performance")',
            'performance_df.write.mode("append").saveAsTable("employees_performance")',
            'performance_df.write.mode("overwrite").saveAsTable("employees_performance")',
            'performance_df.write.saveAsTable("employees_performance")'
        ],
        "answer": 'performance_df.write.mode("append").saveAsTable("employees_performance")',
        "explanation": (
            "DataFrameWriter.mode defines the writing behaviour when data or table already exists.\n"
            "Options include:\n"
            "- append: Append contents of the DataFrame to existing data.\n"
            "- overwrite: Overwrite existing data.\n"
            "- error or errorifexists: Throw an exception if data already exists.\n"
            "- ignore: Silently ignore this operation if data already exists.\n\n"
            "This errorifexists or error is the default save mode. If the table already exists, it will throw the error message: Error: pyspark.sql.utils.AnalysisException: table already exists.\n\n"
            "The \"employees_performance\" table has a date column. So, in order to be able to compare employees' performance across time, each new batch of data with new date should be appended into the table using the append mode."
        )
    },
    {
        "exam": 3,
        "id": "q40_repair_failed_multitask_job",
        "question": (
            "A data engineer has a job with multiple tasks that takes more than 2 hours to complete. In the last run, the final task unexpectedly failed.\n\n"
            "Which of the following actions can the data engineer perform to complete this run while minimizing the execution time ?"
        ),
        "options": [
            "They can re-run this Job Run to execute all the tasks",
            "They need to delete the failed Run, and start a new Run for the Job",
            "They can keep the failed Run, and simply start a new Run for the Job",
            "They can repair this Job Run so only the failed tasks will be re-executed"
        ],
        "answer": "They can repair this Job Run so only the failed tasks will be re-executed",
        "explanation": (
            "You can repair failed multi-task jobs by running only the subset of unsuccessful tasks and any dependent tasks. Because successful tasks are not re-run, this feature reduces the time and resources required to recover from unsuccessful job runs."
        )
    },
    {
        "exam": 3,
        "id": "q41_grant_least_privilege_access",
        "question": (
            "A data scientist from the marketing department requires read-only access to the 'customer_insights' table located in the analytics schema, which is part of the BI catalog. The data will be used to generate quarterly reports. Following the principle of least privilege, only the minimum permissions necessary to perform the required tasks should be granted.\n\n"
            "Which SQL commands will correctly grant access with the least privileges?"
        ),
        "options": [
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;",
            "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;"
        ],
        "answer": "GRANT SELECT ON TABLE bi.analytics.insights TO marketing_team;\nGRANT USE CATALOG ON CATALOG bi TO marketing_team;\nGRANT USE SCHEMA ON SCHEMA bi.analytics TO marketing_team;",
        "explanation": (
            "To access a specific table, the user must be granted SELECT on the table itself, USE SCHEMA on the containing schema, and USE CATALOG on the parent catalog. This provides just enough access for read operations without overprovisioning."
        )
    },
    {
        "exam": 3,
        "id": "q42_secret_scope_read_permission",
        "question": (
            "The data engineering team has a secret scope named \"DataOps-Prod\" that contains all secrets needed by DataOps engineers in a production workspace.\n\n"
            "Which of the following is the minimum permission required for the DataOps engineers to use the secrets in this scope?"
        ),
        "options": [
            "MANAGE permission on each secret in the \"DataOps-Prod\" scope",
            "READ permission on the \"DataOps-Prod\" scope",
            "READ permission on each secret in the \"DataOps-Prod\" scope",
            "MANAGE permission on the \"DataOps-Prod\" scope"
        ],
        "answer": "READ permission on the \"DataOps-Prod\" scope",
        "explanation": (
            "The secret access permissions are as follows:\n"
            "- MANAGE - Allowed to change ACLs, and read and write to this secret scope.\n"
            "- WRITE - Allowed to read and write to this secret scope.\n"
            "- READ - Allowed to read this secret scope and list what secrets are available.\n\n"
            "Each permission level is a subset of the previous level's permissions (that is, a principal with WRITE permission for a given scope can perform all actions that require READ permission)."
        )
    },
    {
        "exam": 3,
        "id": "q43_extract_ldp_data_quality_metrics",
        "question": (
            "A data engineer needs to programmatically extract the data quality results of a LDP pipeline from the associated event log table.\n\n"
            "Which of the following code snippets can the data engineer use to achieve this task?"
        ),
        "options": [
            "SELECT expectations\nFROM catalog.schema.event_log\nWHERE event_type = 'metrics'",
            "SELECT data_quality\nFROM catalog.schema.event_log\nWHERE event_type = 'metrics'",
            "SELECT details:flow_progress.data_quality.expectations\nFROM catalog.schema.event_log\nWHERE event_type = 'flow_progress'",
            "SELECT data_quality\nFROM catalog.schema.event_log\nWHERE event_type = 'flow_progress'"
        ],
        "answer": "SELECT details:flow_progress.data_quality.expectations\nFROM catalog.schema.event_log\nWHERE event_type = 'flow_progress'",
        "explanation": (
            "In the event log table for LDP* pipelines, the data quality results are logged under events of type 'flow_progress' and stored inside the details column in a nested JSON structure:\n\n"
            "- details:flow_progress: contains information about a pipeline's execution progress\n"
            "- details:flow_progress.data_quality: contains the data quality results (expectations, dropped_records, etc.)\n"
            "- details:flow_progress.data_quality.expectations: specifically holds the expectation results.\n\n"
            "* Databricks has recently open-sourced this solution, integrating it into the Apache Spark ecosystem under the name Spark Declarative Pipelines (SDP)."
        )
    },
    {
        "exam": 3,
        "id": "q44_ldp_constraint_violation_drop_row",
        "question": (
            "A data engineer has defined the following data quality constraint in a LDP pipeline:\n\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ________________\n\n"
            "Which clause correctly fills in the blank so records violating this constraint will be dropped, and reported in metrics?"
        ),
        "options": [
            "ON VIOLATION DISCARD ROW",
            "ON VIOLATION DELETE ROW",
            "ON VIOLATION DROP ROW",
            "ON VIOLATION FAIL UPDATE"
        ],
        "answer": "ON VIOLATION DROP ROW",
        "explanation": (
            "The correct clause to fill in the blank is ON VIOLATION DROP ROW, so the full constraint becomes: CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW. This ensures that any record with a null id will be automatically dropped from the pipeline, while still being tracked in the pipeline's metrics, allowing the data engineer to monitor the number of violations without failing the entire job.\n\n"
            "Note: Databricks has recently open-sourced this solution, integrating it into the Apache Spark ecosystem under the name Spark Declarative Pipelines (SDP)."
        )
    },
    {
        "exam": 3,
        "id": "q45_cluster_policies_not_advantage",
        "question": "Which of the following is Not an advantage of using cluster policies?",
        "options": [
            "Enforce cluster-scoped library installations.",
            "Ensure clusters are created with consistent system settings, environment variables, and Spark configuration.",
            "Control cost by limiting per-cluster maximum cost",
            "Schedule clusters to start and stop at specific times."
        ],
        "answer": "Schedule clusters to start and stop at specific times.",
        "explanation": (
            "Scheduling clusters to start and stop at specific times is not supported with cluster policies.\n"
            "Cluster policies are primarily designed to enforce consistent configurations, manage library installations, control costs by setting limits and defaults on cluster creation."
        )
    },
    {
        "exam": 3,
        "id": "q46_delta_sharing_d2d_vs_ods",
        "question": (
            "A data engineering team wants to use Delta Sharing but is unsure whether to use Databricks-to-Databricks sharing (D2D) or the open Delta Sharing protocol (ODS).\n\n"
            "Which of the following statements correctly explains the difference between D2D and ODS?"
        ),
        "options": [
            "Databricks-to-Databricks sharing (D2D) leverages the legacy Hive metastore, whereas the Open Sharing protocol (ODS) is built on Unity Catalog for newer implementations.",
            "Databricks-to-Databricks sharing (D2D) allows sharing with any platform that supports the Delta Sharing open standard, while the Open Sharing protocol (ODS) restricts sharing only to Databricks clients.",
            "Databricks-to-Databricks sharing (D2D) enables data sharing exclusively between Databricks clients, while the Open Sharing protocol (ODS) allows any platform that implements the Delta Sharing open standard to access shared data.",
            "Databricks-to-Databricks sharing (D2D) supports sharing data only through managed tables, while the Open Sharing protocol (ODS) supports both managed and external tables."
        ],
        "answer": "Databricks-to-Databricks sharing (D2D) enables data sharing exclusively between Databricks clients, while the Open Sharing protocol (ODS) allows any platform that implements the Delta Sharing open standard to access shared data.",
        "explanation": (
            "There are mainly two ways to share data using Delta Sharing:\n\n"
            "1- Databricks-to-Databricks sharing (D2D): It lets you share data from your Unity Catalog-enabled workspace with users who also have access to a Unity Catalog-enabled Databricks workspace.\n\n"
            "This approach uses the Delta Sharing server that is built into Databricks and provides support for notebook sharing, Unity Catalog data governance, auditing, and usage tracking for both providers and recipients.\n\n"
            "2- Databricks open sharing protocol (ODS): It lets you share data that you manage in a Unity Catalog-enabled Databricks workspace with users on any computing platform.\n\n"
            "This approach also uses the Delta Sharing server that is built into Databricks and is useful when you manage data using Unity Catalog and want to share it with users who don't use Databricks or don't have access to a Unity Catalog-enabled Databricks workspace.\n\n"
            "So, D2D is optimized for seamless sharing within the Databricks ecosystem, whereas ODS extends interoperability to external platforms that support the open Delta Sharing protocol."
        )
    },
    {
        "exam": 3,
        "id": "q47_autoloader_schema_evolution",
        "question": (
            "A data engineer has implemented the following streaming job a new pipeline using Databricks Auto Loader:\n\n"
            "spark.readStream \\\n"
            '    .format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            '    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema") \\\n'
            '    .load("/mnt/incoming_data") \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", "/mnt/checkpoints/data") \\\n'
            '    .start("sales_data")\n\n'
            "What is the expected behavior of this streaming job if a new column appears in the incoming JSON files that is not part of the original schema?"
        ),
        "options": [
            "The stream fails, and all new columns are saved in a rescued data column for later processing.",
            "The stream fails and will not restart unless the schema is manually updated or the problematic data file is removed.",
            "The stream fails, but it automatically restarts after updating the schema with the new columns.",
            "The stream fails temporarily but continues by ignoring the new columns without schema update."
        ],
        "answer": "The stream fails and will not restart unless the schema is manually updated or the problematic data file is removed.",
        "explanation": (
            "With the failOnNewColumns mode, the stream detects any new columns and fails immediately to enforce strict schema consistency. It will not automatically restart until the schema has been manually updated to include the new columns or the data files causing the schema mismatch are removed. This prevents silent schema drift and ensures deliberate schema management."
        )
    },
    {
        "exam": 3,
        "id": "q48_delta_sharing_identifier",
        "question": (
            "During the setup of Delta Sharing with an external partner, a data engineer asks the partner for their sharing identifier.\n\n"
            "Which of the following best describes the sharing identifier within the context of Databricks-to-Databricks sharing?"
        ),
        "options": [
            "It serves as a public encryption key used during data writes to the partner's tables",
            "It identify the partner's network IP address for firewall whitelisting",
            "It acts as the authentication token for API calls with the recipient's endpoint",
            "It provides a unique reference for the recipient's Unity Catalog metastore"
        ],
        "answer": "It provides a unique reference for the recipient's Unity Catalog metastore",
        "explanation": (
            "A Delta Sharing identifier is a unique string used in Databricks-to-Databricks sharing to identify a recipient's Unity Catalog metastore. This identifier allows the data provider to grant access to shared data.\n\n"
            "The format of the sharing identifier is:\n"
            "<cloud>:<region>:<uuid>\n\n"
            "Example:\n"
            "aws:us-west-2:10a8dbea-54bc-43ad-87de-0320b91cb818\n\n"
            "In this example:\n"
            "- aws represents the cloud provider (Amazon Web Services).\n"
            "- us-west-2 represents the specific AWS region.\n"
            "- 10a8dbea-54bc-43ad-87de-0320b91cb818 is the Universally Unique Identifier (UUID) of the recipient's Unity Catalog metastore.\n\n"
            "The partner can obtain their sharing identifier from their Databricks workspace using Catalog Explorer or by running a SQL query (SELECT CURRENT_METASTORE()). This identifier is then provided to the data provider, who uses it to create a recipient and grant access to shares."
        )
    },
    {
        "exam": 3,
        "id": "q49_materialized_view_definition",
        "question": (
            "Which of the following is being described in this statement?\n\n"
            '"An object that physically stores precomputed query results, updating automatically or on a schedule to improve performance for complex aggregations and BI workloads"'
        ),
        "options": [
            "Materialized view",
            "Temporary view",
            "Standard view",
            "Streaming table"
        ],
        "answer": "Materialized view",
        "explanation": (
            "The statement describes a materialized view. In Databricks SQL, materialized views are Unity Catalog managed tables that physically store the results of a query. Unlike standard views, which compute results on-the-fly, materialized views cache the results and update them as the underlying source tables change; either on a schedule or automatically. By pre-computing expensive or frequently used queries, Materialized views lower query latency and resource consumption. This optimizes performance for complex aggregations and accelerate BI dashboard performance.\n\n"
            "In summary, a materialized view has the following characteristics:\n"
            "- Physically stores the results of a query.\n"
            "- Can be refreshed automatically or on a schedule.\n"
            "- Optimized for complex aggregations and business intelligence (BI) workloads.\n\n"
            "Why other options are incorrect:\n"
            "- Standard view: Does not store data physically; it's just a saved query.\n"
            "- Temporary view: Exists only for the session and is not persistent.\n"
            "- Streaming table: Incrementally ingests data, but it's not for storing precomputed query results."
        )
    },
    {
        "exam": 3,
        "id": "q50_integration_testing_definition",
        "question": "Which of the following statements correctly describes Integration Testing?",
        "options": [
            "It's an approach to test the interaction between subsystems of an application to ensure that modules work properly as a group.",
            "It's an approach to verify if each feature of the application works as per the business requirements",
            "It's an approach to simulate a user experience to ensure that the application can run properly under real-world scenarios",
            "It's an approach to test individual units of code to determine whether they still work as expected if new changes are made to them in the future"
        ],
        "answer": "It's an approach to test the interaction between subsystems of an application to ensure that modules work properly as a group.",
        "explanation": (
            "Integration Testing is an approach to testing the interaction between subsystems of an application. It tests that the software modules are integrated logically and tested as a group."
        )
    },
    {
        "exam": 3,
        "id": "q51_databricks_bundle_generate",
        "question": (
            "A data engineer has an existing Databricks job and wants to manage it using Databricks Asset Bundles. They want to use the Databricks CLI to get the YAML definition of the job and download its referenced artifacts.\n\n"
            "Which of the following commands allows the data engineer to achieve this?"
        ),
        "options": [
            "databricks bundle generate job --existing-job-id",
            "databricks bundle clone job --existing-job-id",
            "databricks bundle get job --existing-job-id",
            "databricks bundle download job --existing-job-id"
        ],
        "answer": "databricks bundle generate job --existing-job-id",
        "explanation": (
            "The correct command is: databricks bundle generate, because it allows the data engineer to generate bundle configuration for a resource that already exists in your Databricks workspace. This process generates a YAML definition of a job, pipeline, or dashboard and automatically downloads referenced artifacts, such as notebooks."
        )
    },
    {
        "exam": 3,
        "id": "q52_drop_external_table",
        "question": (
            "The data engineering team has a Delta Lake table created with following query:\n\n"
            "CREATE TABLE customers_clone\n"
            "LOCATION 's3://my-bucket/'\n"
            "AS SELECT * FROM customers\n\n"
            "A data engineer wants to drop the table with the following query:\n\n"
            "DROP TABLE customers_clone\n\n"
            "Which statement describes the result of running this drop command ?"
        ),
        "options": [
            "An error will occur as the table is shallowly cloned from the customers table",
            "Only the table's metadata will be deleted from the catalog, while the data files will be kept in the storage",
            "The table will not be dropped until VACUUM command is run",
            "Both the table's metadata and the data files will be deleted"
        ],
        "answer": "Only the table's metadata will be deleted from the catalog, while the data files will be kept in the storage",
        "explanation": (
            "External (unmanaged) tables are tables whose data is stored in an external storage path by using a LOCATION clause.\n\n"
            "When you run DROP TABLE on an external table, only the table's metadata is deleted, while the underlying data files are kept."
        )
    },
    {
        "exam": 3,
        "id": "q53_inner_join_static_delta_tables",
        "question": (
            "The data engineering team has the following join logic between three Delta tables:\n\n"
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
            "Which statement describes the result of this code block each time it is executed ?"
        ),
        "options": [
            "All records in the current version of the source tables will be considered in the join operations. The unmatched records will overwrite the students_courses_details table.",
            "Only newly added records to any of the source tables will be considered in the join operations. The matched records will overwrite the students_courses_details table.",
            "Only newly added records to any of the source tables will be considered in the join operations. The unmatched records will overwrite the students_courses_details table.",
            "All records in the current version of the source tables will be considered in the join operations. The matched records will overwrite the students_courses_details table."
        ],
        "answer": "All records in the current version of the source tables will be considered in the join operations. The matched records will overwrite the students_courses_details table.",
        "explanation": (
            "The query reads three static Delta tables using spark.table() function, which means that all records in the current version of these tables will be read and considered in the join operations.\n\n"
            "There is no difference between spark.table() and spark.read.table() function. Actually, spark.read.table() internally calls spark.table().\n\n"
            "The pyspark.sql.DataFrame.join() function performs inner join operation by default, so the matched records will be written to the target table. In our case, the query writes the data in mode \"overwrite\" to the target table, which completely overwrites the table."
        )
    },
    {
        "exam": 3,
        "id": "q54_lakehouse_federation_purpose",
        "question": "What is the primary purpose of Lakehouse Federation in data architecture?",
        "options": [
            "To optimize storage costs by compressing data",
            "To create backups of data stored in Databricks",
            "To enable direct querying across multiple data sources without migrating data",
            "To migrate all data into Databricks for centralized processing"
        ],
        "answer": "To enable direct querying across multiple data sources without migrating data",
        "explanation": (
            "Lakehouse Federation allows users and applications to run queries across diverse data sources—such as data lakes, warehouses, and databases—without requiring the physical migration of data into Databricks. This reduces data duplication, lowers latency, and streamlines access, enabling a unified query experience across distributed environments."
        )
    },
    {
        "exam": 3,
        "id": "q55_delta_clone_modifications",
        "question": "Which of the following statements regarding cloning tables on Databricks is correct?",
        "options": [
            "Any changes made to shallow clones affect only the clones themselves and not the source table. While, changes made to deep clones affect the source table.",
            "Any changes made to deep clones affect only the clones themselves and not the source table. While, changes made to shallow clones affect the source table.",
            "Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.",
            "Changes made to either deep or shallow clones affect the source table."
        ],
        "answer": "Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.",
        "explanation": (
            "In either case, deep or shallow cloning, data modifications applied to the cloned version of the table will be tracked and stored separately from the source, so it will not affect the source table."
        )
    },
    {
        "exam": 3,
        "id": "q56_delta_auto_optimize",
        "question": (
            "Which of the following is being described in this statement?\n\n"
            "\"A Delta Lake's functionality that automatically compacts small files during individual writes to a table by performing two complementary operations on the table\""
        ),
        "options": [
            "Auto compaction",
            "OPTIMIZE operation",
            "Optimized writes",
            "Auto Optimize"
        ],
        "answer": "Auto Optimize",
        "explanation": (
            "Auto Optimize is a functionality that allows Delta Lake to automatically compact small data files of Delta tables. This can be achieved during individual writes to the Delta table.\n\n"
            "Auto optimize consists of 2 complementary operations:\n"
            "- Optimized writes: with this feature enabled, Databricks attempts to write out 128 MB files for each table partition.\n"
            "- Auto compaction: this will check after an individual write, if files can further be compacted. If yes, it runs an OPTIMIZE job with 128 MB file sizes (instead of the 1 GB file size used in the standard OPTIMIZE)."
        )
    },
    {
        "exam": 3,
        "id": "q57_notebook_scoped_python_wheel",
        "question": (
            "A data engineer wants to install a Python wheel scoped to the current notebook's session, so only the current notebook and any jobs associated with this notebook have access to that library.\n\n"
            "Which of the following commands can the data engineer use to complete this task?"
        ),
        "options": [
            "%fs install my_package.whl",
            "%pip install my_package.whl",
            "%sh install my_package",
            "%python install my_package.whl"
        ],
        "answer": "%pip install my_package.whl",
        "explanation": (
            "%pip install allows you to install a Python wheel scoped to the current notebook's session. This library will be only accessible in the current notebook and any jobs associated with this notebook."
        )
    },
    {
        "exam": 3,
        "id": "q58_standard_cluster_access_mode",
        "question": (
            "A team consisting of multiple data analysts wants to work on an analytics project that involves performing basic data exploration, querying small datasets, and running analyses using Python and SQL. They ask a data engineer to configure interactive clusters to support their workloads.\n\n"
            "Which of the following cluster access modes should the engineer configure to best support this use case?"
        ),
        "options": [
            "STANDARD",
            "SINGLE USER",
            "DEDICATED",
            "NO_ISOLATION_SHARED"
        ],
        "answer": "STANDARD",
        "explanation": (
            "For a team of data analysts performing exploratory analysis, querying small datasets, and running analyses in Python and SQL collaboratively, the STANDARD cluster access mode is the most suitable choice. Standard clusters are designed to provide shared access to the cluster resources for general workloads, providing cost-effective compute options while isolating users' workloads from each other. They natively handle Python and SQL workloads, and since the project does not involve specialized computations like R, MLlib, or RDD-based tasks, there is no need for the additional capabilities of Dedicated access mode. This mode balances operational efficiency with simplicity, making it ideal for collaborative but non-specialized analytics tasks.\n\n"
            "Other modes are less appropriate for this scenario. Dedicated Access is intended for specialized workloads or group-based secure collaboration, which is overkill for standard Python/SQL analytics. Single-user clusters, which are now part of Dedicated access mode, are designed for isolated operational workloads, not team collaboration. No Isolation Shared clusters provide minimal data access controls and are generally discouraged for multi-user environments due to security concerns.\n\n"
            "Therefore, configuring Standard mode clusters ensures the analysts can collaborate effectively while keeping the environment secure, efficient, and cost-effective."
        )
    },
    {
        "exam": 3,
        "id": "q59_lakehouse_federation_foreign_catalog",
        "question": (
            "A data engineering team has successfully established a new connection named mysql_connection in Databricks to connect to their external MySQL database. Their goal is to make the MySQL tables available and queryable through Unity Catalog by leveraging Lakehouse Federation, allowing downstream analytics teams to seamlessly access this data.\n\n"
            "Given that the connection is already in place, the team now needs to take the next step to add the MySQL tables within Unity Catalog so that they can be queried in a governed and secure manner, consistent with their organization's data governance policies.\n\n"
            "What is the next step the team should take to achieve this goal?"
        ),
        "options": [
            "Create an external catalog with a default location defined via the existing mysql_connection.",
            "Set up a Unity Catalog metastore for MySQL using the existing mysql_connection.",
            "Create a foreign catalog in Unity Catalog using the existing mysql_connection.",
            "Create an external table referencing MySQL data through the existing mysql_connection."
        ],
        "answer": "Create a foreign catalog in Unity Catalog using the existing mysql_connection.",
        "explanation": (
            "The next step the team should take is to create a foreign catalog in Unity Catalog using the existing mysql_connection. A foreign catalog acts as a metadata bridge, discovering and mapping tables from the data sources like MySQL as foreign catalogs. This makes the tables accessible and queryable in a governed way while ensuring queries are pushed down to the source system."
        )
    },
    {
        "exam": 4,
        "id": "q01_sql_alert_multiple_columns",
        "question": (
            "A data engineer in a call center needs to implement an SQL alert to track ticket volume and status changes. "
            "They want to set the alert based on multiple columns of the tickets table. The alert should be triggered when both of the following conditions are met:\n\n"
            "1. The number of new tickets exceeds 200.\n"
            "2. The number of tickets under processing exceeds 150.\n\n"
            "Which of the following SQL queries correctly implements this alert logic?"
        ),
        "options": [
            "SELECT\n    SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\nFROM tickets\nWHERE new_tickets > 200\n    AND under_processing > 150",
            "SELECT new_tickets + under_processing\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics\nWHERE new_tickets + under_processing > 350",
            "SELECT new_tickets, under_processing\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics\nWHERE new_tickets > 200\n    AND under_processing > 150",
            "SELECT CASE\n    WHEN new_tickets > 200 AND under_processing > 150 THEN 1\n    ELSE 0\nEND\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics"
        ],
        "answer": "SELECT CASE\n    WHEN new_tickets > 200 AND under_processing > 150 THEN 1\n    ELSE 0\nEND\nFROM (\n    SELECT\n        SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_tickets,\n        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS under_processing\n    FROM tickets\n) statistics",
        "explanation": (
            "The query correctly calculates the sums of new and in-progress tickets in a subquery, and then uses a CASE statement to trigger the alert when both conditions are met (new_tickets > 200 AND under_processing > 150), which matches the intended alert logic.\n\n"
            "Remember, alerts in Databricks can only evaluate a single field. That's why we use a CASE WHEN expression to combine multiple conditions into one alert field."
        ),
    },
    {
        "exam": 4,
        "id": "q02_print_working_directory",
        "question": "Which of the following commands prints the current working directory of a notebook?",
        "options": [
            "%sh pwd",
            "os.path.abspath()",
            "print(sys.path)",
            "os.environ['PYTHONPATH']"
        ],
        "answer": "%sh pwd",
        "explanation": (
            "The %sh magic command allows you to run shell code on a notebook.\n\n"
            "The pwd is an acronym for print working directory."
        ),
    },
    {
        "exam": 4,
        "id": "q03_pyspark_withcolumn_override",
        "question": (
            "The data engineering team needs to share a dataset containing Social Security Numbers with an external vendor to perform matching operations. To achieve this, they implemented the following code:\n\n"
            'df_masked = df.withColumn("ssn_hash", sha2("ssn", 256))\n'
            'df_masked.write.saveAsTable("masked_analytics")\n\n'
            "However, this code still exposes the original values.\n\n"
            "Which of the following statements correctly explains the reason for this behavior?"
        ),
        "options": [
            "The sha2 function is not available in PySpark. The table masked_analytics should be created in Spark SQL using a CTAS statement.",
            'The code adds a new column to df_masked instead of overriding the original value. They need to use withColumn("ssn", sha2("ssn", 256)) instead.',
            'The code adds a new column to df_masked without dropping the original value. It must be followed by a .drop("ssn_hash") command.',
            'The sha2 function doesn\'t apply to numerical values. They need to use withColumn("ssn_hash", md5("ssn")) instead.'
        ],
        "answer": 'The code adds a new column to df_masked instead of overriding the original value. They need to use withColumn("ssn", sha2("ssn", 256)) instead.',
        "explanation": (
            "In PySpark, the withColumn function creates a new column or replaces an existing column in a DataFrame based on the given expression. The code in this question adds a new column (ssn_hash) to the dataframe but doesn't remove or overwrite the original ssn column, so the original Social Security numbers are still present in the table.\n\n"
            'To properly mask the data, the team should either overwrite the ssn column with the hash (withColumn("ssn", sha2("ssn", 256))) or drop the original column after hashing.'
        ),
    },
    {
        "exam": 4,
        "id": "q04_dynamic_view_permissions",
        "question": (
            "The data engineering team has a dynamic view with following definition:\n\n"
            "CREATE VIEW students_vw AS\n"
            "SELECT * FROM students\n"
            "WHERE\n"
            "    CASE\n"
            "        WHEN is_account_group_member('instructors') THEN TRUE\n"
            "        ELSE is_active IS FALSE\n"
            "    END;\n\n"
            "Which statement describes the results returned by querying this view?"
        ),
        "options": [
            "Members of the instructors group will only see the records of active students. While users that are not members of the specified group will only see the records of inactive students.",
            "Only members of the instructors group will see the records of all students no matter if they are active or not. While users that are not members of the specified group will see null values for the records of inactive students",
            "Members of the instructors group will see the records of all students no matter if they are active or not. While users that are not members of the specified group will only see the records of inactive students.",
            "Only members of the instructors group will see the records of all students no matter if they are active or not. While users that are not members of the specified group will only see the records of inactive students"
        ],
        "answer": "Members of the instructors group will see the records of all students no matter if they are active or not. While users that are not members of the specified group will only see the records of inactive students.",
        "explanation": (
            "Only members of the instructors group will have full access to the underlying data since the WHERE condition will be True for every record. On the other hand, users that are not members of the specified group (instructors) will only see records of students with active status = false."
        ),
    },
    {
        "exam": 4,
        "id": "q05_autoloader_maxbytespertrigger",
        "question": (
            "A data engineer is using the following Auto Loader stream to incrementally ingest large JSON files. These files cause long micro-batch processing times and occasional memory issues:\n\n"
            "df = spark.readStream \\\n"
            '    .format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            "    .................................... \\\n"
            '    .load("s3://project/source/")\n\n'
            "They want to process only a portion of the data per micro-batch, improving stability and keeping batch times predictable.\n\n"
            "Which option correctly fills in the blank to process only 128 MB of data per micro-batch?"
        ),
        "options": [
            '.option("cloudFiles.maxBytesPerTrigger", "128mb")',
            '.option("triggerInterval", "128mb")',
            '.option("batchSize", "128mb")',
            '.option("cloudFiles.maxDataPerTrigger", "128mb")'
        ],
        "answer": '.option("cloudFiles.maxBytesPerTrigger", "128mb")',
        "explanation": (
            "In Auto Loader, cloudFiles.maxBytesPerTrigger controls the maximum amount of data to process in each micro-batch, allowing the stream to handle large files incrementally and keep batch processing times predictable."
        ),
    },
    {
        "exam": 4,
        "id": "q06_cli_jobs_list_completed_only",
        "question": "Which of the following Databricks CLI commands allows a data engineer to list all runs of a job that completed successfully?",
        "options": [
            "databricks jobs list-runs --job-id <job-id> --success",
            "databricks jobs list-runs --job-id <job-id> --completed-only",
            "databricks jobs list-runs --job-id <job-id> --success-only",
            "databricks jobs list-runs --job-id <job-id> --completed-only --success"
        ],
        "answer": "databricks jobs list-runs --job-id <job-id> --completed-only",
        "explanation": (
            "The correct Databricks CLI command that allows a data engineer to list all runs of a job that completed successfully is:\n\n"
            "databricks jobs list-runs --job-id <job-id> --completed-only.\n\n"
            "The --completed-only parameter is the proper flag to include only completed runs in the results. Otherwise, the command will list both active and completed runs."
        ),
    },
    {
        "exam": 4,
        "id": "q07_git_folders_collaboration",
        "question": (
            "Two junior data analysts are collaborating on a data analytics project using a Databricks notebook. Currently, they are relying on the built-in notebook versioning feature within Databricks to manage changes and maintain some level of version control. While this approach works for small-scale, individual work, the team faces challenges when collaborating as changes are frequently overwritten or lost. Consequently, a senior data engineer suggests that they start using Git folders for source control instead of relying solely on the notebook's built-in versioning system.\n\n"
            "Which of the following reasons could explain why Git folders are recommended over Databricks notebook versioning for collaborative team work?"
        ),
        "options": [
            "Git folders support resolving merge conflicts automatically, making it faster to integrate contributions from different team members without constant manual intervention.",
            "Git folders provide AI-generated code suggestions by analyzing the contributions and coding patterns of other team members, helping developers write compatible code.",
            "Git folders ensure that the team always has the latest notebook version by automatically synchronizing all project notebooks without requiring any commits or pushes.",
            "Git folders support creating and managing branches for development work, which helps prevent accidental overwrites and allows multiple team members to work on features simultaneously."
        ],
        "answer": "Git folders support creating and managing branches for development work, which helps prevent accidental overwrites and allows multiple team members to work on features simultaneously.",
        "explanation": (
            "The main reason Git folders are recommended over Databricks notebook versioning for collaborative teamwork is that Git folders support creating and managing branches for development work. In Databricks, team members can clone their own Git folder linked to the same remote Git repository and work on separate isolated branches without interfering with each other's code. This approach prevents accidental overwrites and allows multiple team members to work on features simultaneously, enabling proper version control and structured collaboration."
        ),
    },
    {
        "exam": 4,
        "id": "q08_lakehouse_federation_connection",
        "question": (
            "A data engineer is tasked with enabling analysts and data scientists to query tables stored in an external PostgreSQL database directly from Databricks, without moving or replicating the data. They plan to use Lakehouse Federation and Unity Catalog to set up a foreign catalog for seamless access to the external data.\n\n"
            "What is the very first step the data engineer should take in this process?"
        ),
        "options": [
            "Grant CREATE SHARE and CREATE RECIPIENT permissions on the metastore to the metastore administrators.",
            "Configure a connection in Unity Catalog to securely connect to the PostgreSQL database, establishing the necessary credentials and network access.",
            'Navigate to the account console as an account administrator to enable the option "Allow Delta Sharing with parties outside your organization"',
            "Configure an external location and storage credentials in Unity Catalog to securely connect to the PostgreSQL's underlying storage."
        ],
        "answer": "Configure a connection in Unity Catalog to securely connect to the PostgreSQL database, establishing the necessary credentials and network access.",
        "explanation": (
            "The very first step the data engineer should take is to configure a connection in Unity Catalog to securely connect to the PostgreSQL database, establishing the necessary credentials and network access. This is essential because before creating any foreign catalogs, Databricks needs a secure, authenticated link to the external PostgreSQL database to allow seamless querying without moving the data."
        ),
    },
    {
        "exam": 4,
        "id": "q09_foreachbatch_merge_deduplication",
        "question": (
            "A data engineer has the following logic to handle duplicates in Spark Structured Streaming:\n\n"
            "spark.readStream \\\n"
            '    .table("orders") \\\n'
            '    .selectExpr("from_json(CAST(value AS STRING), \'...\') as value") \\\n'
            '    .select("value.*") \\\n'
            '    .withWatermark("order_timestamp", "30 seconds") \\\n'
            '    .dropDuplicates("order_id", "order_timestamp") \\\n'
            "    ...\n\n"
            "However, they notice that this logic is not sufficient to prevent duplicates for events that arrive later than the watermark threshold.\n\n"
            "Which of the following code snippets can the data engineer include in a foreachBatch function to completely handle streaming duplicates?"
        ),
        "options": [
            "APPLY CHANGES INTO orders_silver c\nFROM microBatch\nON order_id, order_timestamp\nCOLUMNS *",
            "COPY INTO orders_silver\nFROM microBatch\nDISTINCT ALL\nCOPY_OPTIONS ('mergeSchema' = 'true')",
            "MERGE INTO orders_silver c\nUSING microBatch s\nON s.order_id=c.order_id AND s.order_timestamp=c.order_timestamp\nWHEN NOT MATCHED THEN INSERT *",
            'spark.readStream\n  .table("microBatch")\n  .withWatermark("order_timestamp", "7 days")\n  .dropDuplicates("order_id", "order_timestamp")'
        ],
        "answer": "MERGE INTO orders_silver c\nUSING microBatch s\nON s.order_id=c.order_id AND s.order_timestamp=c.order_timestamp\nWHEN NOT MATCHED THEN INSERT *",
        "explanation": (
            "In Spark Structured Streaming, dropDuplicates with a watermark only removes duplicates that arrive within the defined event-time threshold, for example, within 30 seconds of order_timestamp. However, any records arriving later than that threshold are considered \"too late\" and are not deduplicated by dropDuplicates. To ensure complete deduplication (including very late-arriving data), the foreachBatch sink can use an idempotent write pattern with Delta Lake's MERGE operation.\n\n"
            "The MERGE INTO statement compares each micro-batch of incoming data (microBatch) with the target Delta table (orders_silver) based on unique keys (order_id and order_timestamp). It only inserts rows that do not already exist in the target table, preventing duplicate events across micro-batches or late arrivals. This combination of in-stream deduplication for near real-time performance and MERGE-based sink deduplication for robustness and correctness provided an end-to-end reliable way to handle duplicates in streaming pipelines."
        ),
    },
    {
        "exam": 4,
        "id": "q10_spark_ui_sql_dataframe_tab",
        "question": "Which of the following statements correctly describes the SQL/DataFrame tab in Spark UI?",
        "options": [
            "It provides a list of all the Spark jobs that have been submitted, including details about their start and end times, status, associated stages, and task metrics, allowing users to drill down into individual task performance",
            "It shows the executed operations, including their query plans, execution metrics, physical and logical plans, DAG visualizations, stage and task breakdowns, and performance statistics for monitoring and debugging.",
            "It shows all RDDs and DataFrames that are cached or persisted in memory and on disk, along with their size, storage levels, and block locations, helping users monitor memory usage and optimize caching strategies.",
            "It presents an in-depth view of all stages, showing their dependencies, task execution times, shuffle read/write metrics, and the distribution of tasks across worker nodes, giving insight into stage-level performance."
        ],
        "answer": "It shows the executed operations, including their query plans, execution metrics, physical and logical plans, DAG visualizations, stage and task breakdowns, and performance statistics for monitoring and debugging.",
        "explanation": (
            "The SQL/DataFrame tab in Spark UI is specifically focused on Spark SQL and DataFrame operations for debugging, monitoring, and understanding complex workloads. It provides a detailed view of queries, including: Logical and physical query plans, DAG visualizations of operations, Metrics for execution stages and tasks, Performance statistics for debugging and monitoring queries.\n\n"
            "Why other options are incorrect:\n"
            "- Spark Jobs tab: It provides a list of all the Spark jobs that have been submitted...\n"
            "- Stages tab: It presents an in-depth view of all stages...\n"
            "- Storage tab: It shows all RDDs and DataFrames that are cached or persisted..."
        ),
    },
    {
        "exam": 4,
        "id": "q11_python_script_as_notebook",
        "question": "Which of the following establishes a Python file as a notebook in Databricks ?",
        "options": [
            "The import of the dbutils.notebook module in the file's source code",
            "The creation of a spark session using SparkSession.builder.getOrCreate() in the file's source code",
            "The comment '# Databricks notebook source' on the first line of the file's source code",
            "The magic command %databricks on the first line of the file's source code"
        ],
        "answer": "The comment '# Databricks notebook source' on the first line of the file's source code",
        "explanation": (
            "You can convert Python, SQL, Scala, and R scripts to single-cell notebooks by adding a comment to the first cell of the file: # Databricks notebook source"
        ),
    },
    {
        "exam": 4,
        "id": "q12_shallow_clone_vacuum",
        "question": (
            "The data engineering team has a table 'orders_backup' that was created using Delta Lake's SHALLOW CLONE functionality from the table 'orders'. Recently, the team started getting an error when querying the 'orders_backup' table indicating that some data files are no longer present.\n\n"
            "Which of the following correctly explains this error ?"
        ),
        "options": [
            "The OPTIMIZE command was run on the orders table",
            "The VACUUM command was run on the orders_backup table",
            "The VACUUM command was run on the orders table",
            "The OPTIMIZE command was run on the orders_backup table"
        ],
        "answer": "The VACUUM command was run on the orders table",
        "explanation": (
            "With Shallow Clone, you create a copy of a table by just copying the Delta transaction logs. That means that there is no data moving during Shallow Cloning.\n\n"
            "Running the VACUUM command on the source table may purge data files referenced in the transaction log of the clone. In this case, you will get an error when querying the clone indicating that some data files are no longer present."
        ),
    },
    {
        "exam": 4,
        "id": "q13_lakeflow_declarative_pipelines",
        "question": (
            "Which of the following technologies is being described below?\n\n"
            '"A declarative ETL framework for implementing incremental data processing, while minimizing operational overhead and maintaining table dependencies and data quality."'
        ),
        "options": [
            "ETL",
            "DAB",
            "DBU",
            "LDP"
        ],
        "answer": "LDP",
        "explanation": (
            "The technology described is LDP (Lakeflow Declarative Pipelines). LDP is a declarative ETL framework on Databricks designed to handle incremental data processing efficiently while minimizing operational overhead. It supports automatic orchestration that ensures dependencies between tables are properly managed, and maintains high data quality throughout the pipeline."
        ),
    },
    {
        "exam": 4,
        "id": "q14_rest_api_runs_get_structure",
        "question": (
            "A data engineer is using Databricks REST API to send a GET request to the endpoint '/api/2.1/jobs/runs/get' to retrieve the run's metadata of a multi-task job using its run_id.\n\n"
            "Which statement correctly describes the response structure of this API call?"
        ),
        "options": [
            "Each task of this job run will have a unique orchestration_id",
            "Each task of this job run will have a unique run_id",
            "Each task of this job run will have a unique task_id",
            "Each task of this job run will have a unique job_id"
        ],
        "answer": "Each task of this job run will have a unique run_id",
        "explanation": (
            "Each task of this job run will have a unique run_id to retrieve its output with endpoint '/api/2.1/jobs/runs/get-output'"
        ),
    },
    {
        "exam": 4,
        "id": "q15_structured_streaming_processingtime",
        "question": (
            "Given the following Structured Streaming query:\n\n"
            'spark.table("orders") \\\n'
            '    .withColumn("total_after_tax", col("total")*col("tax")) \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", checkpointPath) \\\n'
            '    .outputMode("append") \\\n'
            "    ._________________ \\\n"
            '    .table("new_orders")\n\n'
            "Fill in the blank to make the query executes a micro-batch to process data every 2 minutes"
        ),
        "options": [
            'trigger(once="2 minutes")',
            'trigger("2 minutes")',
            'processingTime("2 minutes")',
            'trigger(processingTime="2 minutes")'
        ],
        "answer": 'trigger(processingTime="2 minutes")',
        "explanation": (
            "In Spark Structured Streaming, in order to process data in micro-batches at a user-specified intervals, you can use the processingTime trigger method. This allows you to specify a time duration as a string, by default, it's 500ms."
        ),
    },
    {
        "exam": 4,
        "id": "q16_query_profiler_top_operators",
        "question": (
            "A data engineer is using the Query Profiler in Databricks SQL to investigate a slow-performing SQL query. They want to find out which operations in the query are taking the most time.\n\n"
            "Which section of the Query Profile highlights the most expensive operations in the query, helping to identify potential optimization opportunities?"
        ),
        "options": [
            "Query wall-clock duration",
            "Query status",
            "Aggregated task time",
            "Top operators"
        ],
        "answer": "Top operators",
        "explanation": (
            "The correct answer is Top operators. In Databricks SQL, the Top operators section of the Query Profile highlights the most expensive operations within a query by showing which specific operations (such as scans, joins, or aggregations) are consuming the most time. This allows the data engineer to pinpoint performance bottlenecks and focus on optimizing the parts of the query that have the highest impact on overall execution time."
        ),
    },
    {
        "exam": 4,
        "id": "q17_delta_lake_file_statistics_average",
        "question": "Which of the following is Not a valid Delta Lake File Statistics ?",
        "options": [
            "The number of null values for each of the first 32 columns",
            "The minimum and maximum value in each of the first 32 columns",
            "The total number of records in the added data file.",
            "The average value for each of the first 32 columns"
        ],
        "answer": "The average value for each of the first 32 columns",
        "explanation": (
            "Delta Lake automatically captures statistics in the transaction log for each added data file of the table. These statistics indicate per file: Total number of records, Minimum value in each column of the first 32 columns of the table, Maximum value in each column of the first 32 columns of the table, Null value counts for in each column of the first 32 columns of the table.\n\n"
            "The average value in the columns is not part of Delta Lake File Statistics"
        ),
    },
    {
        "exam": 4,
        "id": "q18_delta_sharing_auth_difference",
        "question": (
            "A multinational company wants to share sales analytics data with both its internal Databricks teams located in different regions and external consulting partners. Internal teams access the data via Databricks-to-Databricks sharing (D2D), while external partners use the open Delta Sharing (ODS) protocol.\n\n"
            "In this scenario, how does authentication differ between the D2D sharing and the ODS protocol?"
        ),
        "options": [
            "Databricks-to-Databricks sharing (D2D) uses built-in authentication with no token exchange, whereas open Delta Sharing (ODS) requires external authentication via bearer tokens or OIDC federation.",
            "Databricks-to-Databricks sharing (D2D) and open Delta Sharing (ODS) both use the same authentication method, so there is no difference.",
            "Databricks-to-Databricks sharing (D2D) relies on OIDC federation, whereas open Delta Sharing (ODS) requires authentication via bearer tokens.",
            "Databricks-to-Databricks sharing (D2D) relies on unified login with single sign-on (SSO), whereas open Delta Sharing (ODS) uses external login with OIDC federation."
        ],
        "answer": "Databricks-to-Databricks sharing (D2D) uses built-in authentication with no token exchange, whereas open Delta Sharing (ODS) requires external authentication via bearer tokens or OIDC federation.",
        "explanation": (
            "Databricks-to-Databricks sharing (D2D) uses built-in authentication with no token exchange, allowing internal teams to access shared data seamlessly within the Databricks environment, whereas open Delta Sharing (ODS) requires external authentication, typically via bearer tokens or OIDC federation, to securely grant external partners access to the data."
        ),
    },
    {
        "exam": 4,
        "id": "q19_liquid_clustering_optimize",
        "question": (
            "A data engineer manages a Delta Lake table with liquid clustering enabled. They understand that liquid clustering operates incrementally, but they are unsure how to trigger the clustering operation when new data is ingested into the table.\n\n"
            "Which of the following commands should be executed to cluster the newly added data?"
        ),
        "options": [
            "ANALYZE",
            "ZORDER",
            "VACUUM",
            "OPTIMIZE"
        ],
        "answer": "OPTIMIZE",
        "explanation": (
            "To cluster newly added data in a Delta Lake table with liquid clustering enabled, the data engineer should execute the OPTIMIZE command. OPTIMIZE triggers the clustering operation by physically reorganizing the data files to improve query performance."
        ),
    },
    {
        "exam": 4,
        "id": "q20_all_privileges_excludes_manage",
        "question": "Which of the following privileges is not included in the ALL PRIVILEGES permission?",
        "options": [
            "MANAGE",
            "EXECUTE",
            "MODIFY",
            "BROWSE"
        ],
        "answer": "MANAGE",
        "explanation": (
            "The privilege MANAGE is not included in the ALL PRIVILEGES permission. While ALL PRIVILEGES grants a comprehensive set of permissions such as EXECUTE, BROWSE, and MODIFY, it explicitly excludes MANAGE to prevent accidental data exfiltration or privilege escalation.\n\n"
            "Remember, MANAGE allows a user to view and manage privileges, transfer ownership, drop, and rename an object. It is similar to object ownership, but holding the MANAGE privilege does not automatically grant all other privileges on the object, although the user can grant themselves additional privileges if needed."
        ),
    },
    {
        "exam": 4,
        "id": "q21_job_ownership_groups",
        "question": (
            'The data engineering team created a new Databricks job for processing sensitive financial data. A financial analyst asked the team to transfer the "Owner" privilege of this job to the "finance" group.\n\n'
            'A junior data engineer that has the "CAN MANAGE" permission on the job is attempting to make this privilege transfer via Databricks Job UI, but it keeps failing.\n\n'
            "Which of the following explains the cause of this failure?"
        ),
        "options": [
            'Having the "CAN MANAGE" permission is not enough to grant "Owner" privileges to a group. The data engineer must be the current owner of the job.',
            'The "Owner" privilege is assigned at job creation to the creator and cannot be changed. The job must be re-created using the "finance" group\'s credentials.',
            "Groups can not be owners of Databricks jobs. The owner must be an individual user.",
            'Having the "CAN MANAGE" permission is not enough to grant "Owner" privileges to a group. The data engineer must be a workspace administrator.'
        ],
        "answer": "Groups can not be owners of Databricks jobs. The owner must be an individual user.",
        "explanation": (
            "A job cannot have a group as an owner. If you try to set a group as the owner of a job, you get the error 'Groups can not be owners'."
        ),
    },
    {
        "exam": 4,
        "id": "q22_databricks_notebook_source_comment",
        "question": (
            "A data engineer has noticed the comment '# Databricks notebook source' on the first line of each Databricks Python file's source code pushed to GitHub.\n\n"
            "Which of the following explain the purpose of this comment ?"
        ),
        "options": [
            "This comment makes it easier for humans to understand the source of the generated code from Databricks",
            "This comment add the Python file to the search index in Databricks workspace",
            "This comment is used by Python auto-generated documentation",
            "This comment establishes the Python files as Databricks notebooks"
        ],
        "answer": "This comment establishes the Python files as Databricks notebooks",
        "explanation": (
            "You can convert Python, SQL, Scala, and R scripts to single-cell notebooks by adding a comment to the first cell of the file: # Databricks notebook source"
        ),
    },
    {
        "exam": 4,
        "id": "q23_deletion_vectors_update",
        "question": (
            "A data engineer is working with a large Delta Lake table that has deletion vectors enabled. Considering the underlying mechanics of Delta Lake and its handling of updates, which of the following statements most accurately describes how update operations behave within this table directory?"
        ),
        "options": [
            "The update operation directly modifies the existing Parquet files in place without creating new files.",
            "Each update triggers a complete rewrite of all Parquet files that contain the affected data.",
            "The affected rows are flagged as deleted in the deletion vectors, and the updated rows are written as new Parquet files.",
            "Update operations are ignored entirely when deletion vectors are enabled."
        ],
        "answer": "The affected rows are flagged as deleted in the deletion vectors, and the updated rows are written as new Parquet files.",
        "explanation": (
            "When deletion vectors are enabled in a Delta Lake table, update operations do not rewrite entire Parquet files or modify them in place. Instead, Delta Lake leverages deletion vectors to efficiently track which rows are soft deleted without physically removing them from the data files. During an update, the original rows that require modification are marked as deleted within the deletion vectors, while the updated versions of those rows are written as new rows within Parquet files. This approach allows Delta Lake to perform updates and deletes more efficiently by avoiding costly file rewrites, improving performance especially for large datasets, while still maintaining ACID transaction guarantees and data consistency."
        ),
    },
    {
        "exam": 4,
        "id": "q24_spark_ui_sql_spill_size",
        "question": "In Spark UI, which of the following SQL metrics is displayed on the query's details page?",
        "options": [
            "Query duration",
            "Spill size",
            "Succeeded jobs",
            "Query execution time"
        ],
        "answer": "Spill size",
        "explanation": (
            "In Spark UI, the query's details page displays general information about the query execution time, its duration, the list of associated jobs, and the query execution DAG.\n\n"
            "In addition, it shows SQL metrics in the block of physical operators. The SQL metrics can be useful when we want to dive into the execution details of each operator. For example, 'number of output rows' is a SQL metric that is updated output after a Filter operator. 'Spill size' which is the number of bytes spilled to disk from memory in the operator."
        ),
    },
    {
        "exam": 4,
        "id": "q25_autoloader_schema_location",
        "question": (
            "A data engineer has implemented the following Auto Loader stream to incrementally ingest a large volume of JSON files from cloud storage:\n\n"
            'spark.readStream.format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            "    ._____________________ \\\n"
            '    .load("/mnt/incoming/")\n\n'
            "By default, Auto Loader infers the schema by sampling the first 50 GB or 1000 files it discovers. However, the data engineer wants to avoid re-sampling and reduce the cost of schema inference in subsequent runs, while still tracking schema changes over time.\n\n"
            "Which option correctly fills in the blank to meet the specified requirement?"
        ),
        "options": [
            '.option("cloudFiles.schemaLocation", "/path/to/checkpoint")',
            '.option("checkpointLocation", "/path/to/checkpoint")',
            '.option("cloudFiles.schemaEvolutionMode", "addNewColumns")',
            '.option("mergeSchema", true)'
        ],
        "answer": '.option("cloudFiles.schemaLocation", "/path/to/checkpoint")',
        "explanation": (
            'The correct option to fill in the blank is .option("cloudFiles.schemaLocation", "/path/to/checkpoint"). This tells Auto Loader to store the inferred schema in the specified location so that subsequent runs do not need to re-sample the files, reducing the cost of schema inference while still allowing schema changes to be tracked over time.'
        ),
    },
    {
        "exam": 4,
        "id": "q26_rest_api_duplicate_jobs",
        "question": (
            "A data engineer wanted to create the job 'process-sales' using Databricks REST API.\n\n"
            "However, they mistakely send 2 POST requests to the endpoint '/api/2.1/jobs/create'\n\n"
            "Which statement describes the result of these requests ?"
        ),
        "options": [
            'Only the first job will be created in the workspace. The second request will fail with an error indicating that a job named "process-sales" is already created.',
            '2 jobs will be created in the workspace, but the second one will be renamed to "process-sales (1)"',
            '2 jobs named "process-sales" will be created in the workspace, but with different job_id',
            "The second job will overwrite the previous one created using the first request."
        ],
        "answer": '2 jobs named "process-sales" will be created in the workspace, but with different job_id',
        "explanation": (
            "Sending the same job definition in multiple POST requests to the endpoint '/api/2.1/jobs/create' will create a new job for each request, but each job will have its own unique job_id."
        ),
    },
    {
        "exam": 4,
        "id": "q27_dab_resources_grants",
        "question": (
            "A data engineer has the following Databricks Asset Bundle (DAB) project:\n\n"
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
            "Which of the following correctly describes the result of deploying this DAB project?"
        ),
        "options": [
            "It deploys a Databricks App bookstore_app, and a Volume bookstore_volume, and grants the Service Principal associated with the Databricks App read and write access to the Volume.",
            "It generates an error because the reference ${resources.apps.bookstore_app.id} is incorrect and should instead be ${resources.jobs.bookstore_job.id}.",
            "It deploys a Catalog demo_catalog, a Schema demo_schema, a Volume bookstore_volume, and a Databricks App bookstore_app with access to the volume using a 3-level namespace.",
            "It deploys a Volume bookstore_volume and a Service Principal bookstore_app with read and write access to the Volume."
        ],
        "answer": "It deploys a Databricks App bookstore_app, and a Volume bookstore_volume, and grants the Service Principal associated with the Databricks App read and write access to the Volume.",
        "explanation": (
            "The configuration includes a grant statement that correctly references the app's identifier using ${resources.apps.bookstore_app.id}, which ensures that the Service Principal associated with the deployed Databricks App is automatically given both READ_VOLUME and WRITE_VOLUME privileges on this volume. This means the app's service identity can read from and write data to the bookstore volume as part of its operational workflow."
        ),
    },
    {
        "exam": 4,
        "id": "q28_cluster_permissions_manage",
        "question": "Which of the following describes the minimal permissions a data engineer needs to modify permissions of an existing cluster ?",
        "options": [
            'Cluster creation allowed + "Can Restart" privileges on the cluster',
            'Cluster creation allowed + "Can Manage" privileges on the cluster',
            '"Can Manage" privilege on the cluster',
            '"Can Restart" privilege on the cluster'
        ],
        "answer": '"Can Manage" privilege on the cluster',
        "explanation": (
            "You can configure two types of cluster permissions:\n"
            "1- The 'Allow cluster creation' entitlement controls your ability to create clusters.\n"
            "2- Cluster-level permissions control your ability to use and modify a specific cluster. There are four permission levels for a cluster: No Permissions, Can Attach To, Can Restart, and Can Manage. The table lists the abilities for each permission:"
        ),
    },
    {
        "exam": 4,
        "id": "q29_ldp_constraint_fail_update",
        "question": (
            "A data engineer has defined the following data quality constraint in a LDP pipeline:\n\n"
            "CONSTRAINT valid_id EXPECT (id IS NOT NULL) ________________\n\n"
            "Which clause correctly fills in the blank to immediately stop execution when a record violates this constraint?"
        ),
        "options": [
            "ON VIOLATION FAIL UPDATE",
            "ON VIOLATION FAIL PIPELINE",
            "ON VIOLATION DROP ROW",
            "ON VIOLATION STOP"
        ],
        "answer": "ON VIOLATION FAIL UPDATE",
        "explanation": (
            "The correct clause to fill in the blank is ON VIOLATION FAIL UPDATE, as this ensures that any record violating the valid_id constraint prevents the update from proceeding. This enforces strict data quality and prevents downstream processing of invalid records.\n\n"
            "In this case, manual intervention is required before reprocessing. When a pipeline fails because of an expectation violation, you must decide how to handle the invalid data correctly before re-running the pipeline."
        ),
    },
    {
        "exam": 4,
        "id": "q30_streaming_ignore_deletes",
        "question": (
            "The data engineering team has a large Delta table named 'user_posts' which is partitioned over the 'year' column. This table is used as an input streaming source in a streaming job. The streaming query is displayed below with a blank:\n\n"
            "spark.readStream \\\n"
            '    .table("user_posts") \\\n'
            '    .groupBy("post_category", "post_date") \\\n'
            "    .agg( \\\n"
            '        count("*").alias("total_posts_count"), \\\n'
            '        sum("likes").alias("total_likes")) \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", "/path/checkpoint") \\\n'
            '    .table("users_stats")\n\n'
            "They want to remove previous 2 years data from the table without breaking the append-only requirement of streaming sources.\n\n"
            "Which option correctly fills in the blank to enable stream processing from the table after deleting the partitions?"
        ),
        "options": [
            '.withWatermark("year", "INTERVAL 2 YEARS")',
            '.option("ignoreDeletes", True)',
            '.option("ignoreDeletes", "year")',
            '.withWatermark("year", "INTERVAL 2 YEARS")'
        ],
        "answer": '.option("ignoreDeletes", True)',
        "explanation": (
            "Partitioning on datetime columns can be leveraged when removing data older than a certain age from the table. For example, you can decide to delete previous years data. In this case, the deletion will be effectively a partition-level delete.\n\n"
            "However, if you are using this table as a streaming source, deleting data breaks the append-only requirement of streaming sources, which makes the table no more streamable. To avoid this, you can use the ignoreDeletes option when streaming from this table. This option enables streaming processing from table without parition-level deletes.\n\n"
            'option("ignoreDeletes", True)'
        ),
        
    },
    {
        "exam": 4,
        "id": "q31_cdc_performance_optimization",
        "question": (
            "A data engineer is noticing that a large UC-managed Delta table (~750GB) has become slow when applying intensive CDC feeds.\n\n"
            "Which of the following actions should the data engineer take to improve the performance?"
        ),
        "options": [
            "Partition the table and apply Z-order indexing on the primary keys.",
            "Enable deletion vectors on the table and apply liquid clustering using the primary keys.",
            "Partition the table and apply liquid clustering using the primary keys.",
            "Enable deletion vectors on the table and apply Z-order indexing on the primary keys."
        ],
        "answer": "Enable deletion vectors on the table and apply liquid clustering using the primary keys.",
        "explanation": (
            "Since Change Data Capture (CDC) involves processing updates and deletions, to improve the performance of a large Delta table experiencing slow CDC feeds, the data engineer should enable deletion vectors on the table and apply liquid clustering using the primary keys.\n\n"
            "Enabling deletion vectors allows Delta to efficiently track and manage rows that are deleted or updated without requiring full rewrites of the underlying files, which significantly reduces the overhead for CDC operations. Applying liquid clustering on the CDC merging keys organizes the data physically based on these keys, ensuring that related records are collocated and minimizing the amount of data scanned during updates and deletions. Together, these optimizations help maintain high ingestion and query performance, reduce latency for CDC workloads, and make the table more manageable at scale."
        ),
    },
    {
        "exam": 4,
        "id": "q32_foreachbatch_spark_session_legacy",
        "question": (
            "A data engineer is using a foreachBatch logic to upsert data in a target Delta table.\n\n"
            "The function to be called at each new microbatch processing is displayed below with a blank:\n\n"
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
            "Which option correctly fills in the blank to execute the sql query in the function on a cluster with Databricks Runtime below 10.5 ?"
        ),
        "options": [
            "microBatchDF._jdf.sparkSession().sql(sql_query)",
            "microBatchDF.sql(sql_query)",
            "microBatchDF.sparkSession.sql(sql_query)",
            "spark.sql(sql_query)"
        ],
        "answer": "microBatchDF._jdf.sparkSession().sql(sql_query)",
        "explanation": (
            "Usually, we use spark.sql() function to run SQL queries. However, in this particular case, the spark session can not be accessed from within the microbatch process. Instead, we can access the local spark session from the microbatch dataframe.\n\n"
            "For clusters with Databricks Runtime version below 10.5, the syntax to access the local spark session is:\n"
            "microBatchDF._jdf.sparkSession().sql(sql_query)"
        ),
    },
    {
        "exam": 4,
        "id": "q33_predictive_optimization_unsupported_ops",
        "question": (
            "A data engineering team manages Unity Catalog tables with predictive optimization enabled. They are unsure which operations are automatically performed on these tables as part of predictive optimization's automatic maintenance.\n\n"
            "Which of the following operations is NOT handled automatically by predictive optimization for enabled tables?"
        ),
        "options": [
            "ZORDER",
            "ANALYZE",
            "VACUUM",
            "OPTIMIZE"
        ],
        "answer": "ZORDER",
        "explanation": (
            "Z-order indexing is not handled automatically by predictive optimization for Unity Catalog tables. While predictive optimization can automatically manage the OPTIMIZE, ANALYZE, and VACUUM tasks to maintain table performance, it does not execute ZORDER, and any Z-ordered files are ignored when predictive optimization runs."
        ),
    },
    {
        "exam": 4,
        "id": "q34_sh_magic_command_drawbacks",
        "question": (
            "A junior data engineer is using the %sh magic command to run some legacy code. A senior data engineer has recommended refactoring the code instead.\n\n"
            "Which of the following could explain why a data engineer may need to avoid using the %sh magic command ?"
        ),
        "options": [
            "%sh restarts the Python interpreter. This clears all the variables declared in the notebook",
            "All the listed reasons explain why %sh may need to be avoided",
            "%sh can not access storage to persist the output",
            "%sh executes shell code only on the local driver machine which leads to significant performance overhead."
        ],
        "answer": "%sh executes shell code only on the local driver machine which leads to significant performance overhead.",
        "explanation": (
            "Databricks support the %sh auxiliary magic command to run shell code in notebooks. This command runs only on the Apache Spark driver, and not on the worker nodes."
        ),
    },
    {
        "exam": 4,
        "id": "q35_delta_share_creation_permissions",
        "question": (
            "Which of the following users have the ability to create and manage Delta Shares in Unity Catalog?\n\n"
            "Choose 2 answers:"
        ),
        "options": [
            "Users with MANAGE privilege for the metastore",
            "Account admins",
            "Metastore admins",
            "Workspace admins",
            "Users with CREATE SHARE privilege for the metastore"
        ],
        "answer": [
            "Metastore admins",
            "Users with CREATE SHARE privilege for the metastore"
        ],
        "explanation": (
            "The users who have the ability to create and manage Delta Shares in Unity Catalog are Metastore admins and Users with CREATE SHARE privilege for the metastore, as these roles explicitly have the necessary permissions to create and manage Delta Shares."
        ),
    },
    {
        "exam": 4,
        "id": "q36_unity_catalog_default_privileges",
        "question": (
            "A data engineering team created a new workspace, which is automatically enabled for Unity Catalog. They wanted to create a default workspace catalog and a default schema.\n\n"
            "Which of the following statements correctly describes the default privileges that workspace users have on this catalog and schema?"
        ),
        "options": [
            "Workspace users primarily have CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, and USE SCHEMA privileges on the default schema, along with USE CATALOG on the workspace catalog.",
            "Workspace users have ALL PRIVILEGES on the default schema, along with USE CATALOG on the workspace catalog.",
            "Workspace users have ALL PRIVILEGES on the workspace catalog.",
            "Workspace users do not have any privileges on the default schema by default, unless the workspace administrator explicitly grants them the necessary permissions."
        ],
        "answer": "Workspace users primarily have CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, and USE SCHEMA privileges on the default schema, along with USE CATALOG on the workspace catalog.",
        "explanation": (
            "When a new workspace is created with Unity Catalog enabled, Databricks automatically provisions a default catalog, named workspace, and a default schema, and assigns users a set of basic privileges that allow them to perform common data engineering tasks within that schema.\n\n"
            "Workspace users have the USE CATALOG privilege on the workspace catalog, and specific privileges on the default schema, including: CREATE TABLE, CREATE VOLUME, CREATE FUNCTION, CREATE MATERIALIZED VIEW, CREATE MODEL, USE SCHEMA."
        ),
    },
    {
        "exam": 4,
        "id": "q37_production_job_clusters",
        "question": "For production Databricks jobs, which of the following cluster types is recommended to use?",
        "options": [
            "Production clusters",
            "Job clusters",
            "All-purpose clusters",
            "On-premises clusters"
        ],
        "answer": "Job clusters",
        "explanation": (
            "Job Clusters are dedicated clusters for a job or task run. A job cluster auto-terminates once the job is completed, which saves cost compared to all-purpose clusters.\n\n"
            "In addition, Databricks recommends using job clusters in production so that each job runs in a fully isolated environment."
        ),
    },
    {
        "exam": 4,
        "id": "q38_cluster_permissions_view_ui",
        "question": "Which of the following describes the minimal permissions a data engineer needs to view the metrics and Spark UI of an existing cluster?",
        "options": [
            '"Can Restart" privilege on the cluster',
            '"Can Manage" privilege on the cluster',
            'Cluster creation allowed + "Can Attach To" privileges on the cluster',
            '"Can Attach To" privilege on the cluster'
        ],
        "answer": '"Can Attach To" privilege on the cluster',
        "explanation": (
            "You can configure two types of cluster permissions:\n"
            "1- The 'Allow cluster creation' entitlement controls your ability to create clusters.\n"
            "2- Cluster-level permissions control your ability to use and modify a specific cluster. There are four permission levels for a cluster: No Permissions, Can Attach To, Can Restart, and Can Manage.\n\n"
            "View Spark UI is permitted under \"Can Attach To\"."
        ),
    },
    {
        "exam": 4,
        "id": "q39_spark_functions_extract_date",
        "question": "Which of the following Spark functions is NOT valid for extracting the date from a timestamp column?",
        "options": [
            "CAST(ts AS DATE)",
            "date_part('day', ts)",
            "date_trunc('day', ts)",
            "TO_DATE(ts)"
        ],
        "answer": "date_trunc('day', ts)",
        "explanation": (
            "The valid functions for extracting the date from a timestamp column are:\n"
            "- CAST(ts AS DATE)\n"
            "- TO_DATE(ts)\n"
            "- date_trunc('day', ts) truncates a timestamp to the start of the specified unit, it does not extract only the date portion as a DATE type.\n\n"
            "In addition, date_part can return a numerical component rather than converting to a full date type."
        ),
    },
    {
        "exam": 4,
        "id": "q40_stream_static_exceptall",
        "question": (
            "Given the following query:\n\n"
            'spark.table("stream_sink") \\\n'
            "    .exceptAll( \\\n"
            '        spark.table("stream_data_stage") \\\n'
            '            .dropDuplicates(["id", "row_timestamp"]) \\\n'
            "    ) \\\n"
            "    .write \\\n"
            '    .mode("overwrite") \\\n'
            '    .table("stream_data_stage")\n\n'
            "Which statement describes the result of executing this query ?"
        ),
        "options": [
            'A batch job will overwrite the stream_data_stage table by deduplicated records calculated from all records in the current version of the stream_sink table.',
            "A batch job will overwrite the stream_data_stage table by those deduplicated records from stream_sink that have been added since the last time the job was run.",
            "An incremental job will overwrite the stream_data_stage table by those deduplicated records from stream_sink that have been added since the last time the job was run.",
            "An incremental job will overwrite the stream_sink table by those deduplicated records from stream_data_stage that have been added since the last time the job was run."
        ],
        "answer": "A batch job will overwrite the stream_data_stage table by deduplicated records calculated from all records in the current version of the stream_sink table.",
        "explanation": (
            "Reading a Delta table using spark.table() function means that you are reading it as a static source. So, each time you run the query, all records in the current version of the 'stream_sink' table will be read.\n\n"
            "There is no difference between spark.table() and spark.read.table() function. Actually, spark.read.table() internally calls spark.table().\n\n"
            "The query then writes the data in mode \"overwrite\" to the 'stream_data_stage' table, which completely overwrites the table at each execution."
        ),
    },
    {
        "exam": 4,
        "id": "q41_deprecated_init_scripts_location",
        "question": "Which of the following source locations can no longer be used to store init scripts?",
        "options": [
            "DBFS",
            "Cloud storage",
            "Workspace files",
            "Volumes"
        ],
        "answer": "DBFS",
        "explanation": (
            "As of recent Databricks updates, DBFS (Databricks File System) can no longer be used to store init scripts. Databricks has deprecated the use of DBFS root (dbfs:/) for storing cluster init scripts due to reliability and security concerns.\n\n"
            "Init scripts can now only be stored in the following locations:\n"
            "- Volumes\n"
            "- Cloud storage\n"
            "- Workspace files"
        ),
    },
    {
        "exam": 4,
        "id": "q42_streaming_corrupted_events_handling",
        "question": (
            "An IoT company processes live sensor readings from thousands of devices using a streaming pipeline. Occasionally, devices send corrupted or incomplete events that fail schema validation. The engineering team must ensure that production analytics dashboards, which rely on clean data, continue to update in real time. However, the corrupted records should still be captured for later investigation, using minimal computing resources.\n\n"
            "What should the engineers do to meet these requirements?"
        ),
        "options": [
            "Add retry logic to the main stream so that it attempts to reprocess corrupted messages until they succeed.",
            "Filter out corrupted events in the main real-time stream and write only valid records to the production tables. Create a separate lightweight process that periodically reads and stores the corrupted messages for analysis.",
            "Merge both valid and invalid data into the same Delta table and use downstream queries to apply data quality rules to exclude invalid entries from dashboards.",
            "Include all data, valid or not, in the main stream and use a flag to mark corrupted records."
        ],
        "answer": "Filter out corrupted events in the main real-time stream and write only valid records to the production tables. Create a separate lightweight process that periodically reads and stores the corrupted messages for analysis.",
        "explanation": (
            "The engineers should filter out corrupted or incomplete events from the main real-time streaming pipeline and write only the valid records to the production analytics tables, ensuring that dashboards continue to update accurately and without delay. At the same time, they should implement a separate lightweight process that periodically collects and stores the corrupted messages for later investigation, such as debugging or auditing.\n\n"
            "This design maintains the high reliability and performance of the real-time analytics system by preventing invalid data from affecting dashboards, while still preserving all incoming data for offline analysis, and it does so efficiently without overloading computing resources or complicating the main pipeline."
        ),
    },
    {
        "exam": 4,
        "id": "q43_apply_column_mask",
        "question": (
            "A data engineer at a global bank manages a Delta Lake table customer_accounts with columns: customer_id, name, account_number, credit_card. They want to apply a mask on the credit_card column so that only analysts in the Fraud Detection Department can view the actual values. To achieve this, they implemented the following user-defined function:\n\n"
            "CREATE FUNCTION card_mask(credit_card STRING)\n"
            "RETURN CASE WHEN is_account_group_member('fraud_detection') THEN credit_card\n"
            "            ELSE '****-****-****-****' END;\n\n"
            "Which command can the data engineer use to apply this function as a column mask to the table?"
        ),
        "options": [
            "ALTER TABLE customer_accounts SET MASK card_mask ON (credit_card);",
            "ALTER TABLE customer_accounts SET MASK card_mask;",
            "ALTER TABLE customer_accounts ALTER COLUMN credit_card SET MASK card_mask;",
            "SET MASK card_mask ON TABLE customer_accounts TO COLUMN credit_card;"
        ],
        "answer": "ALTER TABLE customer_accounts ALTER COLUMN credit_card SET MASK card_mask;",
        "explanation": (
            "To ensure that only analysts in the Fraud Detection Department can view the actual credit card numbers while others see masked values, the data engineer should apply the masking function directly to the specific column in the Delta Lake table. The correct SQL command to achieve this is:\n\n"
            "ALTER TABLE customer_accounts ALTER COLUMN credit_card SET MASK card_mask;\n\n"
            "This command modifies the existing credit_card column by associating it with the card_mask function, which conditionally reveals or masks the credit card data based on the user's group membership."
        ),
    },
    {
        "exam": 4,
        "id": "q44_autoloader_schema_evolution_addnewcolumns",
        "question": (
            "A data engineer is configuring the following Databricks Auto Loader stream to ingest JSON data from an S3 bucket:\n\n"
            "spark.readStream \\\n"
            '    .format("cloudFiles") \\\n'
            '    .option("cloudFiles.format", "json") \\\n'
            '    .option("cloudFiles.schemaLocation", "/path/to/checkpoint/dir") \\\n'
            "    .____________________ \\\n"
            '    .load("s3://bucket/data/") \\\n'
            "    .writeStream \\\n"
            '    .option("checkpointLocation", "/path/to/checkpoint/dir") \\\n'
            '    .start("sales_data")\n\n'
            "The pipeline should fail when new columns are detected in the incoming data, but these new columns should still be added to the schema so that subsequent runs can resume successfully with the updated schema. Existing columns must retain their data types.\n\n"
            "Which option correctly fills in the blank to meet the specified requirement?"
        ),
        "options": [
            "failOnNewColumns",
            "rescue",
            "none",
            "addNewColumns"
        ],
        "answer": "addNewColumns",
        "explanation": (
            "The addNewColumns mode is the default schema evolution behavior in Auto Loader. In this mode, when a new column is detected, the stream fails, but the new column is added to the schema. This allows the job to be restarted and continue processing with the updated schema. Importantly, existing column data types are not changed."
        ),
    },
    {
        "exam": 4,
        "id": "q45_pyspark_window_cumulative_average",
        "question": (
            "A data engineer in an international school has implemented the following PySpark code:\n\n"
            "from pyspark.sql.window import Window\n"
            "from pyspark.sql.functions import avg, col\n\n"
            'window_spec = Window.partitionBy("student_id").orderBy("exam_date") \\\n'
            "    .rowsBetween(Window.unboundedPreceding, Window.currentRow)\n\n"
            'df_new = df_student_results.withColumn("avg_score",\n'
            '    avg(col("score")).over(window_spec))\n\n'
            "Which of the following correctly describes what this code does?"
        ),
        "options": [
            "It adds a column showing the cumulative average score of each student from their first exam up to and including the current exam.",
            "It adds a column showing the cumulative average score of each student from the first enrolled student to and including the current student.",
            "It adds a column showing the overall average score of each student, ordered by exam date.",
            "It adds a column showing the overall average score of each exam, regardless of student."
        ],
        "answer": "It adds a column showing the cumulative average score of each student from their first exam up to and including the current exam.",
        "explanation": (
            "The PySpark code uses a Window function to calculate a cumulative or running average score for each student.\n\n"
            "1. Window.partitionBy(\"student_id\"): This divides the data into partitions (groups) based on the student_id.\n"
            "2. .orderBy(\"exam_date\"): This sorts the rows within each student's partition by the exam_date (oldest to newest).\n"
            "3. .rowsBetween(Window.unboundedPreceding, Window.currentRow): This defines the window frame for the calculation as cumulative.\n"
            "4. avg(col(\"score\")).over(window_spec): The avg function is applied over the defined window_spec to get the cumulative average up to that specific exam date."
        ),
    },
    {
        "exam": 4,
        "id": "q46_dab_cicd_commands",
        "question": (
            "A data engineering team at an enterprise organization has recently completed the setup of a new Databricks Asset Bundle project. After successfully configuring the bundle with their CI/CD system, the team wants to ensure that future automated deployments to the production environment run smoothly and reliably.\n\n"
            "In this scenario, which of the following commands should the CI/CD pipeline avoid rerunning during subsequent deployments?"
        ),
        "options": [
            "databricks bundle run",
            "databricks bundle deploy",
            "databricks bundle validate",
            "databricks bundle init"
        ],
        "answer": "databricks bundle init",
        "explanation": (
            "The CI/CD pipeline should avoid rerunning the databricks bundle init command during subsequent deployments because it is only used once to initialize a new Databricks Asset Bundle project by creating its configuration and structure. Re-running it could overwrite existing configurations or reset the project setup. In contrast, commands like databricks bundle validate, databricks bundle deploy, and databricks bundle run are safe and appropriate for repeated use in automated deployment pipelines."
        ),
    },
    {
        "exam": 4,
        "id": "q47_autoloader_definition",
        "question": "Which of the following statements best describes Auto Loader?",
        "options": [
            "Auto loader enables efficient insert, update, deletes, and rollback capabilities by adding a storage layer that provides better data reliability to data lakes.",
            "Auto loader allows applying Change Data Capture (CDC) feed to update tables based on changes captured in source data.",
            "Auto loader monitors a source location, in which files accumulate, to identify and ingest only new arriving files with each command run. While the files that have already been ingested in previous runs are skipped.",
            "Auto loader allows cloning a source Delta table to a target destination at a specific version."
        ],
        "answer": "Auto loader monitors a source location, in which files accumulate, to identify and ingest only new arriving files with each command run. While the files that have already been ingested in previous runs are skipped.",
        "explanation": (
            "Auto Loader incrementally and idempotently processes new data files as they arrive in cloud storage and load them into a target Delta Lake table."
        ),
    },
    {
        "exam": 4,
        "id": "q48_describe_extended_table_comment",
        "question": (
            "A data engineer created a new table along with a comment using the following query:\n\n"
            "CREATE TABLE payments\n"
            "COMMENT 'This table contains sensitive information'\n"
            "AS SELECT * FROM bank_transactions\n\n"
            "Which of the following commands allows the data engineer to review the comment of the table?"
        ),
        "options": [
            "DESCRIBE TABLE payments",
            "DESCRIBE EXTENDED payments",
            "SHOW TBLPROPERTIES payments",
            "SHOW COMMENTS payments"
        ],
        "answer": "DESCRIBE EXTENDED payments",
        "explanation": (
            "DESCRIBE TABLE EXTENDED or simply DESCRIBE EXTENDED allows you to show none only table's comment, but also columns' comments, and other custom table properties."
        ),
    },
    {
        "exam": 4,
        "id": "q49_secret_scope_usage_roles",
        "question": (
            "The data engineering team has a secret scope named \"prod-scope\" that contains sensitive secrets in a production workspace.\n\n"
            "A data engineer in the team is writing a security and compliance documentation, and wants to explain who could use the secrets in this secret scope.\n\n"
            "Which of the following roles is able to use the secrets in the specified secret scope ?"
        ),
        "options": [
            "Workspace Administrators",
            "Secret creators",
            "Users with READ or MANAGE permission on the secret scope",
            "All the mentioned roles are able to use the secrets in the secret scope"
        ],
        "answer": "All the mentioned roles are able to use the secrets in the secret scope",
        "explanation": (
            "Administrators*, secret creators, and users granted access permission can use Databricks secrets. The secret access permissions are as follows:\n\n"
            "- MANAGE - Allowed to change ACLs, and read and write to this secret scope.\n"
            "- WRITE - Allowed to read and write to this secret scope.\n"
            "- READ - Allowed to read this secret scope and list what secrets are available.\n\n"
            "Each permission level is a subset of the previous level's permissions (that is, a principal with WRITE permission for a given scope can perform all actions that require READ permission).\n\n"
            "* Workspace administrators have MANAGE permissions to all secret scopes in the workspace."
        ),
    },
    {
        "exam": 4,
        "id": "q50_unity_catalog_masking_consistency",
        "question": (
            "A data governance team notices that different business units have implemented their own versions of masking policies on the same columns. How does Unity Catalog improve this situation?"
        ),
        "options": [
            "It provides a single source of truth for masking functions, preventing inconsistent exposure.",
            "It lets each team manage its version of masking rules, increasing control over data privacy.",
            "It allows teams to disable masking for testing purposes, providing more flexibility during development.",
            "It allows teams to leverage data object privileges to mask data differently for different groups."
        ],
        "answer": "It provides a single source of truth for masking functions, preventing inconsistent exposure.",
        "explanation": (
            "Unity Catalog improves this situation by providing a single source of truth for masking functions, ensuring that all business units use consistent and centrally governed masking policies across the entire data estate.\n\n"
            "In Unity Catalog, masking logic can be implemented and managed as user-defined functions (UDFs), which encapsulate the masking rules in reusable and standardized code. This means that instead of each team creating its own version of a masking rule, a single, validated UDF can be registered and referenced by all teams. As a result, Unity Catalog enforces consistent data governance, enhances compliance, and reduces the risk of inadvertent exposure of sensitive information across the organization."
        ),
    },
    {
        "exam": 4,
        "id": "q51_foreach_task_efficiency",
        "question": (
            "A junior data engineer creates a Databricks job with 15 notebook tasks, each performing the same data validation logic on 15 different tables. Each task depends on the completion of the previous one, making the workflow long and difficult to maintain.\n\n"
            "What would be a more efficient and scalable solution for this use case?"
        ),
        "options": [
            "Schedule 15 separate jobs instead of having multiple tasks in one job",
            "Configure the 15 notebook tasks to run in parallel, each with a separate cluster configuration",
            "Combine all table validations into one large notebook and loop through all tables sequentially",
            "Use a foreach task to run the same validation notebook for each table in parallel, passing the table name as a parameter"
        ],
        "answer": "Use a foreach task to run the same validation notebook for each table in parallel, passing the table name as a parameter",
        "explanation": (
            "A more efficient and scalable solution in this scenario is to use a For Each task. The For Each task allows you to run a nested task in a loop, passing different parameters to each iteration. In this case, the data engineer can pass each table name as a parameter, running the same validation notebook for all tables. This approach reduces maintenance overhead, and allows the validations to run concurrently without sequential dependencies, making the workflow faster and easier to manage."
        ),
    },
    {
        "exam": 4,
        "id": "q52_modify_privilege_abilities",
        "question": (
            "A data engineer uses the following SQL query:\n\n"
            "GRANT MODIFY ON TABLE employees TO hr_team\n\n"
            "Which of the following describes the ability given by the MODIFY privilege ?"
        ),
        "options": [
            "It gives the ability to add data from the table",
            "It gives the ability to delete data from the table",
            "All the listed abilities are given by the MODIFY privilege",
            "It gives the ability to modify data in the table"
        ],
        "answer": "All the listed abilities are given by the MODIFY privilege",
        "explanation": (
            "The MODIFY privilege gives the ability to add, delete, and modify data to or from an object."
        ),
    },
    {
        "exam": 4,
        "id": "q53_delta_time_travel_except",
        "question": (
            "The data engineering team has a Delta Lake table named 'daily_activities' that is completely overwritten each night with new data received from the source system.\n\n"
            "For auditing purposes, the team wants to set up a post-processing task that uses Delta Lake Time Travel functionality to determine the difference between the new version and the previous version of the table. They start by getting the current version from the transaction log:\n\n"
            'current_version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY daily_activities)").collect()[0][0]\n\n'
            "Which of the following queries can be used by the team to complete this task ?"
        ),
        "options": [
            "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities AS OF VERSION (current_version-1)",
            "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities AS VERSION = (current_version-1)",
            "SELECT * FROM daily_activities\nMINUS\nSELECT * FROM daily_activities AS VERSION = (current_version-1)",
            "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities VERSION AS OF (current_version-1)"
        ],
        "answer": "SELECT * FROM daily_activities\nEXCEPT\nSELECT * FROM daily_activities VERSION AS OF (current_version-1)",
        "explanation": (
            "Every modification to a Delta Lake table creates a new table version. You can use history information to audit operations or query a table at a specific point in time using VERSION AS OF.\n\n"
            "Using the EXCEPT set operator, you can get the difference between the new version and the previous version of the table."
        ),
    },
    {
        "exam": 4,
        "id": "q54_assertions_definition",
        "question": "Which of the following statements correctly describes assertions in unit testing?",
        "options": [
            "An assertion is a boolean expression that checks if code blocks are integrated logically and interacted as a group.",
            "An assertion is a command that shows the differences between the current version of a code unit and the most recently edited version",
            "An assertion is a boolean expression that checks if assumptions made in the code remain true while development.",
            "An assertion is a command that logs failed units of code in production for later debugging and analysis."
        ],
        "answer": "An assertion is a boolean expression that checks if assumptions made in the code remain true while development.",
        "explanation": (
            "Assertions are boolean expressions that enable you to test the assumptions you have made in your code. They are used in unit tests to check if certain assumptions remain true while you're developing your code.\n\n"
            "assert func() == expected_value"
        ),
    },
    {
        "exam": 4,
        "id": "q55_location_keyword_external_tables",
        "question": (
            "The data engineering team is using the LOCATION keyword for every new Delta Lake table created in the Lakehouse.\n\n"
            "Which of the following describes the purpose of using the LOCATION keyword in this case ?"
        ),
        "options": [
            "The LOCATION keyword is used to define the created Delta Lake tables as external database.",
            "The LOCATION keyword is used to configure the created Delta Lake tables as external tables.",
            "The LOCATION keyword is used to set a default schema and checkpoint location for the created Delta Lake tables.",
            "The LOCATION keyword is used to configure the created Delta Lake tables as managed tables."
        ],
        "answer": "The LOCATION keyword is used to configure the created Delta Lake tables as external tables.",
        "explanation": (
            "External (unmanaged) tables are tables whose data is stored in an external storage path by using a LOCATION clause."
        ),
    },
    {
        "exam": 4,
        "id": "q56_delta_append_only_property",
        "question": (
            "A data engineer is responsible for managing a bronze Delta Lake table in Unity Catalog. As part of maintaining data integrity and enforcing governance policies, the engineer wants to restrict modifications to the table by disabling UPDATE and DELETE operations.\n\n"
            "Which of the following commands can the data engineer use to enforce this restriction?"
        ),
        "options": [
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.appendOnly' = 'true');",
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.disableUpdate' = 'true');",
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.preventModification' = 'true');",
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.disableUpdate' = 'true', 'delta.disableDelete' = 'true');"
        ],
        "answer": "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.appendOnly' = 'true');",
        "explanation": (
            "The data engineer can disable UPDATE and DELETE operations on the bronze Delta Lake table by setting the table to append-only mode, which prevents modifications while still allowing inserts. The correct command for this is:\n\n"
            "ALTER TABLE bronze_raw SET TBLPROPERTIES ('delta.appendOnly' = 'true');\n\n"
            "delta.appendOnly is the recognized Delta Lake property to disable updates and deletes, whereas the other options are not valid Delta table properties."
        ),
    },
    {
        "exam": 4,
        "id": "q57_checkpointing_not_shared",
        "question": "Which statement regarding checkpointing in Spark Structured Streaming is Not correct?",
        "options": [
            "Checkpoints stores the current state of a streaming job to cloud storage",
            "Checkpointing with write-ahead logs mechanism ensure fault-tolerant stream processing",
            "Checkpointing allows the streaming engine to track the progress of a stream processing",
            "Checkpoints can be shared between separate streams"
        ],
        "answer": "Checkpoints can be shared between separate streams",
        "explanation": (
            "Checkpoints cannot be shared between separate streams. Each stream needs to have its own checkpoint directory to ensure processing guarantees."
        ),
    },
    {
        "exam": 4,
        "id": "q58_dab_deployment_bind",
        "question": (
            "A data engineer has an existing Databricks job and wants to manage it using Databricks Asset Bundles. They have already generated the YAML definition of the job and downloaded its referenced artifacts. However, they want to ensure that updates to the bundle's YAML will modify the existing job rather than creating a new job.\n\n"
            "Which of the following commands allows the data engineer to achieve this?"
        ),
        "options": [
            "databricks bundle deployment link <bundle_job> <remote-job-id>",
            "databricks bundle deployment bind <bundle_job> <remote-job-id>",
            "databricks bundle deployment match <bundle_job> <remote-job-id>",
            "databricks bundle deployment mirror <bundle_job> <remote-job-id>"
        ],
        "answer": "databricks bundle deployment bind <bundle_job> <remote-job-id>",
        "explanation": (
            "The correct command is: databricks bundle deployment bind <bundle_job> <remote-job-id>. This links the existing remote job to a defined resource in the Databricks Asset Bundle, ensuring that any updates to the bundle's YAML definition will modify the linked job rather than creating a new one."
        ),
    },
    {
        "exam": 4,
        "id": "q59_delta_statistics_high_cardinality_string",
        "question": (
            "The data engineering team has a large Delta table named 'user_messages' with the following schema:\n\n"
            "msg_id INT, user_id INT, msg_time TIMESTAMP, msg_title STRING, msg_body STRING\n\n"
            "The msg_body field represents user messages in free-form text. The table has a performance issue when it's queried with filters on this field.\n\n"
            "Which of the following could explain the reason for this performance issue ?"
        ),
        "options": [
            "The table does not leverage file skipping because it's not optimized with Z-ORDER on the msg_body column.",
            "The table does not leverage file skipping because it's not partitioned on the msg_body column.",
            "The table does not leverage file skipping because Delta Lake statistics are uninformative for string fields with very high cardinality",
            "The table does not leverage file skipping because Delta Lake statistics are not captured on columns of type STRING"
        ],
        "answer": "The table does not leverage file skipping because Delta Lake statistics are uninformative for string fields with very high cardinality",
        "explanation": (
            "The msg_body field represents user messages in free-form text. That means it has a very high cardinality. The statistics gathered on this column by Delta Lake are generally uninformative and useless for data skipping."
        ),
    }
]