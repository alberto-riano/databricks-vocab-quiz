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
]