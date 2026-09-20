# Сводная таблица: механизм → инструменты

Обратный индекс к [исследованию](../../table-splitting-research-2026-09-20.md),
20 сентября 2026. Один инструмент может встречаться в нескольких строках:
способ выбора границ, адаптация размера и согласование с CDC — разные свойства.
Указана исследованная реализация/поставка, а не все коннекторы продукта.

Основания и точные ссылки: [основной аудит кода](CODE-REPORT.md),
[дополнительный аудит](ADDITIONAL-CODE.md), [коммерческая документация](COMMERCIAL.md),
[смежные компоненты](COVERAGE.md). Коммерческие реализации подтверждены публичными
описаниями; их закрытый код не проверялся. Список охватывает подтверждённые
механизмы исследования, а не все существующие инструменты.

## Как выбирают части исходной таблицы

| Механизм | Инструменты и конкретные варианты |
|---|---|
| **Равные интервалы значений ключа: MIN/MAX, lower/upper bounds, stride** | DataX; Spark JDBC; Flink JDBC; Beam JdbcIO; Sqoop; ConnectorX → Polars и dlt; Dask; Daft `MIN_MAX`; pgloader для MySQL; pgcopydb по целочисленному ключу; архивный Apex/Malhar JDBC; ChunJun `range`; SeaTunnel fixed ranges; Transferia Go PostgreSQL — fallback для sequence key; PeerDB QRep min/max; NiFi value partitioning. Через соответствующие движки: Hudi JDBC, Talend Spark `tJDBCInput`, Dataflow `JdbcIO`. Коммерческие: Cloud Data Fusion, Alibaba DataWorks, Azure/Fabric Data Factory `DynamicRange`. |
| **Диапазоны фиксированной ширины: число или время** | Sling **Pro/Platform** `chunk_size`; IBM StreamSets partition processing по одному offset column; CData Sync numeric/date partition key. У каждого продукта свои единицы размера и поддерживаемые типы. |
| **Квантили, NTILE, гистограммы, границы примерно по числу строк** | Transferia Go PostgreSQL `percentile_disc`; Daft `PERCENTILE_DISC`; PeerDB QRep `NTILE`; Gobblin Salesforce histogram; Etlworks 9.7.11+ — COUNT и упорядоченный проход по PK с выбором границ по позициям строк. |
| **Гибрид: равные числовые интервалы либо поиск следующей границы по данным** | Flink CDC; InLong PostgreSQL CDC; SeaTunnel DynamicChunkSplitter; RisingWave PostgreSQL parallel CDC backfill. Выбор пути зависит от типа ключа, распределения и конкретного коннектора. |
| **Keyset chunks: следующий ключ после последнего, с ORDER BY и LIMIT** | Debezium incremental snapshot; Sequin; Estuary PostgreSQL keyed backfill. Это последовательные порции в проверенных путях, а не автоматическое доказательство нескольких одновременных readers одной таблицы. |
| **Индексные/PK ranges, но точная формула границ в описании не установлена** | Fivetran index-based import; DBConvert Streams; Azure PostgreSQL migration service; Databricks Lakeflow query-based connectors; Integrate.io; Huawei CDM; Palantir Foundry custom JDBC. Не приписываем им квантили или равный stride без отдельного подтверждения. |
| **SQL hash / modulo** | Transferia Go PostgreSQL; ChunJun `mod`; Ray Data SQL; SeaTunnel `HASH`; Ora2Pg — MOD выбранного ключа; AWS Glue JDBC; HVR modulo/count expression; Starburst Enterprise Oracle `ORA_HASH`. Пользовательские выражения: Sling **Pro/Platform** `chunk_expr`, Hazelcast JDBC `resultSetFn`. |
| **Строковые диапазоны с отдельной логикой типов/сравнения** | Sqoop TextSplitter; общий RDBMS utility DataX (проверять конкретный reader); SeaTunnel — legacy charset/collation и ограниченный MySQL RANGE/AUTO; mydumper string chunker; MySQL Shell index chunking. Явные строковые границы: Informatica CDI ODBC. |
| **Составной ключ: лексикографические tuple ranges/cursors** | MySQL Shell; Sequin; Etlworks automatic partitioning. Пользовательские многоколоночные boundaries доступны в документированных range-режимах AWS DMS/Qlik Replicate. Поддержка composite PK вообще не означает composite splitter во всех продуктах. |
| **PostgreSQL физические диапазоны CTID / heap pages** | DuckDB PostgreSQL extension; pgcopydb; pgstream; PeerDB; Feldera через `feldera/etl`; архивный pg_flo v0.0.15. Коммерческие/документированные: Fivetran, Airbyte PostgreSQL, Artie, ClickPipes PostgreSQL. Estuary — keyless CTID chunks со своими ограничениями. **Наличие CTID chunks не доказывает одинаковую параллельность, snapshot-consistency или resume-гарантии.** |
| **Oracle ROWID / extents** | Transferia Go Oracle — диапазоны по extents; Google Datastream Oracle — использование ROWID подтверждено, точный planner диапазонов не раскрыт. |
| **Уже существующие partitions / subpartitions исходной таблицы** | Transferia Go MySQL и PostgreSQL inheritance; mydumper; AWS DMS; Qlik Replicate; SAP Data Services; Oracle Data Integrator; Quest SharePlex copy; Starburst Oracle/SingleStore. Физические PostgreSQL readers также могут сначала раскрывать parent до leaf relations: PeerDB, Feldera/ETL. |
| **Нативные distributed regions / tablets / segments** | Dumpling для TiDB — region/handle boundaries; TiCDC — spans по числу regions или весу WrittenBytes; CockroachDB changefeeds — KV/ranges; Yandex Data Transfer Greenplum — чтение сегментов. Это source-native механизмы, не универсальный JDBC splitter. |
| **Случайная выборка идентификаторов → ranges коллекции** | Transferia Go MongoDB — отсортированные sample `_id`; Adiom dsync MongoDB — выборка ID для частей коллекции. Это документные коллекции, не SQL-таблицы. |
| **Серверные segments / partition tokens / PK chunking API** | Gobblin Salesforce Bulk API `PKChunking`; Adiom dsync — source-specific DynamoDB/Cosmos сегменты/планировщики. Серверный протокол определяет допустимое разбиение. |
| **Пользовательские SQL-предикаты / запрос на каждую часть** | DataX `querySql`; Spark JDBC predicates; Dask explicit divisions; Adiom dsync SQL `PartitionQuery`; Hazelcast JDBC `resultSetFn`; NiFi generated queries; Oracle GoldenGate `SQLPREDICATE`; HVR boundary/series/count slicing; Syniti Refresh filters; Etlworks Partition SQL; AWS DMS ranges; Qlik Replicate ranges; Informatica CDI ODBC ranges; SAP Datasphere manual conditions; K2view Fabric — документированный шаблон range parsers. Полнота и непересечение не гарантируются самим фактом наличия SQL interface. |
| **LIMIT/OFFSET / страницы по позиции строки** | NiFi GenerateTableFetch; Logstash JDBC paging; Mage SQL ingestion; Grouparoo PostgreSQL query import. IBM DataStage DB2 документирует также modulo по ROW_NUMBER. У последовательного paging нет автоматического intra-table parallelism. |
| **Экспорт источником в файлы → параллельное чтение файлов** | Starburst Redshift `UNLOAD` → S3/Parquet. Это отдельный физический путь, с промежуточным storage, а не несколько JDBC ranges. |

## Дополнительные свойства: не отдельные способы выбора границ

| Механизм | Инструменты и конкретные варианты |
|---|---|
| **Адаптация размера по времени/оценке стоимости** | mydumper — размер следующего chunk по длительности; MySQL Shell — bytesPerChunk/средняя ширина и адаптивные EXPLAIN-пробы; Fivetran block import — уменьшение chunk после ошибки. Это разные виды адаптации. |
| **Ориентация на байты при планировании** | MySQL Shell; Streamkap snapshot chunk settings; pgstream batch bytes; Transferia Go desired table size. Оценка байтов не равна строгому лимиту или гарантии одинакового времени. |
| **Очередь множества частей на меньшее число readers** | Feldera/ETL; pgstream; pg_flo; mydumper. Etlworks документирует число заданий как threads × multiplier. Помогает уменьшать хвосты без увеличения числа соединений. |
| **Watermark chunks + согласование с CDC** | Debezium incremental snapshot; Flink CDC; семейство InLong CDC; Estuary; Artie по техническому описанию. RisingWave интегрирует parallel CDC backfill с barriers/state. Конкретные протоколы различаются; это не одна общая реализация DBLog. |
| **Общий экспортированный PostgreSQL snapshot для частей** | DuckDB PostgreSQL extension; pgstream (schema-session); Feldera/ETL; pg_flo; согласованный snapshot-путь PeerDB/ClickPipes. Сам CTID без такого протокола этого свойства не даёт. |
| **Отдельная обработка NULL / остатка пространства** | DataX — NULL task; Spark JDBC — NULL в первом диапазоне; MySQL Shell — NULL nullable index в первом chunk; PeerDB — настраиваемая NULL partition; Foundry — отдельный NULL query; SAP Datasphere — residual partition для не покрытых ручными условиями строк. |

## Что не нужно смешивать с разбиением чтения одной SQL-таблицы

| Механизм | Инструменты и конкретные варианты |
|---|---|
| **Параллельно разные таблицы, а не части одной** | Debezium initial snapshot thread pool; Striim DatabaseReader `ParallelThreads`; Confluent JDBC table task assignment; PostgreSQL tablesync workers. Это характеристика проверенного пути, не всех режимов продукта. |
| **Разделение уже прочитанных строк / buffers / сообщений** | Hop ModPartitioner; SSIS Balanced Data Distributor; Kafka Streams partitions; транспортные batches SymmetricDS; fetch batches Embulk/KNIME. Не увеличивает количество независимых source scans само по себе. |
| **Собственная lake/file table: файлы, buckets, key sections** | Paimon — bin packing файлов и key-range sections; Fluss — buckets/hybrid splits; Hudi — собственные file groups. Возможности чтения внешней JDBC-таблицы проверяются отдельно. |
| **Оркестрация переданных задач / делегирование reader** | Meltano → Singer tap; CloudQuery → source plugin; Dagster/Argo/Airflow/Kestra → пользовательские задачи/операторы. Оркестратор не обязан сам вычислять SQL boundaries. |
| **Chunking заявлен, но алгоритм границ не раскрыт** | Hevo historical load; Huawei DRS; MongoDB Mongosync; автоматические partitions SAP Datasphere; intra-table copy YDB в Yandex Data Transfer; Streamkap — byte/chunk параметры без полного описания boundary planner. Не классифицируем их как hash/quantile/CTID по догадке. |

Общие caveats: диапазоны одинаковой ширины не означают одинаковые объёмы;
несколько ranges не доказывают общий snapshot; chunks не обязательно исполняются
параллельно; hash без подходящего access path может многократно сканировать
таблицу. Строки таблицы фиксируют механизмы, а не рейтинг производительности.
