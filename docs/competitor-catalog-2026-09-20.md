# Каталог конкурентов и альтернатив Transferia Rust

Дата исследования: **20 сентября 2026 года**.

Каталог охватывает batch, CDC, очереди, streaming, ETL/ELT, lakehouse ingestion,
интеграции приложений и reverse ETL. Ограничения на количество позиций нет.
Это карта конкурентного поля, а не рейтинг популярности или доли рынка.
Абсолютная полнота всех существующих инструментов не гарантируется.

Ссылки ведут на официальные сайты, документацию и репозитории, использованные
при исследовании. Описания обозначают основное пересечение сценариев, а не
полную матрицу возможностей. Наличие в каталоге не означает одинаковую зрелость,
активность разработки или взаимозаменяемость. Для отдельных редакций,
коннекторов и версий условия эксплуатации и лицензии могут отличаться.

**Обозначения:** **O** — open source; **S** — source-available;
**К** — коммерческий продукт или сервис; **М** — смешанное предложение.
Source-available не приравнивается к open source. Это обзор моделей
распространения, а не аудит лицензий всех компонентов.

**Различие проектов:** Transferia Rust в этом репозитории и
[Transferia на Go](https://github.com/transferia/transferia) — разные реализации.
У Go-проекта указана лицензия Apache-2.0, описаны snapshot, replication,
snapshot + replication, интеграции с БД, Kafka и object storage. По уточнению
владельца исследования он рассматривается вместе с
[Yandex Data Transfer](https://yandex.cloud/en/docs/data-transfer/): открытый
движок и связанный управляемый коммерческий сервис. Их возможности и поставки
не следует автоматически считать идентичными.

Связанный документ: [функциональные пробелы Transferia Rust относительно конкурентов](competitor-feature-gaps-2026-09-19.md).

## Список категорий

1. [Универсальные платформы переноса данных, ELT и ingestion](#universal-ingestion)
2. [Корпоративные ETL/ELT и визуальная интеграция](#enterprise-etl)
3. [Открытые коннекторные движки и инструменты ingestion](#open-ingestion)
4. [Специализированный CDC и гетерогенная репликация](#cdc-replication)
5. [Управляемые облачные CDC и миграции БД](#cloud-cdc)
6. [Облачные ETL и платформы загрузки в lakehouse](#cloud-etl)
7. [Специализированные ingestion-компоненты конкретных приёмников](#sink-ingestion)
8. [Нативная репликация и CDC отдельных СУБД](#native-cdc)
9. [Очереди, коннекторы и перенос сообщений](#messaging)
10. [Управляемые платформы потоковой интеграции](#managed-streaming)
11. [Движки batch/stream processing и непрерывных вычислений](#processing-engines)
12. [IoT, MQTT и edge-интеграция](#iot-edge)
13. [Логи, телеметрия и observability pipelines](#telemetry)
14. [Customer events и CDP](#customer-events)
15. [Reverse ETL и активация данных](#reverse-etl)
16. [Маркетинговый ELT и отраслевые коннекторы](#marketing-elt)
17. [iPaaS, API и интеграция бизнес-приложений](#ipaas)
18. [Российские платформы интеграции и ETL](#regional-platforms)
19. [Массовое копирование, файловые переносы и миграционные утилиты](#bulk-migration)
20. [Оркестрация собственных pipelines](#orchestration)
21. [Вычислительные библиотеки и SQL-преобразования](#processing-libraries)
22. [Федерация и комплексные data platforms](#federation)
23. [Старые названия и приобретённые продукты](#renamed-products)
24. [Архивные решения и проекты с неподтверждённой жизнеспособностью](#legacy-and-unconfirmed)

Категории 1–22 описывают продуктовые и технические классы. Разделы 23–24 —
отдельные списки для учёта переименований, приобретений, прекращённых продуктов
и неопределённости статуса. Продукт обычно указан в основной категории;
пересечения отмечены ссылками или пояснениями, без повторного подсчёта.

<a id="universal-ingestion"></a>
## 1. Универсальные платформы переноса данных, ELT и ingestion

Прямые конкуренты по коннекторному переносу и синхронизации данных.

- [Transferia, Go](https://github.com/transferia/transferia) — **O**. Snapshot, CDC, очереди, object storage; связанный облачный сервис — [Yandex Data Transfer](https://yandex.cloud/en/docs/data-transfer/), **К**.
- [Airbyte](https://airbyte.com/) — **S/К**. Коннекторная загрузка из БД, API и файлов, инкрементальный ELT и CDC.
- [Fivetran](https://www.fivetran.com/) — **К**. Управляемый перенос в warehouse/lakehouse, CDC и SaaS-коннекторы.
- [Estuary Flow](https://estuary.dev/) — **S/К**. Начальная загрузка и непрерывная синхронизация, streaming materialization.
- [Hevo Data](https://hevodata.com/) — **К**. Управляемые pipelines из SaaS и БД.
- [Matillion](https://www.matillion.com/) — **К**. Загрузка и преобразования в облачных аналитических системах.
- [Integrate.io](https://www.integrate.io/) — **К**. ETL, ELT, репликация и reverse ETL.
- [Keboola](https://www.keboola.com/) — **К**. Коннекторы, преобразования и исполнение data workflows.
- [Dataddo](https://www.dataddo.com/) — **К**. No-code перенос между приложениями и хранилищами.
- [CData Sync](https://www.cdata.com/sync/) — **К**. Репликация БД и SaaS, инкрементальная загрузка.
- [Boomi Data Integration, ранее Rivery](https://boomi.com/rivery-is-now-boomi-data-integration/) — **К**. ELT, CDC, преобразования и оркестрация.
- [Etlworks](https://etlworks.com/) — **К**. ETL/ELT, CDC, файлы, API, очереди и EDI.
- [Nexla](https://nexla.com/) — **К**. Универсальная интеграция и подготовка данных.
- [Etleap](https://etleap.com/) — **К**. Управляемые ingestion pipelines, включая Iceberg.
- [Portable](https://portable.io/) — **К**. SaaS/API → аналитические хранилища, включая нишевые источники.
- [Skyvia](https://skyvia.com/) — **К**. Облачная интеграция, импорт, репликация и синхронизация.
- [Polytomic](https://www.polytomic.com/) — **К**. ETL и reverse ETL, операционная синхронизация.
- [Weld](https://weld.app/) — **К**. Full/incremental/CDC загрузка и reverse ETL.
- [TROCCO](https://documents.trocco.io/docs/en/about-managed-etl) — **К**. Управляемая интеграция данных и ELT.
- [DataChannel](https://www.datachannel.co/) — **К**. ELT, reverse ETL и интеграция аналитических данных.
- [Y42](https://www.y42.com/) — **К**. Ingestion, SQL/Python pipelines и оркестрация.
- [CloudQuery](https://www.cloudquery.io/) — **М**. Коннекторная синхронизация, особенно облачных и инфраструктурных данных.

<a id="enterprise-etl"></a>
## 2. Корпоративные ETL/ELT и визуальная интеграция

- [Informatica IDMC / Cloud Data Integration](https://www.informatica.com/products/cloud-integration.html) — **К**. Enterprise-интеграция и преобразования.
- [Qlik Talend Cloud](https://www.qlik.com/us/products/qlik-talend-cloud) — **К**. Перенос, преобразования, качество и управление данными.
- [IBM DataStage](https://www.ibm.com/products/datastage) — **К**. Параллельный enterprise ETL/ELT.
- [IBM StreamSets](https://www.ibm.com/products/streamsets) — **К**. Коннекторные pipelines и обработка изменений схем.
- [Pentaho Data Integration](https://pentaho.com/) — **М**. Визуальный ETL, файлы, БД и batch workflows.
- [CloverDX](https://www.cloverdx.com/) — **К**. Сложные преобразования и автоматизация интеграции.
- [Ab Initio](https://www.abinitio.com/en/data-processing-platform/data-formats-connectors/) — **К**. Масштабная параллельная обработка данных.
- [SAP Data Services](https://www.sap.com/products/data-cloud/data-services.html) — **К**. ETL и качество данных, SAP/non-SAP.
- [Oracle Data Integrator](https://www.oracle.com/middleware/technologies/data-integrator.html) — **К**. ELT с выполнением преобразований на стороне БД.
- [Microsoft SSIS](https://learn.microsoft.com/en-us/sql/integration-services/sql-server-integration-services) — **К**. Batch ETL и загрузка хранилищ.
- [SAS Data Management](https://www.sas.com/en_us/solutions/data-management.html) — **К**. Интеграция и подготовка enterprise-данных.
- [Alteryx](https://www.alteryx.com/) — **К**. Визуальная подготовка данных и аналитические workflows.
- [FME](https://fme.safe.com/) — **К**. Интеграция форматов, особенно геоданных, файлов и API.
- [KNIME](https://www.knime.com/) — **O/К**. Визуальная подготовка и преобразование данных.
- [TimeXtender](https://www.timextender.com/) — **К**. Автоматизация data pipelines на основе метаданных.
- [WhereScape](https://www.wherescape.com/) — **К**. Автоматизация построения и загрузки хранилищ.
- [K2view](https://www.k2view.com/) — **К**. Интеграция и формирование операционных data products.

<a id="open-ingestion"></a>
## 3. Открытые коннекторные движки и инструменты ingestion

- [Apache SeaTunnel](https://seatunnel.apache.org/home/) — **O**. Batch, streaming и CDC.
- [Apache NiFi](https://nifi.apache.org/) — **O**. Визуальные потоки, маршрутизация, буферизация и backpressure.
- [Apache InLong](https://inlong.apache.org/) — **O**. Ingestion, синхронизация и подписки на данные.
- [Alibaba DataX](https://github.com/alibaba/DataX) — **O**. Массовая синхронизация гетерогенных источников.
- [ChunJun, ранее FlinkX](https://github.com/DTStack/chunjun) — **O**. Интеграция данных на Flink.
- [Apache Hop](https://hop.apache.org/) — **O**. Визуальные ETL pipelines.
- [Meltano](https://meltano.com/) — **O/К**. ELT на Singer-коннекторах.
- [dlt](https://dlthub.com/) — **O/К**. Python ingestion из API, файлов и БД.
- [Sling](https://slingdata.io/) — **O/К**. CLI для переноса БД и файлов.
- [ingestr](https://github.com/bruin-data/ingestr) — **O**. Копирование между источниками и приёмниками одной командой.
- [Mage](https://www.mage.ai/) — **O/К**. Загрузка, преобразования и исполнение pipelines.
- [Embulk](https://www.embulk.org/) — **O**. Подключаемые плагины для bulk loading; темп обновлений следует оценивать отдельно.
- [Singer](https://www.singer.io/) — **O**. Экосистема taps/targets и протокол обмена; строительный блок, а не полноценная управляемая платформа.
- [Apache Gobblin](https://github.com/apache/gobblin) — **O**. Distributed ingestion и репликация; актуальность конкретных коннекторов требует отдельной проверки.

<a id="cdc-replication"></a>
## 4. Специализированный CDC и гетерогенная репликация

- [Debezium](https://debezium.io/) — **O**. CDC из журналов БД и первоначальные snapshots.
- [Apache Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) — **O**. Snapshot + CDC pipelines на Flink.
- [Qlik Replicate](https://www.qlik.com/us/products/qlik-replicate) — **К**. Full load и enterprise CDC.
- [Oracle GoldenGate](https://www.oracle.com/integration/goldengate/) — **К**. Репликация, CDC и миграции.
- [IBM Data Replication](https://www.ibm.com/products/data-replication) — **К**. Enterprise CDC.
- [Precisely Connect](https://www.precisely.com/solution/real-time-cdc-and-etl-solutions/) — **К**. CDC/ETL, включая mainframe и IBM i.
- [Striim](https://www.striim.com/) — **К**. CDC и потоковые преобразования.
- [Fivetran HVR](https://fivetran.com/docs/hvr6) — **К**. Self-hosted enterprise-репликация БД и файлов.
- [Quest SharePlex](https://www.quest.com/products/shareplex) — **К**. Репликация Oracle/PostgreSQL и распределение данных.
- [Syniti Data Replication](https://www.syniti.com/solutions/data-replication/) — **К**. Репликация между корпоративными БД.
- [SymmetricDS](https://symmetricds.org/) — **O/К**. Одно- и двунаправленная репликация БД.
- [PeerDB](https://github.com/PeerDB-io/peerdb) — **O**. PostgreSQL → аналитические системы и другие приёмники.
- [Sequin](https://sequinstream.com/) — **O/К**. PostgreSQL CDC и backfill в очереди и streams.
- [Streamkap](https://streamkap.com/) — **К**. Управляемая потоковая репликация.
- [Artie](https://www.artie.com/product) — **М**. CDC-платформа для непрерывной репликации.
- [DBConvert Streams](https://streams.dbconvert.com/) — **К**. Миграция, CDC и синхронизация БД.
- [dsync, Adiom](https://github.com/adiom-data/dsync) — **O/К**. Первоначальная загрузка и непрерывная синхронизация.
- [pgstream, Xata](https://github.com/xataio/pgstream) — **O**. Репликация PostgreSQL, snapshots и обработка изменений схем.
- [Alibaba Canal](https://github.com/alibaba/canal) — **O**. Извлечение и доставка изменений MySQL binlog.
- [Maxwell’s daemon](https://github.com/zendesk/maxwell) — **O**. MySQL binlog → JSON-события; специализированный компонент.

<a id="cloud-cdc"></a>
## 5. Управляемые облачные CDC и миграции БД

- [AWS DMS](https://aws.amazon.com/dms/) — **К**. Full load и непрерывная репликация.
- [Google Cloud Datastream](https://cloud.google.com/datastream) — **К**. Управляемый CDC.
- [Google Database Migration Service](https://cloud.google.com/database-migration) — **К**. Миграция БД в Google Cloud.
- [Azure Database Migration Service](https://azure.microsoft.com/en-us/products/database-migration) — **К**. Миграции БД в Azure.
- [Alibaba Cloud DTS](https://www.alibabacloud.com/en/product/data-transmission-service) — **К**. Миграция, синхронизация и подписки.
- [Tencent Cloud DTS](https://www.tencentcloud.com/document/product/571/18135?lang=en) — **К**. Миграция и непрерывная синхронизация.
- [Huawei Cloud DRS](https://www.huaweicloud.com/intl/en-us/product/drs.html) — **К**. Репликация и миграция БД.

Yandex Data Transfer включён вместе с открытым Transferia в [первой категории](#universal-ingestion).

<a id="cloud-etl"></a>
## 6. Облачные ETL и платформы загрузки в lakehouse

- [AWS Glue](https://aws.amazon.com/glue/) — **К**. Serverless ETL, Spark, batch и streaming.
- [Amazon AppFlow](https://aws.amazon.com/appflow/) — **К**. Перенос данных между SaaS и AWS.
- [Azure Data Factory](https://azure.microsoft.com/en-us/products/data-factory) — **К**. Copy pipelines и гибридная интеграция.
- [Fabric Data Factory](https://learn.microsoft.com/en-us/fabric/data-factory/data-factory-overview) — **К**. Интеграция в Microsoft Fabric.
- [Google Cloud Dataflow](https://cloud.google.com/products/dataflow) — **К**. Управляемый Beam для batch/streaming.
- [Google Cloud Data Fusion](https://cloud.google.com/data-fusion) — **К**. Визуальные коннекторные pipelines.
- [Databricks Lakeflow](https://www.databricks.com/product/data-engineering) — **К**. Ingestion, CDC и преобразования.
- [Snowflake Openflow](https://docs.snowflake.com/en/user-guide/data-integration/openflow/about) — **К**. Интеграция на базе NiFi.
- [Cloudera Data Flow](https://www.cloudera.com/products/data-in-motion/dataflow.html) — **К**. Управляемые потоки NiFi.
- [Alibaba DataWorks Data Integration](https://www.alibabacloud.com/help/en/dataworks/user-guide/what-is-dataworks) — **К**. Batch и real-time синхронизация.
- [OCI Data Integration](https://www.oracle.com/integration/data-integration/) — **К**. Интеграция в Oracle Cloud.
- [Huawei Cloud CDM](https://www.huaweicloud.com/intl/en-us/product/cdm.html) — **К**. Массовый перенос гетерогенных данных.
- [Qlik Open Lakehouse](https://www.qlik.com/us/products/qlik-open-lakehouse) — **К**. Ingestion и управление Iceberg; сюда относится направление Upsolver.

<a id="sink-ingestion"></a>
## 7. Специализированные ingestion-компоненты конкретных приёмников

Эти решения могут заменить Transferia для определённого назначения, но не
являются универсальными системами переноса.

- [ClickHouse ClickPipes](https://clickhouse.com/cloud/clickpipes) — **К**. Ingestion в ClickHouse Cloud.
- [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview) — **К**. Потоковая загрузка в Snowflake.
- [SingleStore Flow, ранее BryteFlow](https://www.singlestore.com/blog/the-journey-from-bryteflow-to-singlestore-flow/) — **К**. Перенос и CDC в SingleStore.
- [Redis Data Integration](https://redis.io/docs/latest/integrate/redis-data-integration/) — **К**. Поддержание актуального представления данных в Redis.
- [Microsoft Fabric Mirroring](https://learn.microsoft.com/en-us/fabric/mirroring/overview) — **К**. Непрерывная репликация данных в Fabric.
- [Amazon Data Firehose](https://aws.amazon.com/firehose/) — **К**. Потоковая доставка в storage и аналитические системы.
- [Amazon OpenSearch Ingestion](https://aws.amazon.com/opensearch-service/features/ingestion/) — **К**. Управляемые ingestion pipelines.
- [Apache Hudi Streamer](https://hudi.apache.org/docs/hoodie_streaming_ingestion/) — **O**. Инкрементальная загрузка в Hudi.
- [Apache Paimon](https://paimon.apache.org/) — **O**. Streaming lakehouse и интеграция изменений; инфраструктурная альтернатива.
- [Apache Fluss](https://fluss.apache.org/) — **O**. Потоковое хранилище и интеграция с lakehouse; инфраструктурная альтернатива.

<a id="native-cdc"></a>
## 8. Нативная репликация и CDC отдельных СУБД

Конкуренты для узкого сценария «можно ли решить задачу средствами самой БД».

- [PostgreSQL logical replication](https://www.postgresql.org/docs/current/logical-replication.html) — **O**. Публикации и подписки между PostgreSQL.
- [pglogical](https://github.com/2ndQuadrant/pglogical) — **O**. Расширение логической репликации PostgreSQL.
- [wal2json](https://github.com/eulerto/wal2json) — **O**. Декодирование PostgreSQL WAL в JSON; компонент для собственного CDC.
- [EDB Postgres Distributed](https://www.enterprisedb.com/docs/pgd/latest/) — **К**. Распределённая репликация PostgreSQL.
- [pgEdge](https://www.pgedge.com/) — **М**. Распределённый PostgreSQL и репликация.
- [Bucardo](https://github.com/bucardo/bucardo) — **O**. Специализированная репликация PostgreSQL; поддержку целевых версий проверять отдельно.
- [TiCDC](https://docs.pingcap.com/tidb/stable/ticdc-overview/) — **O**. Доставка изменений TiDB.
- [TiDB Data Migration](https://docs.pingcap.com/tidb/stable/dm-overview/) — **O**. MySQL-совместимые источники → TiDB.
- [MongoDB Mongosync](https://www.mongodb.com/docs/mongosync/current/) — **К**. Синхронизация между MongoDB-кластерами.
- [CockroachDB changefeeds](https://docs.cockroachlabs.com/docs/stable/change-data-capture-overview) — **К**. Выгрузка изменений из CockroachDB.
- [YugabyteDB CDC](https://docs.yugabyte.com/stable/explore/change-data-capture/) — **М**. Извлечение изменений YugabyteDB.

<a id="messaging"></a>
## 9. Очереди, коннекторы и перенос сообщений

- [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) — **O**. Source/sink framework; лицензии коннекторов различаются.
- [Confluent Connectors](https://www.confluent.io/product/connectors/) — **М/К**. Экосистема и управляемые Kafka-коннекторы.
- [Redpanda Connect, ранее Benthos](https://www.redpanda.com/connect) — **М**. Очереди, БД, API, файлы и storage.
- [Bento](https://warpstreamlabs.github.io/bento/) — **O**. Отдельный fork Benthos для streaming pipelines.
- [Apache Pulsar IO / Functions](https://pulsar.apache.org/) — **O**. Источники, приёмники и обработка сообщений.
- [Apache Camel](https://camel.apache.org/) — **O**. Маршрутизация и интеграционные паттерны.
- [Spring Cloud Stream](https://spring.io/projects/spring-cloud-stream/) — **O**. Приложения обработки сообщений с broker binders.
- [Apache Pekko Connectors](https://pekko.apache.org/docs/pekko-connectors/current/) — **O**. Коннекторные потоковые приложения.
- [Akka / Alpakka](https://doc.akka.io/libraries/alpakka/current/) — **М**. Потоковые коннекторы; лицензия зависит от компонента и версии.
- [Kafka MirrorMaker](https://kafka.apache.org/documentation/#georeplication) — **O**. Репликация между Kafka-кластерами.
- [RabbitMQ Shovel](https://www.rabbitmq.com/docs/shovel) — **O**. Перенос сообщений между брокерами.
- [NATS JetStream mirrors/sources](https://docs.nats.io/learn/jetstream/mirrors-and-sources) — **O**. Репликация и агрегация потоков NATS.

<a id="managed-streaming"></a>
## 10. Управляемые платформы потоковой интеграции

- [Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/overview.html) — **К**. Управляемая потоковая обработка.
- [Ververica](https://www.ververica.com/) — **К**. Enterprise-платформа Flink.
- [Decodable](https://www.decodable.co/) — **К**. Управляемые stream-processing pipelines.
- [Aiven for Apache Kafka Connect](https://aiven.io/kafka-connect) — **К**. Управляемые коннекторы.
- [StreamNative](https://streamnative.io/) — **К**. Потоковая платформа с интеграциями и lakehouse-направлением.
- [Amazon Managed Service for Apache Flink](https://aws.amazon.com/managed-service-apache-flink/) — **К**. Управляемые Flink-приложения.
- [Amazon MSK Connect](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect.html) — **К**. Управляемый Kafka Connect.
- [Google Managed Kafka Connect](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/connect-cluster/create-connect-cluster) — **К**. Connect-кластеры в Google Cloud.
- [Azure Stream Analytics](https://azure.microsoft.com/en-us/products/stream-analytics) — **К**. Потоковые SQL-процессы.
- [Fabric Eventstreams](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/overview) — **К**. Приём, преобразование и маршрутизация событий.
- [Alibaba Realtime Compute for Apache Flink](https://www.alibabacloud.com/en/product/realtime-compute) — **К**. Управляемый Flink.
- [IBM Event Automation](https://www.ibm.com/products/event-automation) — **К**. Enterprise-платформа событийной интеграции.

<a id="processing-engines"></a>
## 11. Движки batch/stream processing и непрерывных вычислений

- **[Apache Spark](https://spark.apache.org/) — O. Масштабный batch ETL и Structured Streaming.**
- [Apache Flink](https://flink.apache.org/) — **O**. Stateful streaming и batch.
- [Apache Beam](https://beam.apache.org/) — **O**. Единая модель batch/streaming pipelines.
- [Kafka Streams](https://kafka.apache.org/documentation/streams/) — **O**. Библиотека обработки Kafka-потоков.
- [ksqlDB](https://www.confluent.io/product/ksqldb/) — **S/К**. Streaming SQL в экосистеме Kafka.
- [RisingWave](https://risingwave.com/) — **O/К**. Streaming SQL, CDC и материализованные представления.
- [Materialize](https://materialize.com/) — **S/К**. Инкрементальные SQL-вычисления.
- [Feldera](https://www.feldera.com/) — **O/К**. SQL pipelines с инкрементальным исполнением.
- [DeltaStream](https://www.deltastream.io/) — **К**. Управляемая обработка потоков.
- [Timeplus](https://www.timeplus.com/) — **М**. Streaming SQL и real-time pipelines.
- [Epsio](https://www.epsio.io/) — **К**. Непрерывные преобразования данных.
- [Quix Streams](https://github.com/quixio/quix-streams) — **O**. Python Streaming DataFrames для Kafka.
- [Pathway](https://github.com/pathwaycom/pathway) — **М**. Python ETL и инкрементальная потоковая обработка.
- [Hazelcast](https://hazelcast.com/) — **М**. Распределённая обработка данных в реальном времени.
- [Apache Storm](https://storm.apache.org/) — **O**. Распределённая обработка событий.

<a id="iot-edge"></a>
## 12. IoT, MQTT и edge-интеграция

- [EMQX](https://www.emqx.com/en) — **М**. MQTT, rules и интеграции с внешними системами.
- [HiveMQ](https://www.hivemq.com/) — **М**. MQTT и интеграция промышленных событий.
- [eKuiper](https://github.com/lf-edge/ekuiper) — **O**. Лёгкий stream-processing engine для edge.
- [Node-RED](https://nodered.org/) — **O**. Визуальные событийные потоки и коннекторы.
- [Apache StreamPipes](https://streampipes.apache.org/) — **O**. Промышленная интеграция и обработка IoT-данных.
- [Solace](https://solace.com/) — **К**. Event mesh и интеграция потоков событий.

<a id="telemetry"></a>
## 13. Логи, телеметрия и observability pipelines

Пересечение с Transferia: чтение потока, парсинг, преобразование, буферизация и доставка.

- [Vector](https://vector.dev/) — **O**. Rust-based pipelines и широкий набор источников/приёмников.
- [Fluent Bit](https://fluentbit.io/) — **O**. Лёгкий сборщик и процессор телеметрии.
- [Fluentd](https://www.fluentd.org/) — **O**. Коннекторная доставка и обработка событий.
- [Logstash](https://www.elastic.co/logstash) — **М**. Input/filter/output pipelines.
- [Cribl Stream](https://cribl.io/products/stream/) — **К**. Обработка и маршрутизация телеметрии.
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) — **O**. Receivers/processors/exporters для телеметрии.
- [Grafana Alloy](https://grafana.com/docs/alloy/latest/) — **O**. Конфигурируемые observability pipelines.
- [Bindplane](https://bindplane.com/) — **М**. Управление OpenTelemetry pipelines.
- [Mezmo Telemetry Pipeline](https://www.mezmo.com/platform/telemetry-pipeline) — **К**. Обработка, преобразование и маршрутизация телеметрии.
- [Edge Delta](https://edgedelta.com/) — **К**. Обработка телеметрии и событий.
- [Chronosphere](https://chronosphere.io/) — **К**. Observability и управление потоками телеметрии.
- [Tenzir](https://tenzir.com/) — **М**. Pipelines для security-данных.
- [Axoflow](https://axoflow.com/) — **К**. Сбор и нормализация security-данных.
- [OpenSearch Data Prepper](https://docs.opensearch.org/latest/data-prepper/) — **O**. Ingestion и преобразования для аналитики/поиска.
- [syslog-ng](https://github.com/syslog-ng/syslog-ng) — **O/К**. Сбор, фильтрация и доставка логов.
- [rsyslog](https://www.rsyslog.com/) — **O**. Производительная маршрутизация логов.

<a id="customer-events"></a>
## 14. Customer events и CDP

Специализированные конкуренты для сбора, обогащения и доставки событий приложений.

- [RudderStack](https://www.rudderstack.com/) — **М**. Event pipelines и warehouse-интеграции.
- [Snowplow](https://snowplow.io/) — **М**. Сбор, валидация и обогащение поведенческих событий.
- [Twilio Segment](https://www.twilio.com/en-us/segment) — **К**. Доставка customer events в приложения и хранилища.
- [Tealium](https://tealium.com/) — **К**. Сбор и маршрутизация клиентских данных.
- [Treasure AI, ранее Treasure Data](https://www.treasure.ai/) — **К**. Customer-data ingestion и активация.
- [Adobe Real-Time CDP](https://business.adobe.com/products/real-time-customer-data-platform/rtcdp.html) — **К**. Сбор, объединение и активация customer data.

<a id="reverse-etl"></a>
## 15. Reverse ETL и активация данных

- [Hightouch](https://hightouch.com/) — **К**. Warehouse → CRM, маркетинговые и операционные приложения.
- [Multiwoven](https://www.multiwoven.com/) — **O/К**. Reverse ETL и data activation.
- [GrowthLoop](https://www.growthloop.com/) — **К**. Активация аудиторий поверх warehouse.
- [Omnata](https://omnata.com/) — **К**. Интеграция enterprise-платформ, включая синхронизацию из хранилищ.

Также сюда относятся уже перечисленные **Polytomic, Weld, Integrate.io,
DataChannel, RudderStack и Fivetran**. Census учтён в
[списке приобретённых брендов](#renamed-products), а не как дополнительный
независимый конкурент.

<a id="marketing-elt"></a>
## 16. Маркетинговый ELT и отраслевые коннекторы

- [Adverity](https://www.adverity.com/) — **К**. Интеграция маркетинговых данных.
- [Funnel](https://funnel.io/) — **К**. Сбор и унификация рекламных данных.
- [Improvado](https://improvado.io/) — **К**. Marketing ETL и аналитические pipelines.
- [Supermetrics](https://supermetrics.com/) — **К**. Рекламные/SaaS-коннекторы в BI и хранилища.
- [Windsor.ai](https://windsor.ai/) — **К**. No-code загрузка данных в BI и БД.
- [Renta](https://renta.im/) — **К**. Customer-data infrastructure и маркетинговые интеграции.
- [Coupler.io](https://www.coupler.io/) — **К**. Перенос из приложений в таблицы, BI и хранилища.
- [Dataslayer](https://www.dataslayer.ai/) — **К**. Автоматизация маркетинговых выгрузок.
- [OWOX](https://www.owox.com/) — **М**. Коннекторы, data marts и аналитические потоки.

<a id="ipaas"></a>
## 17. iPaaS, API и интеграция бизнес-приложений

Конкуренция преимущественно в синхронизации приложений, обработке webhooks и
интеграционных workflows.

- [Boomi Enterprise Platform](https://boomi.com/) — **К**. Enterprise iPaaS; продукт Data Integration отдельно указан выше.
- [MuleSoft Anypoint Platform](https://www.mulesoft.com/) — **К**. API и enterprise-интеграции.
- [SnapLogic](https://www.snaplogic.com/) — **К**. Визуальные application/data pipelines.
- [Workato](https://www.workato.com/) — **К**. Интеграция SaaS и событийная автоматизация.
- [Jitterbit](https://www.jitterbit.com/) — **К**. Приложения, API и перенос данных.
- [Celigo](https://www.celigo.com/) — **К**. ERP, CRM, e-commerce и SaaS.
- [Tray.ai](https://tray.ai/) — **К**. Коннекторы и автоматизация workflows.
- [n8n](https://n8n.io/) — **S/К**. Self-hosted workflows, API и webhooks.
- [WSO2 Integrator](https://wso2.com/integration-platform/integrator/) — **O/К**. API, messaging, файлы и batch.
- [IBM webMethods Integration](https://www.ibm.com/products/webmethods-integration) — **К**. Enterprise application integration.
- [SAP Integration Suite](https://www.sap.com/products/technology-platform/integration-suite.html) — **К**. SAP/non-SAP и событийные процессы.
- [Oracle Integration](https://www.oracle.com/integration/) — **К**. Интеграция enterprise-приложений.
- [TIBCO Platform Integration / BusinessWorks](https://www.tibco.com/platform/integration) — **К**. Корпоративные интеграционные процессы.
- [Frends](https://frends.com/) — **К**. iPaaS и гибридная интеграция.
- [Digibee](https://www.digibee.com/) — **К**. Enterprise integration pipelines.
- [Flowgear](https://www.flowgear.net/) — **К**. Low-code интеграции.
- [Linx](https://linx.software/) — **К**. Low-code backend и интеграционные процессы.
- [elastic.io](https://www.elastic.io/) — **К**. Коннекторная iPaaS.
- [Make](https://www.make.com/en) — **К**. Визуальные сценарии синхронизации приложений.
- [Zapier](https://zapier.com/) — **К**. Событийная автоматизация SaaS.
- [Activepieces](https://www.activepieces.com/) — **O/К**. Открытая платформа автоматизации.
- [Pipedream](https://pipedream.com/) — **М**. API-интеграции и исполняемые workflows.
- [Zoho Flow](https://www.zoho.com/flow/) — **К**. Интеграция бизнес-приложений.
- [Prismatic](https://prismatic.io/) — **К**. Embedded iPaaS для SaaS-продуктов.
- [Paragon](https://www.useparagon.com/) — **К**. Встраиваемые интеграции.
- [Cyclr](https://cyclr.com/) — **К**. Embedded iPaaS и коннекторы.
- [Albato](https://albato.com/) — **К**. Автоматизация и встраиваемые интеграции.

<a id="regional-platforms"></a>
## 18. Российские платформы интеграции и ETL

Yandex/Transferia уже включены в [основную группу](#universal-ingestion).

- [Arenadata Streaming](https://arenadata.tech/ru/products/ads) — **К, на OSS-компонентах**. Kafka, NiFi и корпоративная потоковая интеграция.
- [Loginom](https://loginom.ru/) — **К**. Визуальная подготовка данных и ETL.
- [DATAREON Platform](https://datareon.ru/solution/datareon-platform/) — **К**. Интеграционная платформа, обмен данными и ETL.
- [Digital Q.DataFlows](https://q.diasoft.ru/mediacenter/news/v-platforme-digital-q-dataflows-dobavlena-vozmozhnost-ispolzovaniya-naborov-dannykh-v-etl-protsessakh/) — **К**. Проектирование и выполнение ETL-процессов.
- [Neoflex Datagram](https://www.neoflex.ru/publications/neoflex-datagram-etl-platform) — **К**. Платформа ETL и параллельной обработки данных.

<a id="bulk-migration"></a>
## 19. Массовое копирование, файловые переносы и миграционные утилиты

Конкуренты для snapshot/bulk-copy сценариев; большинство не заменяет
непрерывный универсальный CDC.

- [rclone](https://rclone.org/) — **O**. Копирование и синхронизация object storage и файловых сервисов.
- [rsync](https://rsync.samba.org/) — **O**. Инкрементальная файловая синхронизация.
- [pgloader](https://pgloader.io/) — **O**. Загрузка и миграция в PostgreSQL.
- [pgcopydb](https://pgcopydb.readthedocs.io/en/latest/) — **O**. Копирование PostgreSQL с возможностью сопровождения изменений.
- [Ora2Pg](https://ora2pg.darold.net/) — **O**. Миграция Oracle/MySQL → PostgreSQL.
- [mydumper/myloader](https://github.com/mydumper/mydumper) — **O**. Параллельная выгрузка и загрузка MySQL.
- [MySQL Shell](https://github.com/mysql/mysql-shell) — **O**. Dump/load/copy utilities.
- [SQLines](https://sqlines.com/) — **М**. Миграция схем, SQL и данных.
- [AWS DataSync](https://aws.amazon.com/datasync/) — **К**. Массовый перенос файлов и объектов.
- [Google Storage Transfer Service](https://cloud.google.com/storage-transfer-service) — **К**. Перенос object/file storage.
- [Azure Storage Mover](https://azure.microsoft.com/en-us/products/storage-mover) — **К**. Миграция storage в Azure.
- [IBM Aspera](https://www.ibm.com/products/aspera) — **К**. Передача больших файлов и наборов данных.
- [Progress MOVEit](https://www.progress.com/moveit) — **К**. Managed file transfer.

<a id="orchestration"></a>
## 20. Оркестрация собственных pipelines

Косвенные альтернативы: обеспечивают расписания, зависимости, retries и
управление исполнением; перенос выполняют подключённые компоненты или
пользовательский код.

- [Apache Airflow](https://airflow.apache.org/) — **O**.
- [Dagster](https://dagster.io/) — **O/К**.
- [Prefect](https://www.prefect.io/) — **O/К**.
- [Kestra](https://kestra.io/) — **O/К**.
- [Apache DolphinScheduler](https://dolphinscheduler.apache.org/) — **O**.
- [Argo Workflows](https://argo-workflows.readthedocs.io/en/latest/) — **O**.
- [Flyte](https://flyte.org/) — **O/К**.
- [Luigi](https://github.com/spotify/luigi) — **O**.
- [Digdag](https://www.digdag.io/) — **O**.
- [Astronomer](https://www.astronomer.io/) — **К**, управляемая платформа Airflow.
- [Bruin](https://getbruin.com/) — **O/К**, ingestion и оркестрация data workflows.
- [Apache StreamPark](https://github.com/apache/streampark) — **O**, управление потоковыми приложениями.
- [Dinky](https://github.com/DataLinkDC/dinky) — **O**, разработка и эксплуатация Flink pipelines.
- [Spring Cloud Data Flow](https://spring.io/blog/2025/04/21/spring-cloud-data-flow-commercial/) — **К для дальнейшего развития**: развитие открытых веток прекращено, новые версии предназначены для Tanzu Spring customers.

<a id="processing-libraries"></a>
## 21. Вычислительные библиотеки и SQL-преобразования

Инструменты для самостоятельной реализации части Transferia. Их не следует
сравнивать с готовыми системами переноса по числу коннекторов или
эксплуатационным возможностям.

- [Daft](https://www.daft.ai/) — **O/К**. Распределённая обработка данных.
- [Ray Data](https://docs.ray.io/en/latest/data/data.html) — **O**. Масштабируемые data-processing pipelines.
- [Dask](https://www.dask.org/) — **O**. Параллельная обработка на Python.
- [Polars](https://pola.rs/) — **O/К**. DataFrame-преобразования и streaming execution.
- [DuckDB](https://duckdb.org/) — **O**. SQL над файлами и локальные ETL-процессы.
- [dbt](https://www.getdbt.com/) — **O/К**. Преобразования уже загруженных данных.
- [SQLMesh](https://sqlmesh.readthedocs.io/en/stable/) — **O**. Управление SQL-моделями и инкрементальными преобразованиями.
- [Coalesce](https://coalesce.io/) — **К**. Автоматизация преобразований в аналитических хранилищах.

<a id="federation"></a>
## 22. Федерация и комплексные data platforms

Альтернативный способ решить задачу: эти решения конкурируют за часть задач и
бюджета, но могут вообще не переносить данные физически.

- [Denodo](https://www.denodo.com/en) — **К**. Виртуализация и федерация данных.
- [Dremio](https://www.dremio.com/) — **М**. Lakehouse и доступ к распределённым данным.
- [Trino](https://trino.io/) — **O**. Федеративные SQL-запросы и операции через коннекторы.
- [Starburst](https://www.starburst.io/) — **К**. Enterprise-платформа поверх федеративного SQL.
- [SAP Datasphere](https://www.sap.com/products/data-cloud/datasphere.html) — **К**. Интеграция, репликация и семантический слой.
- [Palantir Foundry](https://www.palantir.com/platforms/foundry/) — **К**. Комплексная платформа с ingestion и преобразованиями.
- [Domo](https://www.domo.com/) — **К**. Коннекторы, интеграция и аналитические workflows.

<a id="renamed-products"></a>
## 23. Старые названия и приобретённые продукты

Не считать повторно как независимых конкурентов.

| Название | Как учтено |
|---|---|
| **Benthos** | [Redpanda Connect](https://www.redpanda.com/connect); **Bento** — самостоятельный fork |
| **Rivery** | [Boomi Data Integration](https://boomi.com/rivery-is-now-boomi-data-integration/) |
| **BryteFlow** | [SingleStore Flow](https://www.singlestore.com/blog/the-journey-from-bryteflow-to-singlestore-flow/) |
| **Upsolver** | Направление ingestion/Iceberg в [Qlik](https://www.qlik.com/us/news/company/press-room/press-releases/qlik-acquires-upsolver-to-deliver-low-latency-ingestion-and-optimization-for-apache-iceberg) |
| **Census** | Приобретённое направление Fivetran; [старый сайт](https://www.getcensus.com/) перенаправляет на Fivetran |
| **Stitch** | [Существующим клиентам оставлен вход](https://www.stitchdata.com/), новым предлагается Qlik Talend Cloud |
| **FlinkX** | [ChunJun](https://github.com/DTStack/chunjun) |
| **Treasure Data** | Текущее позиционирование — [Treasure AI](https://www.treasure.ai/) |
| **Data Virtuality / CData Virtuality** | [Прежняя продуктовая страница](https://www.cdata.com/virtuality/) перенаправляет на CData Connect AI; условия отдельного предложения нужно уточнять |

<a id="legacy-and-unconfirmed"></a>
## 24. Архивные решения и проекты с неподтверждённой жизнеспособностью

Их полезно учитывать при изучении архитектур и миграций с существующих
инсталляций, но они не включаются в подборку наиболее активных альтернатив
без оговорок. Неподтверждённый статус не означает доказанное прекращение проекта.

- [Talend Open Studio](https://community.qlik.com/t5/Installing-and-Upgrading/Talend-Open-studio-for-big-data-7-3-1/td-p/2458681) — прекращён с 31 января 2024 года.
- [Grouparoo](https://github.com/grouparoo/grouparoo) — репозиторий архивирован.
- [Apache Sqoop](https://attic.apache.org/projects/sqoop.html) — Apache Attic.
- [Apache Apex](https://attic.apache.org/projects/apex.html) — Apache Attic.
- [Apache Samza](https://samza.apache.org/) — известный stream-processing framework; на главной последним указан релиз 2023 года, текущую поддержку нужно проверять отдельно.
- [Apache Flume](https://flume.apache.org/) — известная система ingestion; перед новым внедрением нужен отдельный аудит релизов и поддержки.
- [Apache Heron](https://github.com/apache/incubator-heron) — исторически значимый streaming engine; актуальность для нового внедрения не подтверждена этим исследованием.
- [Bytewax](https://github.com/bytewax/bytewax) — Python/Rust stream processing; репозиторий доступен, но текущая активная поддержка не установлена этим исследованием.
- [pg_flo](https://github.com/pgflo/pg_flo) — специализированный PostgreSQL CDC; текущий статус первичного репозитория не удалось надёжно подтвердить.
- [Equalum](https://www.equalum.io/) — известная CDC-платформа; актуальное самостоятельное предложение не удалось подтвердить.
- [Arcion](https://www.arcion.io/) — известное направление CDC; актуальный самостоятельный продукт и условия доступности требуют уточнения.

## Как использовать каталог

Способы разбиения исходных таблиц, аудит локальных исходников и коммерческой
документации, а также предложения для Transferia разобраны в отдельном
[исследовании](table-splitting-research-2026-09-20.md).

Для сравнения с Transferia Rust прежде всего рассматривать готовые системы
переноса, CDC и коннекторные движки. Специализированные приёмники, нативная
репликация БД, оркестраторы, вычислительные библиотеки и федерация решают
отдельные части задачи и не являются автоматически полной заменой.

Каталог не доказывает равенство гарантий доставки. Snapshot consistency,
порядок изменений, exactly-once, сохранение типов и схем, replay, backpressure,
обработка ошибок и поведение при schema drift должны сравниваться отдельно
для конкретной пары источника и приёмника, редакции и версии продукта.
