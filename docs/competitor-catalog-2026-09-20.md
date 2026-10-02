# Каталог конкурентов и альтернатив Transferia Rust

Дата исследования: **20 сентября 2026 года**.

Дополнение от **30 сентября 2026 года**: добавлены 22 пропущенные позиции,
3 ранних/смежных проекта и 2 исторических аналога. Они распределены по
существующим категориям; связанные OSS/managed-поставки и переезды
репозиториев не считаются независимыми компаниями. Новые описания основаны
на документации и README, а не на собственных бенчмарках.

Дополнение от **2 октября 2026 года**: добавлены **42 позиции** в сегменте
customer data: CleverData Join, российские и международные CDP, движки с
доступным исходным кодом, маршрутизация событий и смежные commerce-интеграции.
Причина прежнего пропуска — неполное покрытие сегмента: в разделе CDP уже
были международные платформы, но российские аналоги и соседние продуктовые
семейства не прошли систематическую проверку. Это пробел исследования,
а не обоснованное исключение CleverData из конкурентного поля.

Дополнение по Sesam от **2 октября 2026 года**: добавлены **39 продуктовых
позиций** — 15 data hub/MDM, 9 платформ операционной синхронизации,
12 дополнительных iPaaS/ESB/B2B-продуктов и 3 российских решения.
Это число продуктов/семейств, а не новых независимых компаний.

**Разбор пропуска Sesam:** каталог уже допускал сценарных конкурентов, но
раздел iPaaS описывал преимущественно workflows и известные коннекторные
платформы. Систематическая проверка semantic data hubs, master-data synchronization
и stateful multi-directional sync не отражена в прежнем результате.
Дополнение CDP закрыло клиентские данные, но не многодоменные операционные
сущности. Это наблюдаемый пробел классификации и проверки покрытия;
точная история прежних поисковых запросов не восстановлена. Sesam не был
исключён на основании проверенного технического ограничения.

Для исправления проверены три соседние группы в
[новых подразделах интеграции](#semantic-data-hubs), а также российские
аналоги. Сходство отмечено по конкретному сценарию, источники — официальные
страницы и документация. Это расширение подтверждённого покрытия, не
доказательство исчерпания всех мировых MDM/iPaaS-вендоров.

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

## Участники следующего серверного бенчмарка

Состав расширен **21 сентября 2026 года**. Это план проверки, а не результаты
прогонов или утверждение о поддержке всех маршрутов каждым участником.

1. Transferia Rust.
2. transfer_manager / transferctl Go.
3. Apache SeaTunnel.
4. Sling.
5. Alibaba DataX.
6. Airbyte.
7. Apache Flink CDC.
8. [Debezium](https://debezium.io/) + Kafka Connect + JDBC Sink.
9. [Estuary Flow](https://estuary.dev/).
10. [Apache InLong](https://inlong.apache.org/).
11. [Meltano](https://meltano.com/) с зафиксированными версиями tap и target.
12. [Apache Spark](https://spark.apache.org/).
13. [Apache Sqoop](https://attic.apache.org/projects/sqoop.html) — архивный участник.

**Debezium обязательно проверить на snapshot одной таблицы частями.**
Актуальная [документация PostgreSQL-коннектора](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
описывает экспериментальный параллельный initial snapshot по диапазонам PK:
`snapshot.max.threads` задаёт число потоков, `snapshot.max.threads.multiplier` —
множитель числа частей. Перед прогоном подтвердить эту возможность на конкретной
зафиксированной версии. Проверить режимы 1/4/16 читателей, фактические диапазоны,
одновременность чтения и полную доставку в приёмник. Не подменять этот сценарий
последовательным incremental snapshot или параллельным чтением разных таблиц.
Select overrides могут отключать встроенное разбиение; одинаковые настройки
конкурентности сами по себе не доказывают одинаковые границы частей.

Для всех участников фиксировать одинаковые данные, границы частей и бюджет
ресурсов там, где это реализуемо. Внешнее разбиение несколькими заданиями и
собственные автоматические планировщики показывать отдельными режимами.
Для Estuary сначала установить доступный способ развёртывания и возможность
сопоставимого ограничения ресурсов; для Meltano — возможности выбранных
коннекторов; для Sqoop — полный маршрут до конечного приёмника, включая
промежуточное хранение, если оно требуется. Ограничения и неуспешные проверки
сохранять в отчёте, не исключая участника без объяснения.

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
- [Gathr](https://www.gathr.ai/data-pipelining) — **К**. Ingestion, batch/streaming ETL, преобразования и оркестрация; корпоративная визуальная платформа.
- [Prophecy](https://www.prophecy.ai/) — **К**. Визуальные data workflows и генерируемый код для Databricks, Snowflake и BigQuery; косвенный конкурент в подготовке данных и создании ETL-pipelines, а не отдельный CDC-runtime.

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
- [Conduit](https://github.com/ConduitIO/conduit) — **O/К**. Go-движок source → processors → sinks, CDC-коннекторы, observability и подтверждения после обработки приёмниками. Текущие [документация и changelog](https://conduitdata.io/); связанная управляемая [Meroxa Conduit Platform](https://docs.meroxa.com/) учитывается в том же семействе.

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
- [Dozer](https://github.com/getdozer/dozer) — **O**. Rust CDC и преобразования при переносе в ClickHouse, PostgreSQL, MySQL и другие приёмники. Возможности resume различаются по коннекторам; часть коннекторов в README отнесена к Enterprise. Актуальность коммерческого предложения отдельно не подтверждена.
- [Supabase ETL](https://github.com/supabase/etl) — **O/К**. Rust-библиотека или отдельный процесс: первоначальная копия PostgreSQL и дальнейшая логическая репликация, сохранение состояния и расширяемые приёмники. Проект до первого стабильного релиза. Связанный hosted-продукт — [Supabase Pipelines](https://supabase.com/docs/guides/database/replication/pipelines); возможности поставок не считать идентичными.
- [TapData](https://docs.tapdata.io/data-replication/create-task/) — **O/К**. Полная и инкрементальная репликация разнородных БД, log-based CDC и отдельные двунаправленные маршруты; Community и Enterprise различаются.
- [BladePipe](https://www.bladepipe.com/docs/intro/product_intro/) — **К**. Миграция, real-time CDC, изменения схем, фильтрация, проверка и коррекция данных; семантику автоматических преобразований сравнивать отдельно.
- [CloudCanal, Clougence](https://www.clougence.com/) — **К**. Полная миграция, инкрементальная синхронизация, преобразования и перенос схем между БД и очередями. Самостоятельный продукт, не другое название Alibaba Canal.
- [NineData](https://docs.ninedata.cloud/en/replication/data_replication/) — **К**. Schema/full/incremental replication, сравнение данных, одно- и двунаправленные сценарии; поддержка зависит от пары источника и приёмника.
- [cdcflow](https://github.com/manfredcml/cdcflow) — **O**, **ранний проект**. Rust CDC из PostgreSQL/MySQL/MongoDB в Kafka/PostgreSQL/Iceberg; раздельные режимы changelog и materialized replication. README отмечает неполную реализацию admin API; production-зрелость и гарантии не проверены.

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
- [Onehouse / OneFlow](https://docs.onehouse.ai/category/ingest-data/) — **К**. Managed ingestion из БД, потоков и файлов, преобразования и CDC-сценарии. [Flows создают Hudi-таблицы](https://docs.onehouse.ai/product/ingest-data/flows/create-flow/); совместимость чтения с Iceberg/Delta настраивается через OneTable.
- [LakeSoul](https://github.com/lakesoul-io/LakeSoul) — **O**. Lakehouse-платформа с ingestion, concurrent updates и incremental processing, включая whole-database sync через Flink CDC. Содержит собственный storage/table-management слой; Rust NativeIO не означает полностью Rust-реализацию всего CDC-пути.

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
- [Moonlink, Mooncake Labs](https://github.com/Mooncake-Labs/moonlink) — **S**. Rust ingestion для PostgreSQL CDC → Iceberg, вставок/upserts и подготовки файлов. Preview; Kafka/OTEL и часть catalog integrations в README находятся в roadmap. [Лицензия BSL 1.1](https://github.com/Mooncake-Labs/moonlink/blob/main/LICENSE), не open source на дату проверки; независимое коммерческое предложение не подтверждено.
- [BemiDB](https://github.com/pgstack-io/BemiDB) — **O**. Коннекторы синхронизируют БД/SaaS в columnar-данные на S3, аналитический query engine предоставляет Postgres-совместимый интерфейс. Специализированный ingestion + analytics, а не произвольный source/sink-перенос; текущий репозиторий в `pgstack-io`.

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
- [Apache Iggy Connectors](https://iggy.apache.org/docs/connectors/introduction/) — **O**. Rust-runtime source/sink-плагинов и transforms для внешних систем и Iggy streams. Конкурентное пересечение обеспечивает connector runtime; наличие Postgres-коннектора само по себе не доказывает нативный CDC.
- [Apache RocketMQ Connect](https://github.com/apache/rocketmq-connect) — **O**. Коннекторная платформа вокруг RocketMQ для pipelines, ETL и CDC; [документация 4.x](https://rocketmq.apache.org/docs/4.x/connect/01RocketMQ%20Connect%20Overview/). Требует инфраструктуру RocketMQ и проверки конкретных коннекторов.

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
- [Cloudflare Pipelines](https://developers.cloudflare.com/pipelines/) — **К**, open beta. HTTP/Workers ingestion, SQL-преобразования и запись Iceberg/Parquet/JSON в R2. Связанный с [Arroyo](#processing-engines) managed-продукт; не считать независимым движком и не переносить сюда весь набор коннекторов Arroyo.

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
- [Arroyo](https://github.com/ArroyoSystems/arroyo) — **O/К**. Rust SQL stream processing: stateful windows/joins, checkpointing, Kafka и Iceberg, real-time ingestion. [PostgreSQL source](https://doc.arroyo.dev/connectors/postgres/) работает через Debezium/Kafka; нативный PG source обозначен как план. Связанный managed-продукт — [Cloudflare Pipelines](#managed-streaming).
- [Fluvio / Stateful DataFlow](https://github.com/fluvio-community/fluvio) — **O**. Rust streaming, коннекторы и программируемая обработка потоков. Репозиторий переехал из InfinyOn в `fluvio-community`; README описывает переход инфраструктуры сборок. Доступность прежних облачных предложений InfinyOn отдельно не подтверждена.
- [Numaflow](https://github.com/numaproj/numaflow) — **O**. Kubernetes-платформа непрерывных pipelines с sources, processing, sinks, autoscaling и backpressure. Сценарная альтернатива для streaming; требует Kubernetes и не означает универсальный нативный CDC.
- [clink](https://github.com/orhaugh/clink) — **O**, **ранний проект**. C++/Arrow stream processing с SQL, checkpointing и source/sink-коннекторами. README характеризует его как молодой pre-1.0 проект одного maintainer; заявления о гарантиях и производительности требуют независимой проверки.

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
Проверка дополнения: **2 октября 2026 года**, по официальным продуктовым страницам,
документации и репозиториям. Это продукты, а не число независимых компаний.

**Граница сравнения:** CDP пересекаются с Transferia в ingestion, маршрутизации,
выгрузках в аналитику и активации данных. Identity resolution, объединение
профилей и маркетинговая нормализация имеют собственную семантику; они не
доказывают lossless replication, сохранение исходных PK/типов или поддержку
универсального CDC. В описаниях отмечены более узкие и недостаточно
документированные случаи.

### Ранее учтённые event pipelines и CDP

- [RudderStack](https://www.rudderstack.com/) — **М**. Event pipelines и warehouse-интеграции.
- [Snowplow](https://snowplow.io/) — **М**. Сбор, валидация и обогащение поведенческих событий.
- [Twilio Segment](https://www.twilio.com/en-us/segment) — **К**. Доставка customer events в приложения и хранилища.
- [Tealium](https://tealium.com/) — **К**. Сбор и маршрутизация клиентских данных.
- [Treasure AI, ранее Treasure Data](https://www.treasure.ai/) — **К**. Customer-data ingestion и активация.
- [Adobe Real-Time CDP](https://business.adobe.com/products/real-time-customer-data-platform/rtcdp.html) — **К**. Сбор, объединение и активация customer data.
- [Jitsu](https://github.com/jitsucom/jitsu) — **O/К**. Customer-event ingestion, batch/streaming delivery в DWH, JavaScript-преобразования и SaaS connector syncs; сценарный конкурент рядом с Segment и RudderStack, не универсальный DB CDC.

### Российские CDP и платформы клиентских данных

- [CleverData Join (CleverDATA, LANSOFT)](https://cleverdata.ru/solutions/istochniki_i_priemniki_dannih) — **К**. Приём событий через REST API, объединение онлайн/офлайн-профилей, экспорт сегментов CSV/FTP, доставка в Kafka и выгрузка в ClickHouse. Сценарный конкурент по customer-data ingestion и delivery; универсальный CDC БД этим не подтверждён.
- [Mindbox](https://mindbox.ru/products/cdp/) — **К**. Сбор клиентских событий из сайтов, приложений и офлайн-систем, единые профили и активация; [API-экспорты](https://developers.mindbox.ru/docs/exports-overview) и [Delta Sharing для аналитики](https://developers.mindbox.ru/docs/external-customer-id-in-analytics-exports).
- [Altcraft Platform](https://use.altcraft.com/user-guide/) — **К**. CDP и автоматизация коммуникаций; импорт через API, файлы и SQL, экспорт профилей и аудиторий, включая синхронизацию с SQL-БД. Возможности описаны в [функциональных характеристиках](https://altcraft.com/ru/Altcraft_%D0%A4%D1%83%D0%BD%D0%BA%D1%86%D0%B8%D0%BE%D0%BD%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D0%B5_%D1%85%D0%B0%D1%80%D0%B0%D0%BA%D1%82%D0%B5%D1%80%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8.pdf).
- [enKod](https://enkod.io/cdp/) — **К**. Сбор контактов, заказов и поведенческих событий, объединение клиентских данных и запуск персональных коммуникаций; пересечение в прикладном event ingestion и интеграциях.
- [Retail Rocket Group — Live CDP / Sailplay](https://retailrocket.ru/cdp) — **К**. Клиентские данные из online/offline-каналов, профили и активация. [Sailplay CDP](https://docs.retailrocket.ru/docs/sailplay/lk_guides/lk_guides_clients/) учтён в одной продуктовой семье, без повторного подсчёта поставщика.
- [REES46 CDP](https://rees46.ru/products/cdp/) — **К**. Сбор данных сайта, приложения, CRM и офлайн-магазинов, объединение профилей, сегментация и персонализация; [API обогащения профиля](https://rees46.ru/help/integration/cdp/profile/set.html).
- [Konnektu](https://konnektu.ai/) — **К**. CDP и Data Services для объединения клиентских данных, профилей и активации. Доступно [руководство 2023 года](https://konnektu.ai/wp-content/uploads/2023/05/Руководство-пользователя.pdf); актуальная матрица коннекторов и условия поставки требуют отдельного подтверждения.
- [Loymax Smart Communications](https://loymax.io/) — **К**. CDP и Campaign Manager в платформе лояльности: клиентские профили, сегментация и триггерные коммуникации; отраслевое пересечение по интеграции данных ритейла.
- [Manzana CDP&BI](https://docs.manzanagroup.ru/xwiki/wiki/rrs/download/bi/WebHome/4.pdf?rev=1.1) — **К**. Клиентские данные, чеки, бонусы и купоны как источник для аналитики Manzana BI. Смежная отраслевая платформа; документация 2024 года подтверждает продукт, но не универсальность его внешних коннекторов.
- [Sendsay CDP](https://sendsay.ru/solutions/cdp) — **К**. Сведение данных CRM, сайта, приложения и других систем, сегментация и активация; [API интеграций](https://docs.sendsay.ru/sendsay-api/sendsay-api-guide/).
- [MAXMA CDP](https://maxma.com/cdp) — **К**. Сбор данных касс, сайтов, приложений и анкет в клиентский профиль; готовые интеграции и API. Отраслевой конкурент по online/offline-ingestion для ритейла.
- [CXDP](https://cxdp.ru/api/) — **К**. Клиентские профили и маркетинговая активация; документированы поштучный и массовый API-импорт профилей. Конкуренция ограничена customer-data сценариями.
- [RightWay CDP](https://rightway-tech.ru/cifrovye-resheniya/cdp-platforma/) — **К**. Объединение данных PMS, POS, CRM, сайта и приложения; клиентская база и коммуникации для гостиниц и ритейла. Есть SaaS и on-premise-поставки.

### Международные CDP и активация клиентских данных

- [Rokt mParticle](https://docs.rokt.com/products/mparticle/) — **К**. Сбор и объединение событий и профилей, маршрутизация и активация клиентских данных; учитывать mParticle как продукт Rokt, а не дополнительного независимого поставщика.
- [ActionIQ by Uniphore](https://www.uniphore.com/actioniq/) — **К**. Composable CDP: работа с клиентскими данными, аудиториями и активацией поверх корпоративной data-инфраструктуры.
- [Amperity](https://docs.amperity.com/reference/bridge.html) — **К**. Customer-data ingestion, identity resolution и обмен с хранилищами; [destinations](https://docs.amperity.com/reference/page_destinations.html) доставляют аудитории во внешние системы.
- [BlueConic](https://www.blueconic.com/resources/cdp-integrations) — **К**. Двусторонние интеграции клиентских данных с CRM, e-commerce, рекламой, аналитикой и хранилищами; единые профили и активация.
- [Zeotap CDP](https://zeotap.com/integrations/) — **К**. Интеграции с CRM, cloud storage, data lakes/warehouses и маркетинговыми системами; объединение и доставка customer data.
- [Optimove](https://www.optimove.com/resources/learning-center/customer-data-platform) — **К**. CDP с объединением клиентских данных, аналитикой и оркестрацией коммуникаций; сценарный конкурент по ingestion и activation.
- [Microsoft Dynamics 365 Customer Insights — Data](https://learn.microsoft.com/en-us/dynamics365/customer-insights/data/data-sources) — **К**. Загрузка из корпоративных источников, унификация профилей и подготовка данных для активации. Отдельный продукт Microsoft, не второе название Fabric Data Factory.
- [Salesforce Data 360 (Data Cloud)](https://developer.salesforce.com/docs/data/data-cloud-int/references/data-cloud-ingestionapi-ref/c360-a-api-get-started.html) — **К**. Ingestion API, объединение клиентских данных, сегментация и активация в экосистеме Salesforce; оба названия относятся к одной продуктовой линии.
- [SAP Customer Data Platform](https://help.sap.com/docs/customer-data-platform/user-guide/cdp-monitoring-dashboard) — **К**. Customer-data ingestion и activation с мониторингом обоих направлений; отдельный продукт от SAP Integration Suite и Datasphere.
- [Oracle Unity Data Platform / Unity CDP](https://docs.oracle.com/en/cloud/saas/cx-unity/cx-unity-user/Help/GetStarted/Overview_CXUnity.htm) — **К**. Ingest/export jobs, доставка сегментов и identity resolution. Не смешивать с универсальными Oracle GoldenGate и Oracle Integration.
- [Bloomreach Engagement / Customer Data Engine](https://www.bloomreach.com/en/products/data-engine?spz=learn_orig) — **К**. CDP и автоматизация маркетинга: клиентские события, единые профили и активация; пересечение по прикладным customer-data pipelines.
- [Acquia CDP](https://docs.acquia.com/customer-data-platform/native-and-standard-connectors) — **К**. Входные и выходные коннекторы API, SFTP и S3, профили и маркетинговые назначения; [API-передача и извлечение данных](https://docs.acquia.com/customer-data-platform/api-integration).
- [Redpoint CDP](https://docs.redpointglobal.com/bpd/redpoint-reference-architectures) — **К**. Интеграция customer-data инфраструктуры с BigQuery, Snowflake, Databricks и системами активации; конкуренция по сборке клиентских data pipelines.
- [Simon Data](https://www.simondata.com/integrations?a28476fb_page=2) — **К**. Источники и назначения для клиентских данных, соединение хранилищ с маркетинговыми системами и оркестрация активации.
- [Lytics](https://docs.lytics.com/docs/lytics-integration-options) — **К**. SDK, webhooks, файловые и warehouse-интеграции; [Cloud Connect](https://www.lytics.com/cloud-connect/) закрывает reverse ETL из хранилища в прикладные инструменты.
- [Meiro](https://meiro.io/integrations/) — **К**. Двусторонние интеграции CRM, программ лояльности, хранилищ и маркетинговых систем; в каталоге есть Kafka, PostgreSQL, MySQL, SFTP и REST API.
- [NGDATA Intelligent Engagement Platform](https://ngdata.com/intelligent-engagement-platform) — **К**. Готовые и заказные коннекторы для объединения источников в клиентские профили и передачи результатов в системы маркетинговой автоматизации.
- [Blueshift](https://help.blueshift.com/hc/en-us/articles/8514734693267-Export-customer-data) — **К**. Customer-data activation и экспорт через API, S3, SFTP и интеграции с внешними приложениями; поддерживается выгрузка сегментов по расписанию.
- [Lexer](https://www.lexer.io/platform/connectors) — **К**. Customer-data ingestion из API, БД и файлов; отправка профилей и аудиторий в email, рекламу и loyalty-системы. Часть коннекторов предоставляется через Fivetran.
- [Insider One](https://insiderone.com/customer-data-integration-unified-profiles-guide/) — **К**. SDK, серверные API, batch-источники и коннекторы для клиентских профилей и межканальной активации; маркетинговая платформа с интеграционным слоем.
- [Sensors Data — 神策 CDP](https://www.sensorsdata.cn/product/cdp.html) — **К**. Китайская enterprise CDP: SDK и batch-ingestion, отображение таблиц хранилища, моделирование и потоковая подписка на выходные данные.
- [GrowingIO CDP](https://www.growingio.com/en/products/cdp) — **К**. Китайская CDP: SDK, прямой импорт БД и файлов, OneID, экспорт через OpenAPI, offline-выгрузки и real-time subscriptions.

### Движки с доступным исходным кодом

- [Apache Unomi](https://unomi.apache.org/) — **O**. Customer-data/context server для профилей, событий и сегментации; строительный блок собственной CDP. Готовность конкретных интеграций проверять по [руководству](https://unomi.apache.org/manual/latest/).
- [Tracardi](https://github.com/Tracardi/tracardi) — **S/К**. API-first CDP: приём событий, объединение профилей, workflows и доставка в другие системы. В репозитории указана MIT with Commons Clause; несмотря на маркетинговое «open source», здесь не классифицируется как open source по определению OSI. Возможности очередей и масштабирования зависят от редакции.

### Дополнительные платформы маршрутизации customer events

- [MetaRouter](https://docs.metarouter.io/docs/getting-started-with-metarouter) — **К**. Сбор событий приложений через SDK и серверная доставка в аналитику, рекламу и data-инструменты; [преобразования](https://docs.metarouter.io/docs/integration-transformations) задаются для каждого назначения.
- [Customer.io — Data & integrations](https://customer.io/platform/data-integrations) — **К**. API-first интеграция, объединение и активация клиентских данных; [каталог интеграций](https://customer.io/integrations). Пересечение по событиям и синхронизации прикладных систем.

### Смежные инструменты: более узкое пересечение

- [Carrot quest](https://www.carrotquest.io/blog/cdp-customer-experience-personalization/) — **К**. Клиентские события и профили для персонализации и коммуникаций. Смежный слой engagement; универсальный перенос таблиц и CDC не подтверждены.
- [DashaMail CDP](https://dashamail.ru/features/cdp/) — **К**. Веб-трекинг, товарные данные и события для триггерных рассылок; узкий конкурент по сбору и активации событий e-commerce.
- [Flocktory](https://www.flocktory.com/) — **К**. Персонализация и маркетинговые сценарии с [API заказов и купонов](https://cabinet.flocktory.com/help/client-api). Смежная интеграция commerce-данных; наличие статьи о CDP само по себе не доказывает универсальный CDP/ETL-продукт.

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
интеграционных workflows. Дополнительно выделены [semantic data hubs](#semantic-data-hubs)
и [операционная синхронизация](#operational-sync).

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

<a id="semantic-data-hubs"></a>
### 17.1. Semantic data hubs и синхронизация мастер-данных

Дополнение проверено **2 октября 2026 года**. Ближайшая к Sesam группа —
платформы, которые принимают данные нескольких систем, поддерживают общую
модель сущностей и предоставляют обработанные данные другим приложениям.
MDM-продукты в этой группе — сценарные альтернативы, а не автоматически
эквивалентные движки переноса. Matching, merge, golden records и правила
приоритета источников меняют семантику данных; поддержку произвольного CDC,
сохранение исходных ключей и гарантии доставки нужно проверять отдельно.

- [Sesam Hub](https://docs.sesam.io/hub/data-architecture.html) — **К**. Semantic data hub для master-data synchronization: входные pipes, datasets, общая модель и обратная доставка в бизнес-системы. [Datasets](https://docs.sesam.io/hub/documentation/building-blocks/datasets.html) используют журнал с continuation; [GitHub-организация](https://github.com/sesam-io) содержит коннекторы и примеры, что не доказывает открытость всего ядра.
- [Syncari](https://syncari.com/integration-platform/) — **К**. Stateful multi-directional sync между CRM, ERP, warehouse и другими приложениями; отслеживание изменений, преобразование и объединение данных с обновлением подключённых систем.
- [Cinchy Data Collaboration Platform](https://docs.cinchy.com/data-syncs/building-data-syncs/) — **К**. Общий слой данных и batch/event sync с источниками и приёмниками SQL, Kafka, REST и SaaS. Документация показывает маршрут Salesforce → Cinchy → HubSpot.
- [CluedIn](https://documentation.cluedin.net/integration/introduction) — **К**. Ingestion из приложений, БД и файлов, обработка мастер-данных; [streams и export targets](https://documentation.cluedin.net/getting-started/data-streaming) доставляют записи, например в SQL Server, в режимах синхронизации состояния или журнала событий.
- [Reltio](https://www.reltio.com/) — **К**. Multidomain MDM и унификация сущностей с коннекторами для приёма, обогащения и распространения данных; пересечение по сборке операционного data hub.
- [Semarchy Data Platform](https://docs.semarchy.com/self-hosted/1.0.0/guides/design/certification/integration) — **К**. Интеграционные jobs для движения и преобразования данных в/из master-data hub, публикация сертифицированных данных через запросы и представления.
- [Profisee](https://profisee.com/platform/integration/) — **К**. MDM с интеграционным слоем и публикацией согласованных данных через открытые стандарты и подключённые инструменты. Сценарный конкурент по доставке master data, а не универсальный CDC.
- [Ataccama ONE MDM](https://docs.ataccama.com/mdm/latest/overview.html) — **К**. Модели сущностей, входные/выходные интерфейсы, matching и master records; [load/export operations](https://docs.ataccama.com/mdm/latest/mdm-integration/mdm-integration.html) для интеграции с окружающими системами.
- [Stibo Systems STEP](https://doc.stibosystems.com/doc/version/2025.2/web/content/resmat/javascript/gateway_integration_endpoint_bind.html) — **К**. MDM/PIM с интеграционными endpoints; gateway-интерфейсы обеспечивают входной и выходной обмен с внешними системами. Пересечение по распространению согласованных справочников и сущностей.
- [Pimcore Datahub](https://docs.pimcore.com/platform/Datahub/Basic_Principle/) — **S/К**. Приём и выдача данных Pimcore через настраиваемые endpoints; GraphQL и дополнительные REST/file/webhook-адаптеры. [Редакции 2026.1](https://docs.pimcore.com/platform/2026.1/Pimcore_Platform/Pimcore_Editions/) используют POCL; доступность исходников не приравнивается к OSI open source.
- [MarkLogic Data Hub](https://docs.marklogic.com/datahub/6.2/flows/about-flows.html) — **М**. Data-hub приложение поверх коммерческого MarkLogic: ingestion, mapping, matching и merging в flows; сценарный конкурент по консолидации и подготовке данных. [Исходники Data Hub](https://github.com/marklogic/marklogic-data-hub) не означают открытость всей серверной платформы.
- [Syndigo MDM / Integration Studio](https://syndigo.com/integration-studio/) — **К**. Многодоменная платформа мастер-данных и интеграционный слой для синхронизации и распространения данных в бизнес-системы и каналы.
- [Boomi Data Hub](https://help.boomi.com/docs/Atomsphere/Master%20Data%20Hub/Boomi_DataHub_Overview/) — **К**. Master-data synchronization service, связанный с Boomi Integration; общий домен данных и согласование подключённых источников. Отдельный продукт уже учтённого вендора Boomi, не новая компания.
- [TIBCO EBX](https://www.tibco.com/products/ebx) — **К**. Управление и распространение master/reference data, моделей и иерархий; сценарный конкурент в MDM-интеграции. Отдельный продукт TIBCO, а не переименование BusinessWorks.
- [Informatica Multidomain MDM SaaS](https://www.informatica.com/content/dam/informatica-com/en/collateral/data-sheet/multidomain-mdm-saas-on-google-cloud-platform_data-sheet_4560en.pdf) — **К**. Входной и выходной обмен master data через интеграционные сервисы IDMC: batch/bulk, API и очереди. Отдельный MDM-продукт уже учтённого вендора Informatica.

Уже включённые **K2view, Nexla и Skyvia** также пересекаются с этой группой;
**CDP** отдельно перечислены в [разделе клиентских данных](#customer-events).
Наличие общего вендора не означает идентичность его ETL, iPaaS и MDM-продуктов.

<a id="operational-sync"></a>
### 17.2. Двусторонняя и отраслевая синхронизация приложений

Эти продукты ближе к Sesam по задаче поддержания согласованного состояния
приложений, но не обязательно используют semantic hub или общую MDM-модель.
Поддержка двух направлений проверяется для конкретной пары коннекторов;
рекламное «real-time» не является измерением задержки или доказательством SLA.

- [Stacksync](https://www.stacksync.com/) — **К**. Двусторонняя синхронизация операционных данных между бизнес-приложениями и базами данных; пересечение по постоянному переносу изменений.
- [Unito](https://unito.io/) — **К**. Two-way sync между SaaS-инструментами; согласование записей и полей в подключённых приложениях. Более узкая прикладная альтернатива универсальному переносу БД.
- [HubSpot Data Sync / Data Hub](https://knowledge.hubspot.com/integrations/connect-and-use-hubspot-data-sync?from=HN) — **К**. Одно- и двустороннее обновление объектов между HubSpot и поддерживаемыми приложениями, mapping и контроль синхронизации. Конкуренция в HubSpot-центричных сценариях.
- [DBSync Cloud Workflow](https://docs.mydbsync.com/cloud-workflow) — **К**. No-code/low-code iPaaS для SaaS, облачных и локальных приложений; коннекторы и workflows для переноса операционных записей.
- [Rapidi](https://www.rapidionline.com/en/) — **К**. Интеграция и синхронизация ERP и CRM; сценарная альтернатива для согласования бизнес-данных между прикладными системами.
- [Commercient SYNC](https://www.commercient.com/) — **К**. Интеграции ERP, CRM и e-commerce; перенос бизнес-объектов в готовых отраслевых маршрутах. Направления и состав объектов зависят от интеграции.
- [Exalate](https://docs.exalate.com/docs/overview-what-is-exalate) — **К**. Синхронизация tickets, issues, work items и cases между Jira, ServiceNow, Salesforce, Azure DevOps и другими системами; независимые правила и mapping на каждой стороне.
- [ZigiOps (ZigiWave)](https://www.zigiwave.com/zigiops-integration-platform) — **К**. Двусторонние интеграции ITSM/ITOM/CRM, поля и связи объектов, правила преобразования и разрешения конфликтов; SaaS и локальные поставки.
- [ONEiO](https://www.oneio.cloud/) — **К**. Управляемый сервис интеграции для enterprise IT и сервис-провайдеров; поставщик проектирует и эксплуатирует интеграции. Альтернатива по результату обмена данными, с другой моделью эксплуатации.

### 17.3. Дополнительные iPaaS, ESB и B2B-интеграция

Смежные с Sesam альтернативы по соединению приложений. Общая модель сущностей,
MDM и двустороннее согласование состояния не следуют автоматически из наличия
коннекторов или workflow-редактора.

- [Alumio](https://www.alumio.com/) — **К**. iPaaS для business-critical интеграций, соединения приложений и организации потоков операционных данных.
- [APPSeCONNECT](https://www.appseconnect.com/) — **К**. Low-code интеграция ERP, CRM, e-commerce и маркетплейсов через готовые пакеты и маршруты.
- [Patchworks](https://doc.wearepatchworks.com/product-documentation/welcome/what-is-patchworks) — **К**. iPaaS для соединения приложений; отраслевой акцент на commerce-интеграциях и операционных потоках.
- [Magic xpi](https://www.magicsoftware.com/integration-platform/xpi-lp/) — **К**. Интеграционная платформа Magic Software для соединения приложений и автоматизации процессов; конкуренция по enterprise application integration.
- [Adeptia](https://www.adeptia.com/) — **К**. Приём данных партнёров, преобразования, бизнес-правила и оркестрация операционных потоков; локальная, облачная и гибридная эксплуатация.
- [Lobster Data Platform](https://www.lobstersoftware.com/fr/) — **К**. No-code интеграция ERP, поставщиков и логистики через API, EDI и облачные системы; синхронизация операционных данных supply chain.
- [Cleo Integration Cloud](https://support.cleo.com/hc/en-us/articles/360044671294-Overview-of-CIC-Cloud-Edition) — **К**. B2B, application и data integration; API, EDI и файловый обмен для связанных бизнес-процессов.
- [SEEBURGER BIS](https://www.seeburger.com/) — **К**. Business Integration Suite для EAI/A2A, API, B2B/EDI и managed file transfer; облачная, локальная и гибридная интеграция.
- [InterSystems IRIS Interoperability](https://www.intersystems.com/products/intersystems-iris/interoperability/) — **К**. Интеграционный движок в data platform: соединение приложений, протоколов и форматов сообщений. Сравнивать именно interoperability-сценарии, а не только СУБД IRIS.
- [IBM App Connect](https://www.ibm.com/products/app-connect/connectors) — **К**. Коннекторы и integration flows для cloud, SaaS и on-premises приложений и БД. Отдельная продуктовая линия от IBM webMethods, DataStage и StreamSets.
- [Google Cloud Application Integration](https://docs.cloud.google.com/application-integration/docs/overview) — **К**. Управляемая iPaaS: SaaS/БД/Pub/Sub-коннекторы, событийные и плановые запуски, mapping. Не смешивать с уже учтёнными Dataflow и Datastream.
- [Azure Logic Apps](https://learn.microsoft.com/en-us/azure/logic-apps/logic-apps-what-are-logic-apps) — **К**. Workflows с коннекторами и триггерами для приложений и сервисов; сценарный конкурент по автоматизации обмена. Отдельная линия от Azure/Fabric Data Factory.

<a id="regional-platforms"></a>
## 18. Российские платформы интеграции и ETL

Yandex/Transferia уже включены в [основную группу](#universal-ingestion).
Российские CDP, включая **CleverData Join, Mindbox и Altcraft**, перечислены
в [разделе customer data](#customer-events); повторно здесь не считаются.

- [Arenadata Streaming](https://arenadata.tech/ru/products/ads) — **К, на OSS-компонентах**. Kafka, NiFi и корпоративная потоковая интеграция.
- [Loginom](https://loginom.ru/) — **К**. Визуальная подготовка данных и ETL.
- [DATAREON Platform](https://datareon.ru/solution/datareon-platform/) — **К**. Интеграционная платформа, обмен данными и ETL.
- [Digital Q.DataFlows](https://q.diasoft.ru/mediacenter/news/v-platforme-digital-q-dataflows-dobavlena-vozmozhnost-ispolzovaniya-naborov-dannykh-v-etl-protsessakh/) — **К**. Проектирование и выполнение ETL-процессов.
- [Neoflex Datagram](https://www.neoflex.ru/publications/neoflex-datagram-etl-platform) — **К**. Платформа ETL и параллельной обработки данных.

- [Юнидата / Unidata MDM](https://unidata-platform.ru/images/UD%20Integration%20guide%205.6.pdf) — **К**. Приём, консолидация и предоставление мастер-данных через интеграционные интерфейсы. Руководство относится к версии 5.6; текущую матрицу коннекторов нужно уточнять. Ближе к semantic data hub, чем к универсальному CDC.
- [Entaxy / Entaxy ION](https://entaxy.ru/) — **К, на OSS-компонентах**. Российская low-code интеграционная платформа/ESB, маршруты обмена между информационными системами. Наличие OSS-компонентов не устанавливает лицензию всей поставки.
- [Bercut HIP / Bercut ESB](https://hip.bercut.com/esb) — **К**. Интеграционная шина в составе гибридной платформы: проектирование, исполнение и сопровождение обменов между приложениями. HIP и ESB учтены одной продуктовой семьёй.

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
- [dbcrossbar](https://github.com/dbcrossbar/dbcrossbar) — **O**. Rust CLI для больших табличных переносов между БД, CSV и cloud storage, schema conversion и upsert; альтернатива для batch-копирования, непрерывный CDC не подтверждён.

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
- [CocoIndex](https://github.com/cocoindex-io/cocoindex) — **O**, **смежный проект**. Инкрементальная обработка и обновление производных индексов/AI-контекста при изменении источников. Rust-ядро и Python API; пересекается с подготовкой данных для AI/RAG, не заменяет универсальный гетерогенный CDC-перенос.

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
| **infinyon/fluvio** | Текущий репозиторий — [fluvio-community/fluvio](https://github.com/fluvio-community/fluvio); это тот же проект |
| **BemiHQ/BemiDB** | Текущий репозиторий — [pgstack-io/BemiDB](https://github.com/pgstack-io/BemiDB); не отдельный конкурент |

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
- [Tremor](https://github.com/tremor-rs/tremor-runtime) — **O**. Rust event-processing/ETL, routing и коннекторы Kafka/S3/HTTP; README ограничивает применение для тяжёлых joins больших потоков. [Блог](https://www.tremor.rs/blog/) заканчивается сентябрём 2022 года, в релизах есть RC; текущая production-поддержка не установлена.
- [Streamdal](https://github.com/streamdal/streamdal) — **O**. Исторический аналог обработки данных; репозиторий архивирован 22 февраля 2026 года.
- [DoubleCloud Transfer](https://double.cloud/services/doublecloud-transfer/) — **К**, исторический конкурент по переносу данных. [Официальное сообщение 1 октября 2024 года](https://double.cloud/blog/posts/2024/10/doublecloud-final-update/index.html) объявило сворачивание деятельности; старую документацию не считать подтверждением действующего сервиса.

## Как использовать каталог

Способы разбиения исходных таблиц, аудит локальных исходников и коммерческой
документации, а также предложения для Transferia разобраны в отдельном
[исследовании](table-splitting-research-2026-09-20.md).

Для сравнения с Transferia Rust прежде всего рассматривать готовые системы
переноса, CDC и коннекторные движки. Специализированные приёмники, нативная
репликация БД, оркестраторы, вычислительные библиотеки и федерация решают
отдельные части задачи и не являются автоматически полной заменой.

При расширении каталога проверять матрицу **сценарий × рынок × модель поставки**:
универсальная интеграция, semantic data hub/MDM, одно-/двусторонняя и
многосторонняя операционная синхронизация, customer events/CDP, reverse ETL и отраслевые
платформы; международный, российский и китайский рынки; managed, self-hosted
и OSS/source-available. Для каждого кандидата сохранять официальный источник,
конкретный входной/выходной маршрут и границу пересечения. Переименования и
семейства продуктов сверять до подсчёта. Один список «лучших CDP» не доказывает
полноту, а статья поставщика с определением CDP не доказывает наличие продукта.

Каталог не доказывает равенство гарантий доставки. Snapshot consistency,
порядок изменений, exactly-once, сохранение типов и схем, replay, backpressure,
обработка ошибок и поведение при schema drift должны сравниваться отдельно
для конкретной пары источника и приёмника, редакции и версии продукта.
