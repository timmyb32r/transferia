# Механизмы и условия интерпретации

Разбор измеренных путей и настроек. Числа и выводы — в [REPORT.md](REPORT.md). Ссылки на конкретные снимки исходников — в `source-references.json`; версия исследованной ветки не автоматически равна версии бинарника. Настройки каждого запуска сохраняются отдельно без паролей.

| Участник | Чтение / разбиение в этом эксперименте | Запись и дополнительная работа | Что может менять результат |
|---|---|---|---|
| Transferia Rust Auto | Production release; максимум 1/4 части. При P4 на этих fixtures Auto выбирает CTID-диапазоны. COPY binary → Arrow. | PostgreSQL COPY binary; ClickHouse Native + ZSTD, блоки 65536. Служебные metadata-колонки. | Физические диапазоны обходят индекс; это иной план, чем диапазоны PK. Планирование и discovery входят во время. |
| Transferia Rust exact ranges | Отдельный release из изолированной копии; явный benchmark-only override на Indexed, 1/4 части, синтетические четверти PK. Общий production reader/writer. | Тот же sink и data path, что Auto. | Этот результат нельзя выдавать за поведение Auto или настройку готового продукта. Код override включён в harness. |
| Transferia Go | Предоставленный пользователем transferctl; основной typed-путь `NoHomo=true`. `SnapshotDegreeOfParallelism=1/4`, `DesiredTableSize=1`, чтобы размерный порог не запрещал разбиение. BIGSERIAL активирует sequence-key splitter. | Snapshot binary serialization; собственный pipeline; PG pgx CopyFrom, CH Native + LZ4 в исследованном Go-коде. | Sequence splitter сначала считает `percentile_disc`, затем может перейти на min/max. На обычном BIGINT без sequence код выбирает другой путь, вплоть до modulo/hash. Стоимость получения границ входит во время. |
| SeaTunnel 2.3.12 | JDBC, `partition_num=1/4`, числовой id, bounds 1..N+1; Zeta local runtime. | PG JDBC batch 1000 с `reWriteBatchedInserts=true`; CH connector batch 65536. | JVM/Zeta/Hazelcast startup; `fetch_size=8192`; PG dialect отключает autocommit для cursor fetch. Для CH старый встроенный драйвер потребовал выключить несовместимую response compression. |
| Sling | Внешние 1/4 процесса с явными непересекающимися SQL-диапазонами в одном cgroup. | Собственная temp table на каждый процесс, затем INSERT в итоговую. PG COPY на загрузке; CH Native + ZSTD в исследованном коде. | Staging добавляет работу БД. Чтение по умолчанию не обязательно COPY: код использует обычный stream/cursor, если `allow_bulk_export` не включён. |
| DataX | Один job, один reader/writer content; `querySql` содержит 1/4 точных диапазона, channels=1/4. | PG JDBC batch 1000. | Несколько элементов `job.content` в использованной сборке не выполнили все диапазоны: smoke поймал 2500/10000 строк. Исправлено на массив querySql до измерений. Стандартный splitPk может создавать дополнительные части; здесь он не используется. |
| Airbyte | Официальные source-postgres 3.8.5 и destination-postgres 3.0.18, STDIO-протокол. Число соединений P; internal sample size=P и throughput prior=P bytes/s заставляют production CTID splitter создать ровно P диапазонов на непустой fixture (подтверждается source INFO-логами). | Два JVM-процесса и release Rust bridge, считающий записи объединённого потока для sourceStats checkpoint; payload проходит без изменений. JSON-протокол → direct-load destination; в логах PostgresInsertBuffer пишет блоки по 100000 строк. Исследованный buffer использует временный CSV и COPY. | Лимит соединений не доказывает, что одна таблица читается четырьмя потоками. Результат нельзя помещать в строгую P4-группу без доказательства. Это connector pair, без Kubernetes/control plane Airbyte. |
| Flink CDC 3.4.0 / Flink 1.20.3 | PostgreSQL incremental snapshot implementation в bounded `scan.startup.mode=snapshot`; chunk size N/P, parallelism P. | JDBC sink с PK/upsert-контрактом; checkpoint interval 5s; cluster + SQL client запускаются внутри измерения. | Это не просто SELECT-экспорт: snapshot coordination и changelog-capable sink выполняют дополнительную работу. Повторяющиеся ключи sink обрабатывает иначе, чем строгий INSERT. Fixture неизменяемый, итоговая уникальность проверяется. |
| Debezium 3.6.3.Final + Kafka 4.3 + JDBC | `initial_only`, `snapshot.max.threads=P`, multiplier=1, legacy=false. Одна таблица должна попасть в native chunked snapshot. | Kafka broker и Kafka Connect, JSON envelopes, JDBC Sink insert-only; всё входит в cgroup. | При P1 legacy single scan; при P4 COUNT(1) всей таблицы, три ORDER BY PK OFFSET запроса границ и snapshot coordination. Overrides/no-key могут отключать chunking. Kafka partition count и sink task count согласованы с P. Не сравнивать один source connector с полным ETL. |
| InLong 2.4.0 / Flink 1.18.1 | Официальный `jdbc-inlong`, `scan.partition.*`, 1/4 числовых диапазона, `scan.auto-commit=false` для настоящего cursor fetch. | JDBC sink batch 1000, Flink runtime и SQL client. | Измеряется standalone Sort data plane, без InLong Manager/Agent. Connector fat JAR содержит planner, поэтому конфликтующий planner-loader удалён из эфемерного контейнера. |
| Meltano 4.2.2 | MeltanoLabs tap-postgres 0.10.0, FULL_TABLE, custom_where_clauses; 1/4 внешних pipeline. | target-postgres 0.8.0, `use_copy=true`; Singer JSON между процессами. Код target при наличии key_properties всё равно делает COPY в temp table + merge/upsert, несмотря на переданный load_method. | Python/JSON и per-record конверсия могут ограничивать CPU. Это штатный движок конкурента; Transferia не заменяется Python-клиентом. Весь CPU всех tap/target и Meltano учитывается. |
| Spark 3.5.7 | Native JDBC DataFrame reader, массив 1/4 exact predicates; local[P]. Python только строит план, строки обрабатывает JVM. | JDBC DataFrame writer; PG batch 1000, CH batch 65536. PG commit на границе partition/task, не после каждого batch. | JVM/Spark startup, internal row representation, JDBC conversion. Нет driver-side collect. Для CH явно выбран DriverV1 0.9.8; это не измерение нового V2 writer. |
| Sqoop 1.4.7 / Hadoop 2.10.2 | import с split-by id, bounds 1..N+1, 1/4 mapper в local MapReduce. | Локальные промежуточные файлы, затем отдельный export; обе фазы измеряются. Fetch size по умолчанию 1000; export defaults 100 записей/statement и 100 statements/transaction. | Это архивный инструмент. Сравнение одного import с end-to-end переносом было бы нечестным. Hadoop `preferIPv4Stack=false` требуется для IPv6-only managed hosts. |
| Estuary Flow local preview | Официальный source-postgres-batch dev, explicit SELECT ranges, 1/4 внешних preview pipelines. | Captured JSONL на диск → materialize-postgres dev. Tiny release Rust byte-copy adapter завершает capture после известного N и добавляет fixture commit markers через 1000 документов. | Это **не managed Flow service**: нет Gazette/data-plane distribution. Время/CPU staging включены. Stock CDC connector не признаёт `mdb_replication`; batch connector подходит именно snapshot-задаче. Precreated `flow_document` устраняет параллельную гонку DDL. |

## Одинаковая нагрузка не означает одинаковые гарантии

Все успешные результаты проверяют одну и ту же конечную таблицу: число строк, уникальность PK и все пять значений каждой строки. Это не доказательство одинаковой crash consistency, CDC semantics или поведения на изменяющемся источнике. PostgreSQL остаётся неизменяемым на время кампании; разные MVCC snapshots в независимых external-range процессах здесь эквивалентны по содержимому, но не обязательно будут эквивалентны в production с concurrent writes.

Бюджет 16 logical CPU / 24 GiB общий для всех процессов участника. CPU PostgreSQL/ClickHouse находится за пределами клиентского cgroup. SQL execution time из pg_stat_statements — **не CPU**, включает иные ожидания и может не учитывать COPY/utility statements. Показатель rows/CPU-second нужно читать вместе с wall-clock throughput: медленный ожидающий сети процесс может быть экономным по CPU.

Сохранены необходимые движкам дополнительные поля/слои: у Rust четыре `_system_*` колонки в обоих приёмниках, у Estuary в PostgreSQL заранее создан `flow_document`, у Airbyte измеренный append direct-load путь. Проверка БД не обнаружила raw-таблиц в fair21_raw или дополнительных _airbyte-полей в конечной narrow10m; старые названия параметров не доказывают наличие legacy raw/type-and-dedupe стадии. Поэтому одинаковы пять пользовательских полей, их значения и PK/NOT NULL, но не весь физический layout и не число промежуточных копий. Текстовые поля PostgreSQL могут быть `text` или неограниченным `character varying`; аудит допускает оба неограниченных строковых типа, но не VARCHAR(n). В ClickHouse одинаковы пять пользовательских non-null полей и MergeTree ORDER BY id; у Rust дополнительно присутствуют nullable metadata-поля.

## Первичные источники

- [Debezium PostgreSQL snapshot settings](https://debezium.io/documentation/reference/stable/connectors/postgresql.html), локально `RelationalSnapshotChangeEventSource` и `ChunkBoundaryCalculator`.
- [SeaTunnel JDBC](https://seatunnel.apache.org/docs/2.3.12/connector-v2/source/Jdbc/), локально `ChunkSplitter`, `PostgresDialect`.
- [Spark JDBC predicates and options](https://spark.apache.org/docs/3.5.7/sql-data-sources-jdbc.html).
- [Estuary PostgreSQL batch](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/postgres-batch/), [PostgreSQL materialization](https://docs.estuary.dev/reference/Connectors/materialization-connectors/PostgreSQL/), [local preview](https://docs.estuary.dev/concepts/flowctl/).
- [Meltano tap-postgres](https://hub.meltano.com/extractors/tap-postgres/), [target-postgres](https://hub.meltano.com/loaders/target-postgres/).
- [Airbyte PostgreSQL source](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/postgres.md).

Настройки и измеренные query traces имеют приоритет над предположениями по документации другой версии. Механизм в исходниках объясняет возможную причину; чтобы утверждать причинность количественно, нужен отдельный A/B-запуск с изменением одного фактора.

## Проверка точной версии Debezium

В образе `quay.io/debezium/connect:3.6` фактически находится **3.6.3.Final**, Kafka **4.3.0**. Проверен тег `v3.6.3.Final`, commit `8ee69a6972ae1ab50b90550a0b5bef082ff8bdd3`, а не только текущая ветка:

- [RelationalSnapshotChangeEventSource](https://github.com/debezium/debezium/blob/v3.6.3.Final/debezium-connector-common/src/main/java/io/debezium/relational/RelationalSnapshotChangeEventSource.java): P1/multiplier=1 использует legacy table scan; P4/multiplier=1 запускает chunked snapshot; `rowCountForTableChunked` выполняет `SELECT COUNT(1)`.
- [ChunkBoundaryCalculator](https://github.com/debezium/debezium/blob/v3.6.3.Final/debezium-connector-common/src/main/java/io/debezium/pipeline/source/snapshot/chunked/ChunkBoundaryCalculator.java): отдельный запрос для каждой границы на позиции i×N/P.
- [JdbcConnection](https://github.com/debezium/debezium/blob/v3.6.3.Final/debezium-connector-common/src/main/java/io/debezium/jdbc/JdbcConnection.java): SQL границы — `ORDER BY key OFFSET position ROWS FETCH NEXT 1 ROWS ONLY`.

Для равномерного миллиона строк P4 позиции составляют 250000/500000/750000. Это около 1.5N просмотренных индексных позиций для трёх OFFSET-запросов, дополнительно к COUNT и собственно чтению. Это оценка алгоритмической работы по коду, **не измеренное время и не доказательство физического disk I/O**: страницы могут быть в кеше. При росте P суммарная работа OFFSET растёт примерно как N×(P−1)/2; не следует механически увеличивать число частей.

## Airbyte без полной платформы

Прямой source→destination pipe с параллельным source выдавал ошибку `Sync completed, but unflushed states were detected`: первый checkpoint сообщал 2506 записей, когда merged transport уже передал 10000. Коннекторные бинарники не менялись. Benchmark bridge явно считает записи на границе транспорта и подставляет этот счётчик в `sourceStats.recordCount`, сохраняя payload и checkpoint. Весь CPU bridge включён. Это адаптация локального протокольного запуска, а не утверждение о дефекте полной платформы Airbyte. Её собственный [platform-level record counting](https://airbyte.com/blog/automatic-detection-of-dropped-records) также является отдельной частью протокола.

## Batch не равен транзакции

Одинаковое число 1000 в настройке batch не выравнивает durability/commit cadence. DataX `CommonRdbmsWriter.doBatchInsert` вызывает commit после буфера, Spark JDBC writer выполняет несколько executeBatch внутри транзакции partition/task, Sqoop отдельно задаёт records/statement и statements/transaction. Это влияет на round trips, WAL flush, время удержания транзакции и поведение при сбое. Мы проверяем конечную таблицу после успешного завершения, но не называем гарантии этих путей идентичными.

DataX в принятом narrow-прогоне явно сообщил `byte_speed_limit=-1` и `record_speed_limit=-1`: искусственный throttle не включён. Его использованный дистрибутив содержит PostgreSQL JDBC 42.3.3 и ClickHouse JDBC 0.2.4; часть других JVM-инструментов использует общий PostgreSQL JDBC 42.7.13, Spark→CH — 0.9.8 DriverV1. Версии драйверов не унифицировались заменой встроенных зависимостей движков.

Общие PostgreSQL JDBC URL стенда явно задают `prepareThreshold=0` и `reWriteBatchedInserts=true`; это часть выбранной конфигурации, а не утверждение о defaults каждого продукта. Отдельный рандомизированный блок 0/5 выполнен для DataX и Spark: примерно 1,00× и 1,04× по throughput соответственно. Три повтора и два движка не доказывают отсутствия эффекта у остальных участников; [точные результаты](DIAGNOSTICS.md).

## Проверенные планы PostgreSQL

`query-plans.json` содержит EXPLAIN без ANALYZE на том же источнике: полное чтение выбирает Seq Scan, четверть PK — Index Scan, физическая четверть — Tid Range Scan. Для narrow оценка total cost первой PK-четверти 10980 против 7604 у CTID; для wide 41437 против 38215. Это **оценки планировщика, не миллисекунды**. На этих данных CTID экономит индексный обход, но величина выигрыша всей доставки дополнительно зависит от sink, сети и преобразования строк. Данные загружены в порядке PK; на перемешанном heap разница может быть другой.

## Дополнительный Debezium bulk-вариант

Отдельная строка `debezium_bulk` использует тот же snapshot, число частей, broker, JDBC sink и таблицу, но producer batch=256 KiB, linger=5 ms, LZ4, buffer=64 MiB; consumer fetch.min.bytes=1 MiB, fetch.max.wait.ms=50, max.partition.fetch.bytes=8 MiB, fetch.max.bytes=32 MiB. Максимум poll и JDBC batch остаются 1000. Сравнение измеряет **пакет настроек**: оно не изолирует индивидуальный эффект LZ4 или fetch.min.bytes.

Debezium завершается по внешнему критерию полного зафиксированного N в назначении, затем контроллер останавливает Connect и Kafka. `all_rows_committed_seconds` сохранён отдельно, основной wall включает их остановку. При одновременной остановке broker/Connect журнал может содержать LeaveGroup coordinator errors уже после полной записи. Это не тест восстановления Kafka offsets и не обещание CDC exactly-once. Managed PostgreSQL также вызывает предупреждение проверки флага rolreplication у user1; реальные snapshot/replication подключения работают через роль mdb_replication.

## Где ещё есть пространство настройки

Estuary materialize-postgres поддерживает `delta_updates`: исходник при этом пропускает чтение текущего документа. Основная серия использует стандартный keyed materialization local-preview путь; delta-mode отдельно проверен в [DIAGNOSTICS.md](DIAGNOSTICS.md). Ни один из них не является измерением managed Flow. Включение Sling `allow_bulk_export` переводит источник на COPY CSV через psql и меняет parser/process path; основной ряд использует штатный cursor path. Вариант Sling COPY не измерялся, поэтому обещать его ускорение по одному чтению исходников нельзя.

## Читатели, sink tasks и соединения — три разных числа

У Debezium JDBC Sink 3.6.3 [минимум пула по умолчанию равен 5, максимум 32](https://github.com/debezium/debezium/blob/v3.6.3.Final/debezium-connector-jdbc/src/main/java/io/debezium/connector/jdbc/JdbcSinkConnectorConfig.java). Поэтому 16 sink tasks требуют как минимум 80 соединений для своих пулов, а не 16. Первые P16 попытки упёрлись в Odyssey user pool_size=50; лимит user1 поднят до 200 на обоих тестовых PG, затем прогоны повторены. Наблюдение после P16 зафиксировало 80 idle backend-соединений назначения; это pooled backend connections, не 80 одновременно исполняемых запросов.

Это дополнительная нагрузка на управление соединениями и память БД, которая **не входит в client RSS**. Сравнение CPU клиента не является сравнением полной стоимости всей распределённой системы. Для основного P1/P4 лимит 50 не был достигнут; повышение лимита описано как изменение инфраструктурного допуска, а не ускоряющая настройка запросов.

## Промежуточные файлы и память

`intermediate-file-sizes.json` сохраняет размеры файлов после успешных прогонов, отдельно от таймера. В первом narrow/P4 Debezium Kafka record logs занимают 3417 MiB; с bulk-настройками — 155 MiB. Это примерно 22× меньше на диске, **не 22× быстрее доставка**. JSON schemas/envelopes сериализуются для каждой строки, а сжатие хорошо использует повторяемость такого потока. Формат конвертера остаётся JSON со схемами; Avro/Protobuf + Schema Registry не измерялись.

Cgroup memory включает файловый кеш Kafka, Sqoop staging и Flow preview fixtures; RSS показывает память процессов и может повторно учитывать shared pages. Поэтому график ресурсов показывает обе величины, и ни одна не называется чистым live heap. Внешние системы и память PostgreSQL backend-пулов остаются вне клиентского cgroup.

## Исправленные настройки стенда

- **InLong:** `scan.fetch-size=8192` недостаточно при default `scan.auto-commit=true`. [pgJDBC требует autocommit=false для cursor-based ResultSet](https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor). Поддержка настройки проверена в InLong 2.4 source factory; основной ряд повторён с false. Старые успешные результаты сохранены как superseded, поскольку измеряли иной способ буферизации. SeaTunnel 2.3.12 PostgresDialect и Sqoop сами отключают autocommit.
- **Go→CH:** первоначальный harness ошибочно передавал 65536 в `BufferTriggingSize`, принимая байтовый порог за число строк. Это давало около 256 узких строк на flush, а default Interval=1s добавлял искусственную паузу. Такой прогон прерван и исключён. Основной вариант явно задаёт существующий row-count trigger `InflightBuffer=65536`, byte ceiling=256 MiB и Interval=-1 ns, который в данном коде отключает interval-throttler и timer trigger. Последний неполный блок сбрасывается по окончанию snapshot. Это настройка benchmark, не изменение Go-бинарника.
- **DataX→CH:** batchSize=65536 — верхняя граница по строкам; общий writer также имеет default byte cap=32 MiB. На широких строках байтовый порог может сработать раньше. Этот штатный memory bound сохранён и не выдаётся за всегда одинаковое фактическое число строк в INSERT.
- **SeaTunnel heap:** local launcher добавляет `-Xmx512m` из `jvm_client_options` после JAVA_TOOL_OPTIONS, перекрывая общий `-Xmx4g`. Wide/P4→CH завершился `GC overhead limit exceeded`. Штатный `-DJvmOption=-Xmx4g` задаёт heap после defaults; повторный полный wide/P4 transfer прошёл. Все старые SeaTunnel main cases исключены и повторены с единым 4 GiB heap. Внешний лимит 24 GiB не менялся.

## Снимки исходников, использованные при разборе

Для release-matched позиций указан проверенный тег/commit. Для остальных это конкретный исследованный снимок, а не утверждение об идентичности запущенному binary.

| Компонент | Исследованный код | Соответствие бинарнику |
|---|---|---|
| transferia-go | [d32b451f3579](https://github.com/transferia/transferia/tree/d32b451f3579688a5cebd88032a266565bc96c35) | Снимок для анализа; binary identity отдельно |
| seatunnel | [1fce0a77c7af](https://github.com/apache/seatunnel/tree/1fce0a77c7af34884d4776bbb8c3f728a433f2e0) | Release tag 2.3.12 |
| sling | [9db0426f81e4](https://github.com/slingdata-io/sling-cli/tree/9db0426f81e45f8c8cf841e2883f50b38a3fb740) | Снимок для анализа; binary identity отдельно |
| datax | [80ec23d5c532](https://github.com/alibaba/DataX/tree/80ec23d5c5328eb90ca364d2749e92dfaf44541e) | Снимок для анализа; binary identity отдельно |
| debezium | [8ee69a6972ae](https://github.com/debezium/debezium/tree/8ee69a6972ae1ab50b90550a0b5bef082ff8bdd3) | Точная версия 3.6.3.Final |
| flink-cdc | [06581fc73b84](https://github.com/apache/flink-cdc/tree/06581fc73b84b1da45fccf93441d96a118d9bfc1) | Release tag release-3.4.0 |
| flink-jdbc | [73a3f34f455d](https://github.com/apache/flink-connector-jdbc/tree/73a3f34f455d43aa252106b109ea02d27fc3fadb) | Снимок для анализа; binary identity отдельно |
| inlong | [d348b01bda8e](https://github.com/apache/inlong/tree/d348b01bda8e64a8743799bff52d9293aad18753) | Release tag 2.4.0-RC0 |
| meltano | [f91d2e4f0d93](https://github.com/meltano/meltano/tree/f91d2e4f0d93065d7ca50ff775124382f4db2d0e) | Снимок для анализа; binary identity отдельно |
| singer-postgres | [821f1cfdc094](https://github.com/singer-io/tap-postgres/tree/821f1cfdc0942f3ca839d4cf5abacb18858ac64a) | Снимок для анализа; binary identity отдельно |
| singer-sdk | [ce2e8f2b7fc1](https://github.com/meltano/sdk/tree/ce2e8f2b7fc145fd610965c713794839d137fe1d) | Снимок для анализа; binary identity отдельно |
| spark | [ed00d046951a](https://github.com/apache/spark/tree/ed00d046951a7ecda6429accd3b9c5b2dc792b65) | Release tag v3.5.7 |
| sqoop | [2328971411f5](https://github.com/apache/sqoop/tree/2328971411f57f0cb683dfb79d19d4d19d185dd8) | Release tag release-1.4.7-rc0 |
| estuary-connectors | [262fcee1dab2](https://github.com/estuary/connectors/tree/262fcee1dab22b57a234e247f8b65b93a7bf0032) | Image revision label совпадает |
| airbyte | [156d6d32937c](https://github.com/airbytehq/airbyte/tree/156d6d32937c546b048c010dcb7cfbe52201341e) | Снимок для анализа; binary identity отдельно |
| meltanolabs-tap-postgres | [78332a27922e](https://github.com/MeltanoLabs/tap-postgres/tree/78332a27922e121eef32b78d5c5cdf3141e9eca2) | Снимок для анализа; binary identity отдельно |
| meltanolabs-target-postgres | [0e6eb1f50e26](https://github.com/MeltanoLabs/target-postgres/tree/0e6eb1f50e26aa075622008920424445b867b9e5) | Снимок для анализа; binary identity отдельно |

## Transferia Go: binary SELECT не равен COPY TO

В [storage.go исследованного Go commit](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/postgres/storage.go) `loadTable` выполняет pgx `Query` с binary result formats, затем `NewChangeItemsFetcher` строит ChangeItem-представление строк. Это не PostgreSQL COPY TO, который использует Rust snapshot reader. В назначении Go использует CopyFrom. Разные wire framing, представление строки и стоимость выделений — возможные причины разницы client CPU; этот опыт не изолирует их вклад.

Тот же путь SELECT→ChangeItems и sequence/percentile splitter проверены непосредственно в серверном build tree: `~/arcadia/transfer_manager/go/pkg/providers/postgres/{storage.go,sharding_storage.go,sharding_storage_sequence.go}`. Поиск был ограничен этим конкретным пакетом. `go version -m` показывает go1.26.5, но не раскрывает commit Arcadia binary; совпадение с публичной ревизией не утверждается.

## Estuary materialization: SQL calls не равны round trips

В [materialize-postgres/driver.go](https://github.com/estuary/connectors/blob/262fcee1dab22b57a234e247f8b65b93a7bf0032/materialize-postgres/driver.go) `Load` ставит отдельный INSERT ключа во временную таблицу, затем выполняет общий join; `Store` ставит INSERT/UPDATE строки. Команды собираются в `pgx.Batch` и отправляются через `SendBatch`. Поэтому около двух миллионов SQL calls для миллиона новых строк **не означают два миллиона сетевых round trips**. `delta_updates` меняет необходимость Load; диагностическая серия с этим флагом сохранена отдельно от основного ряда.

## DataX: квантизация времени завершения

В [AbstractScheduler.schedule](https://github.com/alibaba/DataX/blob/80ec23d5c5328eb90ca364d2749e92dfaf44541e/core/src/main/java/com/alibaba/datax/core/job/scheduler/AbstractScheduler.java) default `core.container.job.sleepInterval` равен 10000 мс; scheduler проверяет `SUCCEEDED` между такими ожиданиями. В JDBC diagnostic одна и та же narrow/P4 работа занимала 11.5 или 21.5 с при примерно 14.5 CPU-секундах. Это согласуется с дополнительным циклом проверки завершения, а не с двукратным изменением скорости обработки строк.

Отдельный `datax_poll_probe.py` меняет только этот интервал на 100 мс. Основная серия сохраняет upstream default; диагностические результаты опубликованы отдельно. Из наблюдений не вычитается придуманная константа 10 с. На длинном задании относительное влияние такого ожидания меньше; именно поэтому нужен 10M follow-up.

## Одинаковый JDBC batch, разная геометрия SQL

У DataX используется pgJDBC 42.3.3. Его `PgPreparedStatement.transformQueriesAndParameters` ограничивает переписанный INSERT 128 строками. Batch из 1000 строк превращается в 7×128 + 64 + 32 + 8, то есть **10 INSERT**. Код проверен в официальном [source JAR 42.3.3](https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3-sources.jar).

Spark использует 42.7.13. В [этой версии PgPreparedStatement](https://github.com/pgjdbc/pgjdbc/blob/REL42.7.13/pgjdbc/src/main/java/org/postgresql/jdbc/PgPreparedStatement.java) потолок определяется числом bind-параметров и row ceiling; прежнего ограничения 128 здесь нет. Для наших пяти колонок 1000 строк распадаются на 512 + 256 + 128 + 64 + 32 + 8, то есть **6 INSERT**.

Это согласуется с narrow/P4 наблюдением: около 10011 SQL calls у DataX и 6009 у Spark на миллион строк, включая немного metadata-работы. Число INSERT уменьшается на 40%, но из этого **не следует 40% ускорения доставки**: транзакции, обработка строк, startup и scheduler различаются. Встроенные драйверы конкурентов не заменялись ради унификации, поэтому версия драйвера — часть измеренной конфигурации.

`pg_stat_statements.track_utility=off` на этих кластерах: низкие/нулевые counters для COPY/cursor-путей нельзя трактовать как отсутствие работы PostgreSQL. SQL deltas используются только как ограниченная диагностика конкретных statements; из них не построен рейтинг database CPU или общей стоимости системы.

## Transferia Go: отдельная проверка homogeneous PG→PG

Основная конфигурация явно задаёт `NoHomo=true`: это typed ChangeItem-путь, используемый и для разных типов БД. Для PG→PG в [Provider](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/postgres/provider.go) и [Unmarshaller](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/postgres/unmarshaller.go) есть homogeneous-ветка с другим представлением значения. `go_homo_probe.py` отдельно задаёт `NoHomo=false`, сохраняя binary SELECT, число частей, rename и общие ограничения назначения. Результаты P1/P4, narrow/wide представлены в DIAGNOSTICS.md; основной Go-ряд нельзя выдавать за предел скорости всех его режимов.

## JDBC writer batch 8192: throughput и CPU могут двигаться в разные стороны

Отдельный проверенный опыт сравнивает batch 1000 и 8192 в PG→PG/P4. DataX в обеих диагностических группах использует completion poll 100 мс, чтобы не смешивать размер batch с задержкой завершения. Для 8192 строк старый драйвер DataX всё равно разбивает пакет на INSERT по 128 строк, а современный драйвер Spark может отправить 8192 строк одним переписанным INSERT (40960 параметров для пяти колонок). Это согласуется с наблюдаемыми примерно 7827 против 141 destination SQL calls на миллион строк; служебные запросы входят в эти числа.

У DataX медиана узкого задания выросла с 87,6 до 122,0 тыс. строк/с, но эффективность снизилась с 68,6 до 59,9 тыс. строк/CPU-с. На широких строках — с 59,3 до 65,4 тыс. строк/с при снижении с 31,7 до 22,4 тыс. строк/CPU-с. Ускорение wall time не означает уменьшение вычислительной работы. Изменяются буферизация и частота commit; по этому опыту нельзя приписать весь эффект одному фактору. Для Spark точные основные controls и диагностические медианы приведены в [DIAGNOSTICS.md](DIAGNOSTICS.md). Это две проверенные настройки, а не исчерпывающий поиск оптимального batch.

Для Spark→ClickHouse в URL также сохранён `jdbc_ignore_unsupported_values=true`. [Документация драйвера](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/language-clients/java/jdbc/jdbc.mdx) описывает эту настройку как обработку неподдерживаемых JDBC-возможностей (`SQLFeatureNotSupportedException`); она не является основанием считать пропущенные значения допустимыми. Все пять полей каждой строки проверяются независимо после записи. Путь V1 и его настройки зафиксированы в конфигурации; сравнение V1/V2 не проводилось.

## Airbyte destination: проверка фактического direct-load пути

В runtime 3.0.18 журнал показывает `DirectLoadTableAppendStreamLoader` и `PostgresInsertBuffer`, завершающий вставки по 100000 строк. В БД отсутствуют таблицы схемы `fair21_raw`; у удержанной конечной `fair21.narrow10m` нет `_airbyte_*` колонок. Поэтому этот опыт не следует описывать как старый raw-table → type-and-dedupe pipeline только из-за legacy-названий config options.

В [исследованном PostgresInsertBuffer](https://github.com/airbytehq/airbyte/blob/156d6d32937c546b048c010dcb7cfbe52201341e/airbyte-integrations/connectors/destination-postgres/src/main/kotlin/io/airbyte/integrations/destination/postgres/write/load/PostgresInsertBuffer.kt) records форматируются во временный CSV, затем `copyFromCsv` загружает его через COPY и удаляет файл. Этот source snapshot обозначен как destination 3.0.21, поэтому совпадение имён классов с журналом 3.0.18 не объявляется доказательством полной идентичности исходников и бинарника. [Сохранённые наблюдения](airbyte-destination-evidence.json).

## Flink CDC: большой chunk — большой heap

Первый 10M/P4 запуск с TaskManager process size 8 GiB завершился fatal `OutOfMemoryError: Java heap space`; ожидающий SQL client остановлен вручную после фиксации ошибки. [Доказательство](flink-large-failure.json) сохранено отдельно, а неудачный прогон остаётся в raw results. Повтор с process size 18 GiB и managed fraction 0.1 успешно проверен; общий cgroup по-прежнему 24 GiB. Это исключение явно помечено в большом графике и таблице, а не перенесено на основную серию 1M.

В [коде именно release 3.4.0](https://github.com/apache/flink-cdc/blob/06581fc73b84b1da45fccf93441d96a118d9bfc1/flink-cdc-connect/flink-cdc-source-connectors/flink-cdc-base/src/main/java/org/apache/flink/cdc/connectors/base/source/reader/external/IncrementalSourceScanFetcher.java) `pollWithBuffer` хранит snapshot-часть в `HashMap<Struct, SourceRecord>`, применяет overlapping change records до end watermark и только затем выпускает коллекцию. Четыре части по 2,5 млн строк — существенная heap-нагрузка, несмотря на JDBC fetch size 8192. **Fetch size не ограничивает этот буфер целой части.**

`scan.incremental.snapshot.backfill.skip=true` выбирает `pollWithoutBuffer`. Это отдельная диагностическая настройка с изменёнными гарантиями при concurrent writes; она не включена в основной ряд. Диагностика сохраняет тот же увеличенный heap и P4, меняя только этот флаг, на неизменяемом источнике. Её фактический результат — в [DIAGNOSTICS.md](DIAGNOSTICS.md).

Этот случай также показывает, почему **число частей и число одновременных readers — разные параметры**. Здесь для сопоставимости фиксированы четыре части одной таблицы. У буферизующего Flink CDC четыре крупных chunk увеличивают живой набор объектов, тогда как потоковый COPY reader не обязан удерживать всю свою часть. Большее число мелких chunk при тех же четырёх readers могло бы изменить память и скорость Flink, но такой режим не подменял общий P4 и отдельно здесь не измерялся. Равное разбиение полезно для контроля эксперимента, но не гарантирует оптимальной конфигурации каждого движка.
