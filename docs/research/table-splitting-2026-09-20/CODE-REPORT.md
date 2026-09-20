# Разбиение таблиц: проверенные реализации

Это аудит указанных компонентов на commits из [manifest](SOURCE-MANIFEST.json). «Есть код» не означает успешный прогон или одинаковые гарантии во всех коннекторах. Производительность здесь оценивается аналитически; новые бенчмарки не запускались.

## Transferia Go

**Механизм:** автоматическое / настраиваемое.

PostgreSQL: секции наследования; для одиночного serial/identity-ключа полного snapshot обычной таблицы — percentile_disc, при ошибке min/max; для полностью числового PK без фильтра/view — modulo суммы; остальные поддержанные случаи — hashtext(row(keys)::text). MySQL: существующие PARTITION. Oracle: диапазоны ROWID по dba_extents и общий SCN. MongoDB: отсортированные sample _id задают ranges; смешанные BSON type brackets отклоняются, filter/offset не делятся. Другие источники имеют собственные splitter-реализации.

**Эффективность и ограничения.** Диапазоны используют индекс и учитывают skew через квантили; точный percentile требует дополнительного прохода/сортировки. Hash/modulo обычно не дают index range scan. Настройка параллелизма одна, фактические стратегии разных БД существенно различаются.

Код: [sharding_storage.go:25](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/postgres/sharding_storage.go#L25), [sharding_storage_sequence.go:83](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/postgres/sharding_storage_sequence.go#L83), [storage_sharding.go:13](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/mysql/storage_sharding.go#L13), [sharding_storage.go:36](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/oracle/provider/sharding_storage.go#L36), [sharding_storage.go:148](https://github.com/transferia/transferia/blob/d32b451f3579688a5cebd88032a266565bc96c35/pkg/providers/mongo/sharding_storage.go#L148).

## Apache SeaTunnel

**Механизм:** автоматическое / настраиваемое.

JDBC: FixedChunkSplitter делит заданный/вычисленный диапазон; DynamicChunkSplitter выбирает равномерное числовое разбиение либо запросы границ по данным. Строки: hash, legacy charset/collation-aware и новые RANGE/AUTO. В проверенном MySQL dialect RANGE требует *_bin collation и sample до 256 printable ASCII значений одинаковой длины; это выборочная проверка. AUTO при неподходящих данных выбирает HASH/NONE, а при неподдерживаемом dialect ошибается. CDC — отдельный путь.

**Эффективность и ограничения.** Больше вариантов типов и адаптации, чем простой min/max; строковый диапазон обязан соответствовать collation БД. Автоматический переход к hash может резко изменить стоимость чтения. JDBC batch сам по себе не доказывает общий MVCC snapshot.

Код: [FixedChunkSplitter.java:63](https://github.com/apache/seatunnel/blob/ea3df1667719520c51d40546d04f50912ba24313/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/FixedChunkSplitter.java#L63), [DynamicChunkSplitter.java:60](https://github.com/apache/seatunnel/blob/ea3df1667719520c51d40546d04f50912ba24313/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/DynamicChunkSplitter.java#L60), [CollationBasedSplitter.java:26](https://github.com/apache/seatunnel/blob/ea3df1667719520c51d40546d04f50912ba24313/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/CollationBasedSplitter.java#L26), [ChunkSplitter.java:202](https://github.com/apache/seatunnel/blob/ea3df1667719520c51d40546d04f50912ba24313/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/ChunkSplitter.java#L202), [MysqlDialect.java:243](https://github.com/apache/seatunnel/blob/ea3df1667719520c51d40546d04f50912ba24313/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/dialect/mysql/MysqlDialect.java#L243).

## Alibaba DataX

**Механизм:** настраиваемое.

Общий RDBMS reader: splitPk, MIN/MAX, арифметические диапазоны целого ключа; в общем utility есть строковое разбиение и специальный Oracle-путь. NULL выделяется отдельным заданием. Произвольные querySql можно задать списком; это не автоматический splitter произвольного SELECT.

**Эффективность и ограничения.** Дешёвое планирование, хорошо для плотного индексированного ID; skew и пробелы дают неравные объёмы. Нельзя переносить возможности общего utility на любой reader без проверки его валидации.

Код: [SingleTableSplitUtil.java:30](https://github.com/alibaba/DataX/blob/80ec23d5c5328eb90ca364d2749e92dfaf44541e/plugin-rdbms-util/src/main/java/com/alibaba/datax/plugin/rdbms/reader/util/SingleTableSplitUtil.java#L30), [SingleTableSplitUtil.java:113](https://github.com/alibaba/DataX/blob/80ec23d5c5328eb90ca364d2749e92dfaf44541e/plugin-rdbms-util/src/main/java/com/alibaba/datax/plugin/rdbms/reader/util/SingleTableSplitUtil.java#L113), [ReaderSplitUtil.java:59](https://github.com/alibaba/DataX/blob/80ec23d5c5328eb90ca364d2749e92dfaf44541e/plugin-rdbms-util/src/main/java/com/alibaba/datax/plugin/rdbms/reader/util/ReaderSplitUtil.java#L59).

## Apache NiFi

**Механизм:** настраиваемое.

GenerateTableFetch генерирует отдельные SQL-запросы: страницы по номеру строки/LIMIT-OFFSET либо интервалы значений одной колонки. FlowFiles можно исполнять downstream параллельно. Maximum-value columns задают инкрементальную фильтрацию отдельно.

**Эффективность и ограничения.** Прозрачный SQL и удобное управление задачами; большие OFFSET дороги. Без стабильного ORDER BY и общего снимка страницы изменяемой таблицы могут пересекаться/пропускаться.

Код: [GenerateTableFetch.java:142](https://github.com/apache/nifi/blob/29236d6bee259dc661d95670bfdaadaa75ede9cc/nifi-extension-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/GenerateTableFetch.java#L142), [GenerateTableFetch.java:469](https://github.com/apache/nifi/blob/29236d6bee259dc661d95670bfdaadaa75ede9cc/nifi-extension-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/GenerateTableFetch.java#L469).

## ChunJun / FlinkX

**Механизм:** настраиваемое.

JdbcInputFormat поддерживает splitStrategy=range и mod: диапазон split key либо остаток деления, задания распределяются по parallelism. Есть отдельная логика polling/incremental и конечной границы.

**Эффективность и ограничения.** Range лучше использует индекс; modulo проще распределяет числовые значения, но обычно перечитывает источник. Это не эквивалент DBLog/согласованного CDC snapshot.

Код: [JdbcInputFormat.java:136](https://github.com/DTStack/chunjun/blob/cff5fbffb498a60e0fb700f77073deeebb7ed3e6/chunjun-connectors/chunjun-connector-jdbc-base/src/main/java/com/dtstack/chunjun/connector/jdbc/source/JdbcInputFormat.java#L136), [JdbcInputFormat.java:598](https://github.com/DTStack/chunjun/blob/cff5fbffb498a60e0fb700f77073deeebb7ed3e6/chunjun-connectors/chunjun-connector-jdbc-base/src/main/java/com/dtstack/chunjun/connector/jdbc/source/JdbcInputFormat.java#L598).

## Apache Flink CDC

**Механизм:** параллельный snapshot + журнал.

JdbcSourceChunkSplitter: числовые MIN/MAX и оценка плотности (distribution factor); иначе последовательный поиск следующей границы по chunk key через ограниченный упорядоченный запрос. Snapshot splits распределяются воркерам; чтение журнала между low/high watermark согласует каждый chunk с CDC. MySQL имеет собственный аналог splitter.

**Эффективность и ограничения.** Нет одного длительного snapshot на всю таблицу; устойчивее для долгих backfill. Нужны корректный chunk key, журнал, состояние splits и reconciliation; не все коннекторы имеют одинаковые возможности.

Код: [JdbcSourceChunkSplitter.java:346](https://github.com/apache/flink-cdc/blob/a1ec262c5f1564ba009681832c77e685478f84a8/flink-cdc-connect/flink-cdc-source-connectors/flink-cdc-base/src/main/java/org/apache/flink/cdc/connectors/base/source/assigner/splitter/JdbcSourceChunkSplitter.java#L346), [SnapshotSplitAssigner.java:70](https://github.com/apache/flink-cdc/blob/a1ec262c5f1564ba009681832c77e685478f84a8/flink-cdc-connect/flink-cdc-source-connectors/flink-cdc-base/src/main/java/org/apache/flink/cdc/connectors/base/source/assigner/SnapshotSplitAssigner.java#L70), [SnapshotSplitReader.java:77](https://github.com/apache/flink-cdc/blob/a1ec262c5f1564ba009681832c77e685478f84a8/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/debezium/reader/SnapshotSplitReader.java#L77).

## Apache InLong

**Механизм:** коннекторное.

В sort-слое есть CDC split/assigner-реализации семейства Flink CDC: диапазоны snapshot и далее binlog/CDC. Это коннекторная возможность, а не общее разбиение любого входящего потока.

**Эффективность и ограничения.** Наследует преимущества и ограничения chunk-key/watermark подхода; важно различать версии sort/Flink и конкретный коннектор.

Код: [PostgresChunkSplitter.java:130](https://github.com/apache/inlong/blob/13c437946fcf503aa5d9c5cb7d9e3ae135e1911b/inlong-sort/sort-flink/sort-flink-v1.13/sort-connectors/postgres-cdc/src/main/java/org/apache/inlong/sort/cdc/postgres/source/PostgresChunkSplitter.java#L130).

## Debezium

**Механизм:** последовательные incremental chunks; initial parallelism отдельно.

Incremental snapshot проходит таблицу упорядоченными порциями ключа, ограничивает верхний ключ, открывает/закрывает watermark window и удаляет из буфера snapshot-записи, для которых пришли изменения. Initial snapshot имеет pool задач по таблицам; сам размер incremental chunk не означает параллельное чтение одной таблицы.

**Эффективность и ограничения.** Хорошая интеграция с CDC и ограниченный буфер; не следует приписывать Debezium intra-table parallelism только по snapshot.max.threads. Гарантии зависят от коннектора и режима сигналов/read-only.

Код: [AbstractIncrementalSnapshotChangeEventSource.java:201](https://github.com/debezium/debezium/blob/93cd1a45e7e0344fdb6024014130b2fdbc88e7ef/debezium-connector-common/src/main/java/io/debezium/pipeline/source/snapshot/incremental/AbstractIncrementalSnapshotChangeEventSource.java#L201), [RelationalSnapshotChangeEventSource.java:206](https://github.com/debezium/debezium/blob/93cd1a45e7e0344fdb6024014130b2fdbc88e7ef/debezium-connector-common/src/main/java/io/debezium/relational/RelationalSnapshotChangeEventSource.java#L206).

## PeerDB

**Механизм:** несколько стратегий.

PostgreSQL QRep: NTILE по упорядоченному watermark для примерно равного числа строк; min/max для равных диапазонов значений; CTID block ranges, включая обход leaf partitions/наследования. NULL может выделяться отдельной partition.

**Эффективность и ограничения.** CTID избегает сортировки и случайного индексного чтения; NTILE дороже планировать, но лучше баланс строк. QRep и initial CDC snapshot — разные жизненные циклы; не всякий watermark подходит для точного CDC.

Код: [qrep_partition.go:35](https://github.com/PeerDB-io/peerdb/blob/971d47b06419bed37a804eece2bb784ceaf58549/flow/connectors/postgres/qrep_partition.go#L35), [qrep_partition.go:81](https://github.com/PeerDB-io/peerdb/blob/971d47b06419bed37a804eece2bb784ceaf58549/flow/connectors/postgres/qrep_partition.go#L81), [qrep_partition.go:119](https://github.com/PeerDB-io/peerdb/blob/971d47b06419bed37a804eece2bb784ceaf58549/flow/connectors/postgres/qrep_partition.go#L119).

## pgstream

**Механизм:** физические диапазоны.

PostgreSQL ctidReader экспортирует snapshot на schema-session и читает таблицы через очередь диапазонов heap-страниц, SELECT FROM ONLY ... WHERE ctid BETWEEN .... Максимальная страница определяется SELECT MAX(ctid), а не устаревающим relpages.

**Эффективность и ограничения.** Не требует PK и сохраняет согласованность читателей в живом snapshot. Нужно учитывать leaf relations, TOAST/пустые страницы и невозможность восстановить исчезнувший MVCC snapshot по одному сохранённому CTID. MAX(ctid) может потребовать дополнительного полного прохода: физическое разбиение не гарантирует бесплатное планирование.

Код: [ctid_table_reader.go:23](https://github.com/xataio/pgstream/blob/732afff71df94ad3c2b06f0234b6f0e40626692d/pkg/snapshot/generator/postgres/data/ctid_table_reader.go#L23), [ctid_table_reader.go:134](https://github.com/xataio/pgstream/blob/732afff71df94ad3c2b06f0234b6f0e40626692d/pkg/snapshot/generator/postgres/data/ctid_table_reader.go#L134), [snapshot_tx.go:12](https://github.com/xataio/pgstream/blob/732afff71df94ad3c2b06f0234b6f0e40626692d/pkg/snapshot/generator/postgres/data/snapshot_tx.go#L12).

## Sequin

**Механизм:** keyset backfill.

Читает bounded batches через keyset cursor: сортировочная колонка плюс PK, либо только составной PK. TableReader сохраняет cursor и маркирует backfill watermark. Один cursor-проход сам по себе не является N независимыми диапазонами одной таблицы.

**Эффективность и ограничения.** Эффективный resume по индексу без растущего OFFSET; скорость одного прохода ограничена зависимостью следующей страницы от предыдущей. Конкретные sentinel/типовые границы требуют отдельного аудита, это не готовый образец lossless-контракта.

Код: [keyset_cursor.ex:21](https://github.com/sequinstream/sequin/blob/46ce4e1048437575ce3c40ebb3eb589a4b9e4f27/lib/sequin/runtime/keyset_cursor.ex#L21), [keyset_cursor.ex:94](https://github.com/sequinstream/sequin/blob/46ce4e1048437575ce3c40ebb3eb589a4b9e4f27/lib/sequin/runtime/keyset_cursor.ex#L94), [table_reader.ex:129](https://github.com/sequinstream/sequin/blob/46ce4e1048437575ce3c40ebb3eb589a4b9e4f27/lib/sequin/runtime/table_reader.ex#L129).

## Adiom dsync

**Механизм:** коннекторное / пользовательские границы.

SQL batch принимает PartitionQuery, превращает его строки в partition cursors; запрос чтения параметризуется ими. У MongoDB, DynamoDB и Cosmos собственные планировщики/сегменты, а не SQL MIN/MAX на все источники.

**Эффективность и ограничения.** Полезная абстракция task cursor; корректность SQL-разбиения перекладывается на контракт PartitionQuery. Границы/целостность snapshot надо проверять отдельно для каждого источника.

Код: [conn.go:39](https://github.com/adiom-data/dsync/blob/9c0e12db57fa10bd69220482b4ce151f7620eb30/connectors/sqlbatch/conn.go#L39), [conn.go:219](https://github.com/adiom-data/dsync/blob/9c0e12db57fa10bd69220482b4ce151f7620eb30/connectors/sqlbatch/conn.go#L219), [planner.go:26](https://github.com/adiom-data/dsync/blob/9c0e12db57fa10bd69220482b4ce151f7620eb30/connectors/mongo/planner.go#L26).

## pgcopydb

**Механизм:** ключ / физические страницы.

Крупные PostgreSQL-таблицы делятся по уникальной целочисленной колонке либо CTID; расчёт границ зависит от размера и числа table jobs. Для CTID проверяется применимость, relpages и наличие статистики; возможен отказ от разбиения.

**Эффективность и ограничения.** Хорошая практическая модель для PostgreSQL bulk COPY. Оценки числа страниц/размера неточны; общий snapshot и блокировки DDL критичны. Не всякий table access method поддерживает дешёвый CTID range scan.

Код: [copydb_schema.c:821](https://github.com/dimitri/pgcopydb/blob/d8c1ec51f7104a0c80feaa02bfcd56fa3e35ff5e/src/bin/pgcopydb/copydb_schema.c#L821), [schema.c:1363](https://github.com/dimitri/pgcopydb/blob/d8c1ec51f7104a0c80feaa02bfcd56fa3e35ff5e/src/bin/pgcopydb/schema.c#L1363).

## pgloader

**Механизм:** MySQL integer ranges.

MySQL multiple-readers: выбирает подходящий числовой ключ, вычисляет MIN/MAX, делит значения на rows-per-range и распределяет интервалы между readers. Название rows-per-range здесь не гарантирует равное фактическое число строк.

**Эффективность и ограничения.** Просто и хорошо при плотном ключе; статическое распределение страдает от skew/дыр. Нельзя переносить этот механизм на все поддерживаемые источники pgloader.

Код: [mysql.lisp:10](https://github.com/dimitri/pgloader/blob/231ab86778ca5ffd7de40878714760c8b4860cdf/src/sources/mysql/mysql.lisp#L10), [mysql.lisp:46](https://github.com/dimitri/pgloader/blob/231ab86778ca5ffd7de40878714760c8b4860cdf/src/sources/mysql/mysql.lisp#L46).

## Ora2Pg

**Механизм:** секции / modulo.

Oracle exports могут идти по partitions/subpartitions; параллельные копии одной таблицы используют ABS(MOD(key, oracle_copies)) с автоматически выбранным либо DEFINED_PK ключом. JOBS на стороне загрузки — отдельный уровень.

**Эффективность и ограничения.** Секции дают pruning; modulo может требовать несколько полных сканов. ORACLE_COPIES, число таблиц и число writers нельзя смешивать в одну метрику параллелизма.

Код: [Ora2Pg.pm:11257](https://github.com/darold/ora2pg/blob/cc2c434f1185af72891122abcb219d07c5aa7e35/lib/Ora2Pg.pm#L11257), [Ora2Pg.pm:984](https://github.com/darold/ora2pg/blob/cc2c434f1185af72891122abcb219d07c5aa7e35/lib/Ora2Pg.pm#L984).

## mydumper / myloader

**Механизм:** адаптивные диапазоны.

Отдельные integer, string и partition chunkers. Диапазоны могут дробиться и передаваться свободным threads; MIN:START:MAX для rows задаёт адаптацию размера по времени запроса. В текущем checkout есть отдельное планирование строковых PK.

**Эффективность и ограничения.** Снижает straggler tail и не ограничивается числовым PK. Много граничных случаев: составные ключи, collation, экстремальные значения; consistency определяется также locks/transaction snapshot, а не splitter.

Код: [mydumper_integer_chunks.c:349](https://github.com/mydumper/mydumper/blob/c01ca9dd4bce063b333ece6b6176a337a1513fb7/src/mydumper/mydumper_integer_chunks.c#L349), [mydumper_string_chunks.c:307](https://github.com/mydumper/mydumper/blob/c01ca9dd4bce063b333ece6b6176a337a1513fb7/src/mydumper/mydumper_string_chunks.c#L307), [mydumper_arguments.c:396](https://github.com/mydumper/mydumper/blob/c01ca9dd4bce063b333ece6b6176a337a1513fb7/src/mydumper/mydumper_arguments.c#L396).

## MySQL Shell dump/copy utilities

**Механизм:** адаптивные индексные диапазоны.

Chunking по выбранному PK/unique index, в том числе составному; bytesPerChunk переводится в ожидаемое число строк по средней ширине, это не жёсткий предел размера. NULL nullable unique-index добавляются в первый chunk. Числовой путь умеет постоянный шаг или адаптивные EXPLAIN-пробы, последующие столбцы уточняют диапазоны. Работы читаются параллельно; chunks файлов не следует путать с input ranges.

**Эффективность и ограничения.** Оценка в байтах лучше для широких строк, чем фиксированное число строк. EXPLAIN дешевле точного COUNT, но неточен; сложнее planner и работа с корреляцией составных ключей.

Код: [dumper.cc:202](https://github.com/mysql/mysql-shell/blob/977806ff9412abf13da82a1d6ed49e2b909bc121/modules/util/dump/dumper.cc#L202), [dumper.cc:1909](https://github.com/mysql/mysql-shell/blob/977806ff9412abf13da82a1d6ed49e2b909bc121/modules/util/dump/dumper.cc#L1909), [ddl_dumper_options.cc:48](https://github.com/mysql/mysql-shell/blob/977806ff9412abf13da82a1d6ed49e2b909bc121/modules/util/dump/ddl_dumper_options.cc#L48).

## TiDB Data Migration / Dumpling

**Механизм:** MIN/MAX / native regions.

DM использует Dumpling: MySQL-путь выбирает split field и MIN/MAX; TiDB-путь использует TABLESAMPLE/границы регионов и handles. При неподходящем ключе/малом объёме splitter не применяется.

**Эффективность и ограничения.** Native ranges используют физическую организацию распределённой БД; min/max сохраняет проблему skew. TiDB snapshot timestamp/consistency не переносится автоматически на MySQL.

Код: [dump.go:841](https://github.com/pingcap/tidb/blob/b44616b44d0ffed028b76b6da9e95418105bae87/dumpling/export/dump.go#L841), [dump.go:1107](https://github.com/pingcap/tidb/blob/b44616b44d0ffed028b76b6da9e95418105bae87/dumpling/export/dump.go#L1107).

## Apache Spark JDBC

**Механизм:** равные интервалы / ручные predicates.

partitionColumn + lowerBound/upperBound + numPartitions создают диапазоны чисел/date/timestamp. Первая часть включает NULL, крайние части открыты: bounds задают шаг, а не фильтр всего набора. Можно передать собственные SQL predicates через JDBC API.

**Эффективность и ограничения.** Простая параллельная экстракция с ограничением соединений; диапазоны равны по значениям, не строкам. Отдельные JDBC соединения сами по себе не имеют общего source snapshot.

Код: [JDBCRelation.scala:71](https://github.com/apache/spark/blob/e151681a240a419f3921ecb337c6090572f0fbe9/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala#L71), [JDBCRelation.scala:164](https://github.com/apache/spark/blob/e151681a240a419f3921ecb337c6090572f0fbe9/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala#L164).

## Apache Flink JDBC

**Механизм:** числовые/временные интервалы / параметры.

SQL/Table API scan.partition.* задаёт колонку, число частей и bounds; BETWEEN-предикаты ограничивают результат и не включают NULL. JdbcNumericBetweenParametersProvider, произвольные parameter providers и sliding timing provider также есть в legacy Java API; возможности этого API нельзя автоматически приписывать Table API.

**Эффективность и ограничения.** Удобно для явно ограниченного batch; неправильные bounds могут исключить строки. JDBC batch partitioning не равно Flink CDC watermark snapshot.

Код: [JdbcNumericBetweenParametersProvider.java:46](https://github.com/apache/flink-connector-jdbc/blob/73a3f34f455d43aa252106b109ea02d27fc3fadb/flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/split/JdbcNumericBetweenParametersProvider.java#L46), [JdbcDynamicTableSource.java:164](https://github.com/apache/flink-connector-jdbc/blob/73a3f34f455d43aa252106b109ea02d27fc3fadb/flink-connector-jdbc-core/src/main/java/org/apache/flink/connector/jdbc/core/table/source/JdbcDynamicTableSource.java#L164).

## Apache Beam JdbcIO

**Механизм:** диапазоны / расширяемый helper.

readWithPartitions вычисляет или принимает bounds и numPartitions; стандартные helpers — Long и DateTime. Auto bounds получают через MIN/MAX, COUNT нужен при вычислении числа partitions. JdbcReadWithPartitionsHelper позволяет задать поддержку других типов.

**Эффективность и ограничения.** Переносимо между runners, отделяет планирование от чтения. Generic SplittableDoFn не означает, что именно JdbcIO умеет динамически разделить уже выполняемый запрос. В проверенных ranges нет NULL-ветки; Long helper вычисляет upperBound+1, что требует проверки крайнего overflow. Это статические наблюдения, не runtime воспроизведение.

Код: [JdbcIO.java:218](https://github.com/apache/beam/blob/2448f5ac7d6a3bcbc61a7acd6edeeb400829ced1/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java#L218), [JdbcReadWithPartitionsHelper.java:31](https://github.com/apache/beam/blob/2448f5ac7d6a3bcbc61a7acd6edeeb400829ced1/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcReadWithPartitionsHelper.java#L31), [JdbcUtil.java:476](https://github.com/apache/beam/blob/2448f5ac7d6a3bcbc61a7acd6edeeb400829ced1/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcUtil.java#L476).

## Daft

**Механизм:** MIN/MAX / percentile.

SQLScanOperator для numeric/temporal partition_col делает COUNT(*), в том числе при заданном num_partitions. Границы — min-max либо PERCENTILE_DISC; при ошибке percentile переходит к min-max. Если все границы одинаковы, читает одним scan.

**Эффективность и ограничения.** COUNT и percentile добавляют работу до чтения. Multi-range >=/< predicates не включают NULL; min-max integer arithmetic использует float, поэтому большие integer требуют проверки точности. Это ограничения увиденного кода, без runtime repro. Равные quantiles не гарантируют равные байты; общий snapshot отдельно не доказан.

Код: [sql_scan.py:41](https://github.com/Eventual-Inc/Daft/blob/1feced9b5af78586da19dc10d32c5bc0c25855e0/daft/sql/sql_scan.py#L41), [sql_scan.py:102](https://github.com/Eventual-Inc/Daft/blob/1feced9b5af78586da19dc10d32c5bc0c25855e0/daft/sql/sql_scan.py#L102), [sql_scan.py:183](https://github.com/Eventual-Inc/Daft/blob/1feced9b5af78586da19dc10d32c5bc0c25855e0/daft/sql/sql_scan.py#L183).

## Ray Data

**Механизм:** hash/modulo.

SQLDatasource создаёт запросы MOD(ABS(hash(shard_keys)), parallelism)=task_id; для нескольких полей использует CONCAT. Проверяет поддержку выражений, иначе один read task.

**Эффективность и ограничения.** Балансирует распределение ключей, но без функционального индекса обычно делает несколько полных сканов. Это маршрутизация, а не доказательство идентичности ключей; NULL/collisions в маршрутизации нужно рассматривать отдельно.

Код: [sql_datasource.py:114](https://github.com/ray-project/ray/blob/674900c91faad20ed39074a754505186c3a02f19/python/ray/data/_internal/datasource/sql_datasource.py#L114), [sql_datasource.py:202](https://github.com/ray-project/ray/blob/674900c91faad20ed39074a754505186c3a02f19/python/ray/data/_internal/datasource/sql_datasource.py#L202).

## Dask DataFrame SQL

**Механизм:** диапазоны index_col.

Пользовательские divisions либо равномерные numeric/time boundaries из MIN/MAX; число partitions явно или по оценке bytes_per_chunk из первых строк и COUNT. Последний интервал включает верхнюю границу.

**Эффективность и ограничения.** Поддерживает ручные skew-aware границы; выборка первых строк может неверно оценить среднюю ширину. SQL NULL и общий snapshot требуют отдельного решения.

Код: [sql.py:17](https://github.com/dask/dask/blob/9dc535daa30d7d36d63d4a003b54342fbc7032db/dask/dataframe/io/sql.py#L17), [sql.py:172](https://github.com/dask/dask/blob/9dc535daa30d7d36d63d4a003b54342fbc7032db/dask/dataframe/io/sql.py#L172).

## Polars через ConnectorX

**Механизм:** делегированные MIN/MAX ranges.

read_database_uri с engine=connectorx передаёт partition_on/partition_range/partition_num либо список SQL. Сам ConnectorX вычисляет диапазоны и запускает независимые запросы. ADBC — другой backend, его возможности не следует приравнивать к ConnectorX.

**Эффективность и ограничения.** Низкий overhead загрузки Arrow и явное управление параллелизмом; range skew и MVCC остаются. Это функция чтения, не полноценный CDC/checkpoint protocol.

Код: [partition.rs:76](https://github.com/sfu-db/connector-x/blob/6bd1ec90a27dc67abdf403af645993f4abad0295/connectorx/src/partition.rs#L76), [sql.rs:354](https://github.com/sfu-db/connector-x/blob/6bd1ec90a27dc67abdf403af645993f4abad0295/connectorx/src/sql.rs#L354).

## DuckDB PostgreSQL extension

**Механизм:** CTID + общий snapshot.

Postgres scanner выдаёт page-range задачи, ограничивает threads, экспортирует pg_export_snapshot и подключает readers к нему; бинарный COPY и CTID scan связаны ограничениями реализации.

**Эффективность и ограничения.** Особенно полезный образец для быстрой PostgreSQL full-table экстракции. Возможности зависят от relation/protocol; generic DuckDB file row groups — другой механизм.

Код: [postgres_scanner.cpp:88](https://github.com/duckdb/postgres_scanner/blob/d57412f6f442819137ad0ed8524cc115fa11d48a/src/postgres_scanner.cpp#L88), [postgres_scanner.cpp:106](https://github.com/duckdb/postgres_scanner/blob/d57412f6f442819137ad0ed8524cc115fa11d48a/src/postgres_scanner.cpp#L106), [postgres_scanner.cpp:256](https://github.com/duckdb/postgres_scanner/blob/d57412f6f442819137ad0ed8524cc115fa11d48a/src/postgres_scanner.cpp#L256).

## RisingWave

**Механизм:** параллельный CDC backfill.

В текущем коде PostgreSQL external reader есть even/uneven snapshot splits по выбранной колонке PK; ParallelizedCdcBackfillExecutor распределяет splits, хранит прогресс и согласует snapshot с upstream CDC.

**Эффективность и ограничения.** Встроенная интеграция с barriers/stateful engine; это существенно больше обычного SQL splitter. Наличие executor в исходниках не гарантирует доступность во всех редакциях/конфигурациях.

Код: [postgres.rs:187](https://github.com/risingwavelabs/risingwave/blob/ee8b82b3a8bb9cce2160e062670564b8af70600f/src/connector/src/source/cdc/external/postgres.rs#L187), [cdc_backill_v2.rs:48](https://github.com/risingwavelabs/risingwave/blob/ee8b82b3a8bb9cce2160e062670564b8af70600f/src/stream/src/executor/backfill/cdc/cdc_backill_v2.rs#L48).

## Apache Gobblin

**Механизм:** watermark ranges; Salesforce histogram/PK chunking.

Extractor Partitioner делит watermark interval в work units. Salesforce-source отдельно строит histogram buckets и может запросить серверный PKChunking через Bulk API.

**Эффективность и ограничения.** Histogram учитывает плотность, server-side chunking использует возможности источника. У timestamp-watermark свои late-arrival/изменяемый-ключ ограничения; не заменяет CDC для DELETE.

Код: [Partitioner.java:50](https://github.com/apache/gobblin/blob/cb6650da4742252b3e18315338596e4ca63ed802/gobblin-core/src/main/java/org/apache/gobblin/source/extractor/partition/Partitioner.java#L50), [SalesforceHistogramService.java:57](https://github.com/apache/gobblin/blob/cb6650da4742252b3e18315338596e4ca63ed802/gobblin-salesforce/src/main/java/org/apache/gobblin/salesforce/SalesforceHistogramService.java#L57), [SalesforceExtractor.java:721](https://github.com/apache/gobblin/blob/cb6650da4742252b3e18315338596e4ca63ed802/gobblin-salesforce/src/main/java/org/apache/gobblin/salesforce/SalesforceExtractor.java#L721).

## Apache Apex / Malhar (архив)

**Механизм:** статические ranges + poller.

JdbcPollInputOperator создаёт непересекающиеся ranges для имеющихся строк и дополнительную polling partition для новых значений ключа.

**Эффективность и ограничения.** Полезный исторический паттерн bootstrap+append; предполагает возрастающий ключ и не является журналовым CDC для произвольных updates/deletes.

Код: [AbstractJdbcPollInputOperator.java:76](https://github.com/apache/apex-malhar/blob/0f016c82e4e1363b3f7de4ae28138bc7ba896d37/library/src/main/java/org/apache/apex/malhar/lib/db/jdbc/AbstractJdbcPollInputOperator.java#L76), [AbstractJdbcPollInputOperator.java:465](https://github.com/apache/apex-malhar/blob/0f016c82e4e1363b3f7de4ae28138bc7ba896d37/library/src/main/java/org/apache/apex/malhar/lib/db/jdbc/AbstractJdbcPollInputOperator.java#L465).

## Apache Sqoop (архив)

**Механизм:** типизированные JDBC splitters.

DataDrivenDBInputFormat и реализации Integer/BigDecimal/Float/Text/Date splitters формируют диапазоны split-by по bounds/boundary-query; Oracle имеет отдельный путь. Mapper count задаёт плановый параллелизм.

**Эффективность и ограничения.** Исторический источник многих современных подходов. Text/collation, NULL и типовая арифметика требуют внимания; Hadoop startup невыгоден для малых таблиц.

Код: [DataDrivenDBInputFormat.java:140](https://github.com/apache/sqoop/blob/f8beae32a067d72bf9ed6e903b041ad347ca5491/src/java/org/apache/sqoop/mapreduce/db/DataDrivenDBInputFormat.java#L140), [TextSplitter.java:37](https://github.com/apache/sqoop/blob/f8beae32a067d72bf9ed6e903b041ad347ca5491/src/java/org/apache/sqoop/mapreduce/db/TextSplitter.java#L37), [OracleDataDrivenDBInputFormat.java:33](https://github.com/apache/sqoop/blob/f8beae32a067d72bf9ed6e903b041ad347ca5491/src/java/org/apache/sqoop/mapreduce/db/OracleDataDrivenDBInputFormat.java#L33).

## Sling OSS vs CLI Pro/Platform

**Механизм:** граница поставок.

Открытый ProcessChunks знает chunk_size/chunk_count/chunk_expr, но ChunkByColumnRange/ChunkByCount/ChunkByExpression в публичном дереве возвращают ошибку «use official release». Реализованное автоматическое chunking нельзя засчитать OSS-сборке только по этим настройкам. Возможности Pro описаны отдельно в коммерческой матрице.

**Эффективность и ограничения.** Важный пример различия интерфейса и реализации; ручные SQL streams остаются другим механизмом, ответственность за покрытие диапазонов у автора конфигурации.

Код: [schemata.go:59](https://github.com/slingdata-io/sling-cli/blob/9db0426f81e45f8c8cf841e2883f50b38a3fb740/core/dbio/database/schemata.go#L59), [replication.go:688](https://github.com/slingdata-io/sling-cli/blob/9db0426f81e45f8c8cf841e2883f50b38a3fb740/core/sling/replication.go#L688).

## Estuary Flow: публичный PostgreSQL connector (BSL)

**Механизм:** keyset / keyless CTID chunks.

ScanTableChunk читает bounded ordered key chunks и хранит resume key; для keyless таблиц есть bounded CTID ranges. SQL capture согласует backfill с журналом. В checkout действует BSL по умолчанию; разрешение Apache/MIT требует письменного согласия. Это source-available, не OSS.

**Эффективность и ограничения.** Normal keyed backfill и keyless physical mode имеют разные гарантии: документация прямо ограничивает correctness keyless режима. Не использовать его как lossless-default для изменяемой таблицы.

Код: [backfill.go:21](https://github.com/estuary/connectors/blob/262fcee1dab22b57a234e247f8b65b93a7bf0032/source-postgres/backfill.go#L21), [backfill.go:349](https://github.com/estuary/connectors/blob/262fcee1dab22b57a234e247f8b65b93a7bf0032/source-postgres/backfill.go#L349).
