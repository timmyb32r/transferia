# Дополнительный аудит исходников: границы source-read, batch и native partitions

Дата аудита: 2026-09-20. Это дополнение к [основным 32 разборам](CODE-REPORT.md). Результаты относятся к указанным файлам и commit, а не ко всем версиям/плагинам продукта. Код конкурентов не запускался; оценки эффективности — аналитические. «Не найден в этом пути» не означает доказанного отсутствия во всём продукте.

Важные дополнительные реализации: **Feldera/ETL и pg_flo — CTID**, **Hazelcast — пользовательские SQL parts**, **TiCDC — region/write-weight spans**. dlt/Hudi умеют делегировать разбиение другому engine. Остальные записи объясняют, почему одноимённые batch/partition настройки нельзя смешивать с parallel source extraction.

## 1. dlt: SQLDatabase и backend ConnectorX

Обычный SQLAlchemy backend выполняет один SELECT и режет уже полученный CursorResult на batch по chunk_size. Это ограничение памяти/размера выдачи, а не несколько независимых range-reader. Альтернативный ConnectorXTableLoader передаёт backend_kwargs в cx.read_sql; следовательно, параметры разбиения ConnectorX могут передаваться этому движку. Собственный универсальный SQL range planner в просмотренном TableLoader не обнаружен.

**Эффективность и границы:** Один курсор — простое потоковое чтение без N повторных запросов, но без intra-table read parallelism. Через ConnectorX можно получить его parallel ranges; применимость зависит от ConnectorX, типа ключа и настроек. Нельзя считать chunk_size числом SQL-партиций.

**Проверенные места:**

- [dlt/dlt/sources/sql_database/helpers.py:301](https://github.com/dlt-hub/dlt/blob/3ec3cfcaa6312bf2136f6750494d7cc3075ebf37/dlt/sources/sql_database/helpers.py#L301) (commit `3ec3cfcaa631`).
- [dlt/dlt/sources/sql_database/helpers.py:394](https://github.com/dlt-hub/dlt/blob/3ec3cfcaa6312bf2136f6750494d7cc3075ebf37/dlt/sources/sql_database/helpers.py#L394) (commit `3ec3cfcaa631`).

## 2. Embulk JDBC

Базовый AbstractJdbcInputPlugin при transaction передаёт control.run(..., schema, 1): один input task. Incremental-позиция/последний ключ и fetchSize не превращают этот task в несколько range scans. Вывод относится к данному JDBC plugin, а не к любому пользовательскому Embulk input.

**Эффективность и границы:** Невысокая нагрузка на источник и простой курсор. Параллелизм output/обработки не устраняет предел одного source task; чтобы поделить одну таблицу, нужен другой plugin или явно разнесённые SQL-задания.

**Проверенные места:**

- [embulk-jdbc/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/AbstractJdbcInputPlugin.java:230](https://github.com/embulk/embulk-input-jdbc/blob/a23b50ef3c74b03a2f4186a4ce0fe00e8ab1b4c9/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/AbstractJdbcInputPlugin.java#L230) (commit `a23b50ef3c74`).

## 3. Apache Hudi Streamer JDBC

JdbcSource принимает hoodie.streamer.jdbc.extra.options.* и передаёт опции Spark DataFrameReader. В том числе документирует lowerBound/upperBound. Разбиение JDBC-чтения делегируется Spark; это не самостоятельный Hudi splitter. Разбиение собственных Hudi snapshot/file groups — отдельный слой.

**Эффективность и границы:** Переиспользует зрелый Spark JDBC parallel read. Нужны корректные partitionColumn/numPartitions/bounds и управление snapshot на стороне источника; равные интервалы значений наследуют перекос Spark. Не путать с Hudi output partition path.

**Проверенные места:**

- [hudi/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/JdbcSource.java:131](https://github.com/apache/hudi/blob/fc3c08a0b22f4b58f8b2ba92b2fc725a482535bc/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/JdbcSource.java#L131) (commit `fc3c08a0b22f`).
- [hudi/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/JdbcSource.java:155](https://github.com/apache/hudi/blob/fc3c08a0b22f4b58f8b2ba92b2fc725a482535bc/hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/JdbcSource.java#L155) (commit `fc3c08a0b22f`).

## 4. SymmetricDS initial load

SelectFromTableSource строит initial-load SQL, допускающий override/WHERE, и читает queryForCursor. В DataExtractorService одна reload-выборка сознательно разбивается MultiBatchStagingWriter на N outgoing batches. Это подтверждённое разбиение транспортной выдачи; автоматически вычисленных непересекающихся диапазонов source-table в этом пути нет.

**Эффективность и границы:** Пакеты помогают доставке/retry и ограничению размера сообщений. Они не доказывают parallel source scan. Явные initial-load predicates дают пользовательский отбор, но их полноту и непересечение нельзя приписать автоматическому планировщику. OSS core: AGPLv3 по README; коммерческие расширения оцениваются отдельно.

**Проверенные места:**

- [symmetricds/symmetric-core/src/main/java/org/jumpmind/symmetric/extract/SelectFromTableSource.java:316](https://github.com/JumpMind/symmetric-ds/blob/e02b4782f8398f0ddacbd497e74d9aaf190a977b/symmetric-core/src/main/java/org/jumpmind/symmetric/extract/SelectFromTableSource.java#L316) (commit `e02b4782f839`).
- [symmetricds/symmetric-core/src/main/java/org/jumpmind/symmetric/service/impl/DataExtractorService.java:2041](https://github.com/JumpMind/symmetric-ds/blob/e02b4782f8398f0ddacbd497e74d9aaf190a977b/symmetric-core/src/main/java/org/jumpmind/symmetric/service/impl/DataExtractorService.java#L2041) (commit `e02b4782f839`).

## 5. Apache Hop: TableInput и ModPartitioner

TableInput исполняет авторский SQL (включая параметры) через openQuery. ModPartitioner распределяет уже прочитанные строки по Math.abs(value) % nrPartitions, используя значение либо hash. Это параллелизм pipeline после SQL-чтения. Пользователь может задать разные WHERE для разных SQL-заданий; автоматического вычисления границ в рассмотренном TableInput нет.

**Эффективность и границы:** Mod/hash после чтения полезен для downstream CPU, но не ускоряет исходный SELECT. Ручные SQL ranges гибки, зато snapshot и отсутствие пересечений обязан обеспечить автор pipeline.

**Проверенные места:**

- [hop/plugins/transforms/tableinput/src/main/java/org/apache/hop/pipeline/transforms/tableinput/TableInput.java:246](https://github.com/apache/hop/blob/40d77549c8f460320def782cc0f7d41433e1eeec/plugins/transforms/tableinput/src/main/java/org/apache/hop/pipeline/transforms/tableinput/TableInput.java#L246) (commit `40d77549c8f4`).
- [hop/engine/src/main/java/org/apache/hop/pipeline/ModPartitioner.java:107](https://github.com/apache/hop/blob/40d77549c8f460320def782cc0f7d41433e1eeec/engine/src/main/java/org/apache/hop/pipeline/ModPartitioner.java#L107) (commit `40d77549c8f4`).

## 6. KNIME Database Reader

Проверенный DefaultDBReader выполняет SQLQuery одним Statement.executeQuery и обрабатывает ResultSet в локальной транзакции. Здесь нет автоматически порождённых range-запросов. Это не исключает построения нескольких SQL-query узлов/циклов пользователем или возможностей других KNIME extensions.

**Эффективность и границы:** Простое семантически явное чтение одного запроса; распараллеливание workflow само по себе не означает partitioned extraction. Для ручных диапазонов нужны стабильные границы и общий snapshot. Модуль knime-database распространяется под GPLv3 с additional permissions, не под лицензией всего коммерческого продукта.

**Проверенные места:**

- [knime/org.knime.database/src/org/knime/database/agent/reader/impl/DefaultDBReader.java:154](https://github.com/knime-oss/knime-database/blob/43942fd089a7ff2be8ff23c3b586cc02ff1a85ab/org.knime.database/src/org/knime/database/agent/reader/impl/DefaultDBReader.java#L154) (commit `43942fd089a7`).
- [knime/org.knime.database/LICENSE.txt:2](https://github.com/knime-oss/knime-database/blob/43942fd089a7ff2be8ff23c3b586cc02ff1a85ab/org.knime.database/LICENSE.txt#L2) (commit `43942fd089a7`).

## 7. Logstash JDBC integration

Auto paging использует Sequel each_page; explicit paging последовательно подставляет offset/size и прибавляет offset на jdbc_page_size, пока страница полная. Это последовательная пагинация одного input, а не P конкурентных source ranges. Пользовательский SQL/несколько inputs могут реализовать своё разбиение.

**Эффективность и границы:** Память ограничивается страницей. OFFSET на больших таблицах может многократно перечитывать префиксы; необходим детерминированный ORDER BY, а при изменениях между запросами возможны сдвиги страниц. Apache-2.0 лицензия проверена для integration-jdbc, а не автоматически для всех компонентов Logstash.

**Проверенные места:**

- [logstash-jdbc/lib/logstash/plugin_mixins/jdbc/statement_handler.rb:86](https://github.com/logstash-plugins/logstash-integration-jdbc/blob/f7586e67912852d5c5a6ec0f4db5f4a4f30e7806/lib/logstash/plugin_mixins/jdbc/statement_handler.rb#L86) (commit `f7586e679128`).
- [logstash-jdbc/lib/logstash/plugin_mixins/jdbc/statement_handler.rb:121](https://github.com/logstash-plugins/logstash-integration-jdbc/blob/f7586e67912852d5c5a6ec0f4db5f4a4f30e7806/lib/logstash/plugin_mixins/jdbc/statement_handler.rb#L121) (commit `f7586e679128`).

## 8. PostgreSQL native logical replication: tablesync

Tablesync worker делает initial COPY конкретной relation и затем догоняет WAL до согласованного состояния. copy_table создаёт COPY table либо COPY(SELECT...) для фильтров/особых relation. Единица рассмотренного начального sync — таблица; несколько tablesync workers не доказывают разрезание одной relation по CTID.

**Эффективность и границы:** Нативный COPY и согласование с logical WAL дают эффективную встроенную доставку. Одна огромная таблица остаётся отдельной единицей initial sync в этом коде; для intra-table scan нужны pgcopydb/CTID readers или иное средство. Указана скачанная HEAD-ревизия, не обещание поведения всех выпущенных версий PostgreSQL.

**Проверенные места:**

- [postgres/src/backend/replication/logical/tablesync.c:1075](https://github.com/postgres/postgres/blob/9e17d25e79d4756be08b4a5521b4b58450217137/src/backend/replication/logical/tablesync.c#L1075) (commit `9e17d25e79d4`).
- [postgres/src/backend/replication/logical/tablesync.c:34](https://github.com/postgres/postgres/blob/9e17d25e79d4756be08b4a5521b4b58450217137/src/backend/replication/logical/tablesync.c#L34) (commit `9e17d25e79d4`).

## 9. pglogical initial copy

copy_table_data копирует одну таблицу по COPY TO/COPY FROM; copy_tables_data обходит список таблиц. SQL может учитывать replication sets/фильтры. В исследованном пути единицей работы является table copy, а не автоматически рассчитанный набор диапазонов одной таблицы.

**Эффективность и границы:** Хорошо соответствует PostgreSQL logical replication и фильтрации. Большой table copy не становится параллельным только из-за нескольких таблиц/репликационных workers; стоимость длинного snapshot и CDC catch-up остаётся.

**Проверенные места:**

- [pglogical/pglogical_sync.c:522](https://github.com/2ndQuadrant/pglogical/blob/9a0e182745885ad0152ea387988c95a483396a81/pglogical_sync.c#L522) (commit `9a0e18274588`).
- [pglogical/pglogical_sync.c:743](https://github.com/2ndQuadrant/pglogical/blob/9a0e182745885ad0152ea387988c95a483396a81/pglogical_sync.c#L743) (commit `9a0e18274588`).

## 10. pgEdge Spock initial copy

Spock также имеет copy_table_data: COPY одной relation с replication-set/row filters; источник удерживает REPEATABLE READ snapshot для COPY. Наблюдаемые изменения в sync/slot протоколе не являются новым внутри-табличным range planner.

**Эффективность и границы:** Нативный COPY плюс строгий handoff к репликации. В просмотренном sync пути нет автоматических CTID/PK поддиапазонов; не переносим на Spock возможности отдельного pgcopydb или pgEdge Cloud без доказательства.

**Проверенные места:**

- [spock/src/spock_sync.c:1046](https://github.com/pgEdge/spock/blob/69d584ae77652c8d506bc89bec12d3c402095b58/src/spock_sync.c#L1046) (commit `69d584ae7765`).
- [spock/src/spock_sync.c:854](https://github.com/pgEdge/spock/blob/69d584ae77652c8d506bc89bec12d3c402095b58/src/spock_sync.c#L854) (commit `69d584ae7765`).

## 11. Hazelcast Jet JDBC source

Parallel JDBC API принимает resultSetFn(connection, totalParallelism, index). Пользователь сам пишет SQL каждой части; встроенный пример — MOD(id, parallelism)=index. Упрощённая перегрузка jdbc(String,String,...) прямо описана как single-worker/single-query.

**Эффективность и границы:** Гибкость: можно реализовать range/hash/нативные партиции. Это интерфейс пользовательского splitter, а не авто-planner. Javadoc прямо предупреждает о пропусках/дубликатах при конкурентных изменениях и полном повторном чтении после restart; MOD без функционального индекса может потребовать P full scans. Проверенный Sources.java Apache-2.0; весь репозиторий mixed Apache/Hazelcast Community License.

**Проверенные места:**

- [hazelcast/hazelcast/src/main/java/com/hazelcast/jet/pipeline/Sources.java:1521](https://github.com/hazelcast/hazelcast/blob/b823b0b2e294423b946a5086d088e7fc026784a8/hazelcast/src/main/java/com/hazelcast/jet/pipeline/Sources.java#L1521) (commit `b823b0b2e294`).
- [hazelcast/hazelcast/src/main/java/com/hazelcast/jet/pipeline/Sources.java:1530](https://github.com/hazelcast/hazelcast/blob/b823b0b2e294423b946a5086d088e7fc026784a8/hazelcast/src/main/java/com/hazelcast/jet/pipeline/Sources.java#L1530) (commit `b823b0b2e294`).
- [hazelcast/LICENSE:5](https://github.com/hazelcast/hazelcast/blob/b823b0b2e294423b946a5086d088e7fc026784a8/LICENSE#L5) (commit `b823b0b2e294`).

## 12. Feldera PostgreSQL CDC → feldera/etl

CDC adapter включает max_copy_connections_per_table и делегирует initial copy зафиксированному fork feldera/etl. Там partitioned parent раскрывается до leaf relations; реальные pg_relation_size/block_size дают блоки, статистика строк — только вес распределения. Планируется несколько CTID ranges на worker, диапазоны помещаются в общую очередь и большие запускаются раньше. Workers импортируют один exported snapshot; крайние ranges открыты наружу, чтобы оценка размера не отрезала видимые tuple.

**Эффективность и границы:** Сильный образец physical splitter: не нужен PK для самого COPY, число work items отделено от соединений, динамическая очередь сглаживает хвост. CTID — адрес версии tuple, не identity для CDC или произвольного restart. Долгий snapshot и WAL всё равно требуют контроля. Статистика весов не гарантирует равное реальное время; проверенный код не является результатом benchmark. Feldera OSS MIT, ETL dependency Apache-2.0; enterprise файлы отдельно.

**Проверенные места:**

- [feldera/crates/adapters/src/integrated/postgres/cdc_input.rs:646](https://github.com/feldera/feldera/blob/b4a369ea86e92b330cab1845ef63ca0f69bce839/crates/adapters/src/integrated/postgres/cdc_input.rs#L646) (commit `b4a369ea86e9`).
- [feldera/Cargo.toml:399](https://github.com/feldera/feldera/blob/b4a369ea86e92b330cab1845ef63ca0f69bce839/Cargo.toml#L399) (commit `b4a369ea86e9`).
- [feldera-etl/crates/etl/src/replication/table_sync/copy.rs:285](https://github.com/feldera/etl/blob/248fa4077c5b0f3e7bc2918a9709be2a03efea73/crates/etl/src/replication/table_sync/copy.rs#L285) (commit `248fa4077c5b`).
- [feldera-etl/crates/etl/src/replication/table_sync/copy.rs:586](https://github.com/feldera/etl/blob/248fa4077c5b0f3e7bc2918a9709be2a03efea73/crates/etl/src/replication/table_sync/copy.rs#L586) (commit `248fa4077c5b`).
- [feldera-etl/crates/etl/src/postgres/client/transaction.rs:128](https://github.com/feldera/etl/blob/248fa4077c5b0f3e7bc2918a9709be2a03efea73/crates/etl/src/postgres/client/transaction.rs#L128) (commit `248fa4077c5b`).

## 13. pg_flo v0.0.15: CTID initial copy

В восстановленном архиве Go module CopyTable читает pg_class.relpages, создаёт очередь диапазонов по 1000 heap pages, запускает MaxCopyWorkersPerTable. Последний range заканчивается максимальным uint32; SELECT использует ctid >= start AND ctid < end. Каждая range-транзакция импортирует snapshotID. Это историческая v0.0.15, не доказательство текущего состояния недоступного GitHub проекта.

**Эффективность и границы:** Реальный parallel scan без выбора PK, общая очередь. В этой версии есть конкретный риск: generateRanges(0) возвращает пустой список, хотя relpages может быть устаревшей статистикой; также lookup только по relname требует проверки неоднозначных схем. Это вывод чтения кода, не воспроизведённый баг. Алгоритм нельзя копировать без безопасного покрытия пустых/устаревших оценок. GitHub permalinks могут быть недоступны; локальный module archive и metadata — сохраняемое доказательство.

**Проверенные места:**

- [pg-flo/pkg/replicator/copy_and_stream_replicator.go:163](https://github.com/pgflo/pg_flo/blob/33adfb2473093c9eb023fc56b5dd2fe97dadfbc3/pkg/replicator/copy_and_stream_replicator.go#L163) (commit `33adfb247309`).
- [pg-flo/pkg/replicator/copy_and_stream_replicator.go:155](https://github.com/pgflo/pg_flo/blob/33adfb2473093c9eb023fc56b5dd2fe97dadfbc3/pkg/replicator/copy_and_stream_replicator.go#L155) (commit `33adfb247309`).
- [pg-flo/pkg/replicator/copy_and_stream_replicator.go:256](https://github.com/pgflo/pg_flo/blob/33adfb2473093c9eb023fc56b5dd2fe97dadfbc3/pkg/replicator/copy_and_stream_replicator.go#L256) (commit `33adfb247309`).
- [pg-flo/pkg/replicator/copy_and_stream_replicator.go:234](https://github.com/pgflo/pg_flo/blob/33adfb2473093c9eb023fc56b5dd2fe97dadfbc3/pkg/replicator/copy_and_stream_replicator.go#L234) (commit `33adfb247309`).

Архив: [Go module v0.0.15](https://proxy.golang.org/github.com/pgflo/pg_flo/@v/v0.0.15.zip). Локальный файл: [copy_and_stream_replicator.go](/Users/timmyb32r/cursor/transferia/target/research/table-splitting/sources/pg-flo/pkg/replicator/copy_and_stream_replicator.go:164).

## 14. TiCDC: native TiKV span scheduling

Табличный key span делится по границам native TiKV regions. regionCountSplitter группирует примерно равное количество regions либо соблюдает configured regions-per-span. writeBytesSplitter использует WrittenBytes + базовый вес каждого region и группирует соседние regions по ожидаемой write-нагрузке. Это распределение native CDC keyspace, не generic JDBC backfill внешнего MySQL.

**Эффективность и границы:** Использует уже существующую физическую географию данных и метрики горячих диапазонов; не требует COUNT/NTILE SQL. Равное число regions не равно равной нагрузке, weighted вариант учитывает горячие участки. Цена — тесная привязка к TiKV/PD, свежести region metadata и корректному handoff spans.

**Проверенные места:**

- [ticdc/maintainer/split/region_count_splitter.go:73](https://github.com/pingcap/ticdc/blob/d4bee4bfd64c322e93411feb46eccb7312fd2caa/maintainer/split/region_count_splitter.go#L73) (commit `d4bee4bfd64c`).
- [ticdc/maintainer/split/region_count_splitter.go:99](https://github.com/pingcap/ticdc/blob/d4bee4bfd64c322e93411feb46eccb7312fd2caa/maintainer/split/region_count_splitter.go#L99) (commit `d4bee4bfd64c`).
- [ticdc/maintainer/split/write_bytes_splitter.go:115](https://github.com/pingcap/ticdc/blob/d4bee4bfd64c322e93411feb46eccb7312fd2caa/maintainer/split/write_bytes_splitter.go#L115) (commit `d4bee4bfd64c`).

## 15. Apache Paimon: splits собственного lake table

AppendOnlySplitGenerator сортирует DataFileMeta по sequence number и bin-pack группирует файлы по max(fileSize, openFileCost) до targetSplitSize. Streaming для bucket-aware таблиц сохраняет группу bucket целиком. ChainTableUtils дополнительно умеет batch splits по непересекающимся key-range sections внутри bucket.

**Эффективность и границы:** Работа по metadata избегает сканирования пользовательских строк при планировании; open-file cost предотвращает чрезмерно мелкие tasks. Это split файлов/своего table format, а не SQL-ranges произвольной внешней БД. Переносим полезную идею стоимости I/O + открытия файла, а не приписываем Paimon JDBC snapshot.

**Проверенные места:**

- [paimon/paimon-core/src/main/java/org/apache/paimon/table/source/AppendOnlySplitGenerator.java:56](https://github.com/apache/paimon/blob/10cf6ebc7c4f0ba6066d32d8aed2b0b85e0b8349/paimon-core/src/main/java/org/apache/paimon/table/source/AppendOnlySplitGenerator.java#L56) (commit `10cf6ebc7c4f`).
- [paimon/paimon-core/src/main/java/org/apache/paimon/table/source/AppendOnlySplitGenerator.java:65](https://github.com/apache/paimon/blob/10cf6ebc7c4f0ba6066d32d8aed2b0b85e0b8349/paimon-core/src/main/java/org/apache/paimon/table/source/AppendOnlySplitGenerator.java#L65) (commit `10cf6ebc7c4f`).
- [paimon/paimon-core/src/main/java/org/apache/paimon/utils/ChainTableUtils.java:365](https://github.com/apache/paimon/blob/10cf6ebc7c4f0ba6066d32d8aed2b0b85e0b8349/paimon-core/src/main/java/org/apache/paimon/utils/ChainTableUtils.java#L365) (commit `10cf6ebc7c4f`).

## 16. Apache Fluss: buckets и hybrid splits

FlinkSourceEnumerator получает lake, KV snapshot и log splits собственной Fluss table. Он сохраняет правило: splits одного bucket назначаются одному reader; отдельно отслеживает native table partitions. Это native snapshot/log decomposition, а не авторазбиение SQL таблицы в сторонней БД.

**Эффективность и границы:** Готовое физическое распределение уменьшает planning overhead и помогает сохранить порядок bucket. Перекошенный bucket может ограничивать параллелизм; физические buckets и логический SQL partition key — разные решения.

**Проверенные места:**

- [fluss/fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/source/enumerator/FlinkSourceEnumerator.java:100](https://github.com/apache/fluss/blob/09130be63bce9b075146133ed70aaabebfec9589/fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/source/enumerator/FlinkSourceEnumerator.java#L100) (commit `09130be63bce`).
- [fluss/fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/source/enumerator/FlinkSourceEnumerator.java:103](https://github.com/apache/fluss/blob/09130be63bce9b075146133ed70aaabebfec9589/fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/source/enumerator/FlinkSourceEnumerator.java#L103) (commit `09130be63bce`).

## 17. Trino generic JDBC

BaseJdbcClient.getSplits возвращает FixedSplitSource с одним JdbcSplit без additional predicate. Наличие универсальной distributed execution системы Trino не делает этот базовый JDBC reader автоматическим intra-table range planner. Другие connector implementations могут переопределять поведение.

**Эффективность и границы:** Pushdown сокращает данные и использует мощность source SQL engine. Один generic split может быть bottleneck raw extraction; наличие Hive/Iceberg file splits не является доказательством JDBC range splitting.

**Проверенные места:**

- [trino/plugin/trino-base-jdbc/src/main/java/io/trino/plugin/jdbc/BaseJdbcClient.java:630](https://github.com/trinodb/trino/blob/cee1916bcda0327c53e21ba9c4ea95723d490e85/plugin/trino-base-jdbc/src/main/java/io/trino/plugin/jdbc/BaseJdbcClient.java#L630) (commit `cee1916bcda0`).

## 18. Maxwell MySQL bootstrap

SynchronousBootstrapper строит SELECT * FROM table, добавляет пользовательский whereClause и ORDER BY PK, затем исполняет один streaming ResultSet (fetchSize=Integer.MIN_VALUE для драйвера). Это выборка/фильтр, а не автоматически вычисленные конкурентные chunks.

**Эффективность и границы:** Простая выгрузка с малой клиентской памятью; whereClause позволяет вручную выбирать часть данных. Async bootstrap означает взаимодействие с потоком изменений, а не автоматический range partitioning; его consistency-риски нужно рассматривать отдельно.

**Проверенные места:**

- [maxwell/src/main/java/com/zendesk/maxwell/bootstrap/SynchronousBootstrapper.java:207](https://github.com/zendesk/maxwell/blob/310cf1cb85af5ea3a4aac63afbc8c006ebbfac60/src/main/java/com/zendesk/maxwell/bootstrap/SynchronousBootstrapper.java#L207) (commit `310cf1cb85af`).
- [maxwell/src/main/java/com/zendesk/maxwell/bootstrap/SynchronousBootstrapper.java:222](https://github.com/zendesk/maxwell/blob/310cf1cb85af5ea3a4aac63afbc8c006ebbfac60/src/main/java/com/zendesk/maxwell/bootstrap/SynchronousBootstrapper.java#L222) (commit `310cf1cb85af`).

## 19. Bucardo fullcopy и delta key batches

В examined copy loop fullcopy использует COPY(SELECT ... FROM ONLY table) без WHERE. Для дельт строятся порции изменённых PK и условия ANY/IN. Поэтому PK chunks в этом коде относятся к изменённым ключам, а не разбиению initial full table на равные диапазоны.

**Эффективность и границы:** COPY эффективен для bulk, key batches уменьшают overhead передачи CDC-изменений. Нельзя приравнивать number_chunks в delta path к parallel full snapshot. Пользовательские sync/table filters остаются отдельным механизмом.

**Проверенные места:**

- [bucardo/Bucardo.pm:9936](https://github.com/bucardo/bucardo/blob/d9919c804d2c84a25e9bd8895908e80932609160/Bucardo.pm#L9936) (commit `d9919c804d2c`).
- [bucardo/Bucardo.pm:9928](https://github.com/bucardo/bucardo/blob/d9919c804d2c84a25e9bd8895908e80932609160/Bucardo.pm#L9928) (commit `d9919c804d2c`).

## 20. Singer tap-postgres (Stitch lineage)

Full-table sync читает один server-side cursor, сортирует по xmin и сохраняет xmin bookmark для resume. View sync — один SELECT. Размер itersize управляет порцией получения курсора; не найден N-range planner в рассматриваемой sync strategy.

**Эффективность и границы:** Потоковое чтение и checkpoint не равны concurrent chunks. xmin имеет транзакционную, не primary-key семантику; алгоритм resume требует отдельного correctness-аудита и не должен автоматически становиться образцом нового splitter. Лицензия этого tap — AGPLv3; Singer protocol не определяет универсальный способ table partitioning.

**Проверенные места:**

- [singer-postgres/tap_postgres/sync_strategies/full_table.py:43](https://github.com/singer-io/tap-postgres/blob/821f1cfdc0942f3ca839d4cf5abacb18858ac64a/tap_postgres/sync_strategies/full_table.py#L43) (commit `821f1cfdc094`).
- [singer-postgres/tap_postgres/sync_strategies/full_table.py:123](https://github.com/singer-io/tap-postgres/blob/821f1cfdc0942f3ca839d4cf5abacb18858ac64a/tap_postgres/sync_strategies/full_table.py#L123) (commit `821f1cfdc094`).

## 21. Meltano Singer SDK generic SQLStream

Базовый SQLStream.get_records исполняет build_query(context) одним соединением. Если непустой partition context передан базовой реализации, она явно выбрасывает NotImplementedError: stream does not support partitioning. Subclass/plugin может реализовать это сам; state partitioning API Singer не означает готовый SQL boundary planner.

**Эффективность и границы:** Расширяемый framework и явный отказ неподдерживаемого режима. Возможности конкретного tap нельзя выводить из наличия context/partition API в SDK; parallel SQL extraction нуждается в реализации connector.

**Проверенные места:**

- [singer-sdk/singer_sdk/sql/stream.py:267](https://github.com/meltano/sdk/blob/ce2e8f2b7fc145fd610965c713794839d137fe1d/singer_sdk/sql/stream.py#L267) (commit `ce2e8f2b7fc1`).
- [singer-sdk/singer_sdk/sql/stream.py:271](https://github.com/meltano/sdk/blob/ce2e8f2b7fc145fd610965c713794839d137fe1d/singer_sdk/sql/stream.py#L271) (commit `ce2e8f2b7fc1`).

## 22. Kestra JDBC plugin

Query исполняет один SQL statement; STORE потоково сохраняет результат во внутреннее хранилище, FETCH/FETCH_ONE возвращают результаты. Пользовательский orchestration может запускать много Query с разными параметрами, но fetchType/STORE не являются автоматическим делением таблицы на диапазоны.

**Эффективность и границы:** Хорошо для явно управляемых SQL tasks и файловых staging результатов. Полнота, непересечение диапазонов и согласованный snapshot при нескольких Query — задача workflow/connector, не свойство fetchType.

**Проверенные места:**

- [kestra-jdbc/plugin-jdbc/src/main/resources/doc/io.kestra.plugin.jdbc.md:11](https://github.com/kestra-io/plugin-jdbc/blob/4fcc170b244d5850de8d7babf842d39b953678a8/plugin-jdbc/src/main/resources/doc/io.kestra.plugin.jdbc.md#L11) (commit `4fcc170b244d`).

## 23. SQLines SQLData migration

Worker получает следующую таблицу из GetNextTask или авторский query из GetNextQueryTask. TransferRows открывает один source cursor; отдельно конвейеризует fetch следующего batch и запись текущего batch двумя потоками. Это parallel tables/queries и read-write pipeline, а не автоматически рассчитанные N диапазонов одной таблицы.

**Эффективность и границы:** Конкурентный fetch/write может скрывать сетевую задержку при одном source scan. Ручные SQL query tasks могут описывать части таблицы, но условия полноты, snapshot и балансировки тогда задаёт пользователь. Число sessions само по себе не свидетельствует о разрезании одной таблицы.

**Проверенные места:**

- [sqlines/sqldata/sqldata.cpp:1889](https://github.com/dmtolpeko/sqlines/blob/4d0d87896cead95481fc4656f831a8ac6d7ec291/sqldata/sqldata.cpp#L1889) (commit `4d0d87896cea`).
- [sqlines/sqldata/sqldata.cpp:1909](https://github.com/dmtolpeko/sqlines/blob/4d0d87896cead95481fc4656f831a8ac6d7ec291/sqldata/sqldata.cpp#L1909) (commit `4d0d87896cea`).
- [sqlines/sqldata/sqldb.cpp:943](https://github.com/dmtolpeko/sqlines/blob/4d0d87896cead95481fc4656f831a8ac6d7ec291/sqldata/sqldb.cpp#L943) (commit `4d0d87896cea`).

## Что эти проверки добавляют к проектированию Transferia

1. Отделять число диапазонов от числа соединений: shared queue с запасом небольших work items обычно лучше фиксированных P огромных tasks; реальный пример — Feldera/ETL.
2. Метаданные размеров/строк использовать как оценки стоимости, а не единственное доказательство полноты покрытия. Открытые края диапазонов помогают, но пустой план на stale relpages требует отдельной защиты.
3. Учёт реальных байтов/нагрузки лучше равного числа объектов: Paimon использует размер файлов и стоимость открытия, TiCDC — written bytes regions. Для snapshot это надо адаптировать к read cost, не просто скопировать CDC write weight.
4. Интерфейс user SQL splitter полезен как явный advanced mode, но не снимает с системы проверки границ, NULL, snapshot и подтверждения результата. Hazelcast честно документирует ограничения; новый default Transferia должен их предотвращать.
5. Не рекламировать fetchSize, batch_size, result.partitions или downstream hash shuffle как ускорение чтения одной source-table.
