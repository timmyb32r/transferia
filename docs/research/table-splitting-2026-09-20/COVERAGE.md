# Полный реестр охвата каталога

Дата: 20 сентября 2026. Это контроль полноты **учёта**, а не заявление о полном аудите всех коннекторов каждого продукта. Каждая продуктовая строка исходного [каталога](../../competitor-catalog-2026-09-20.md) учтена ниже; старые имена вынесены отдельно.

**Ключевое ограничение:** загрузка репозитория и поиск символов не доказывают отсутствие возможности. Где нет конкретного подтверждения splitter, статус остаётся «не установлено», либо указан перенос ответственности на подключённый коннектор. Детальные положительные выводы: [код](CODE-REPORT.md), [дополнительный кодовый аудит](ADDITIONAL-CODE.md), [коммерческие продукты](COMMERCIAL.md).

**Классы:** O — OSS-компонент; S — source-available; К — коммерческий продукт; М — смешанная поставка. Колонка «каталог → уточнение» сохраняет исходную классификацию и показывает найденные расхождения. Наличие OSS-компонента не лицензирует весь сервис. Верхнеуровневая лицензия не заменяет аудит зависимостей и enterprise-каталогов.

**Загрузка:** 134 репозиториев/компонентов в manifest: downloaded=130, downloaded-module-archive=2, error=2. Репозитории получены shallow/sparse, то есть локально находятся конкретные исходники, а не вся история и не все плагины. Два fallback-архива Go — фиксированные исторические версии.

Локальный корень: `target/research/table-splitting/sources/` (игнорируется Git). Для каждой строки `key` ниже означает соответствующий подкаталог. [SOURCE-MANIFEST.json](SOURCE-MANIFEST.json) содержит remote, commit, sparse paths, фактическое число файлов, лицензирующие документы и ошибки скачивания.

## 1. Универсальные платформы переноса данных, ELT и ingestion

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 1 | [Transferia, Go](https://github.com/transferia/transferia) | O | универсальный ingestion; `transferia-go`. Механизм и ограничения в CODE-REPORT. |
| 2 | [Airbyte](https://airbyte.com/) | S/К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 3 | [Fivetran](https://www.fivetran.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 4 | [Estuary Flow](https://estuary.dev/) | S/К → BSL по умолчанию; отдельные каталоги могут иметь собственные лицензии; Apache+MIT требуют письменного согласия | универсальный ingestion; `estuary-connectors`. Механизм и ограничения в CODE-REPORT. |
| 5 | [Hevo Data](https://hevodata.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 6 | [Matillion](https://www.matillion.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 7 | [Integrate.io](https://www.integrate.io/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 8 | [Keboola](https://www.keboola.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 9 | [Dataddo](https://www.dataddo.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 10 | [CData Sync](https://www.cdata.com/sync/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 11 | [Boomi Data Integration, ранее Rivery](https://boomi.com/rivery-is-now-boomi-data-integration/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 12 | [Etlworks](https://etlworks.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 13 | [Nexla](https://nexla.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 14 | [Etleap](https://etleap.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 15 | [Portable](https://portable.io/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 16 | [Skyvia](https://skyvia.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 17 | [Polytomic](https://www.polytomic.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 18 | [Weld](https://weld.app/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 19 | [TROCCO](https://documents.trocco.io/docs/en/about-managed-etl) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 20 | [DataChannel](https://www.datachannel.co/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 21 | [Y42](https://www.y42.com/) | К | универсальный ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 22 | [CloudQuery](https://www.cloudquery.io/) | М | универсальный ingestion; `cloudquery`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. Дополнительная точечная проверка этого продукта — в заключительной ingestion-таблице ниже. |
## 2. Корпоративные ETL/ELT и визуальная интеграция

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 23 | [Informatica IDMC / Cloud Data Integration](https://www.informatica.com/products/cloud-integration.html) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 24 | [Qlik Talend Cloud](https://www.qlik.com/us/products/qlik-talend-cloud) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 25 | [IBM DataStage](https://www.ibm.com/products/datastage) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 26 | [IBM StreamSets](https://www.ibm.com/products/streamsets) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 27 | [Pentaho Data Integration](https://pentaho.com/) | М → Pentaho Developer Edition 11.0: BSL-1.1 | enterprise ETL; `pentaho`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 28 | [CloverDX](https://www.cloverdx.com/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 29 | [Ab Initio](https://www.abinitio.com/en/data-processing-platform/data-formats-connectors/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 30 | [SAP Data Services](https://www.sap.com/products/data-cloud/data-services.html) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 31 | [Oracle Data Integrator](https://www.oracle.com/middleware/technologies/data-integrator.html) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 32 | [Microsoft SSIS](https://learn.microsoft.com/en-us/sql/integration-services/sql-server-integration-services) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 33 | [SAS Data Management](https://www.sas.com/en_us/solutions/data-management.html) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 34 | [Alteryx](https://www.alteryx.com/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 35 | [FME](https://fme.safe.com/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 36 | [KNIME](https://www.knime.com/) | O/К → GPLv3 с дополнительными разрешениями в org.knime.database/LICENSE.txt | enterprise ETL; `knime`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 37 | [TimeXtender](https://www.timextender.com/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 38 | [WhereScape](https://www.wherescape.com/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 39 | [K2view](https://www.k2view.com/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 3. Открытые коннекторные движки и инструменты ingestion

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 40 | [Apache SeaTunnel](https://seatunnel.apache.org/home/) | O | коннекторный ingestion; `seatunnel`. Механизм и ограничения в CODE-REPORT. |
| 41 | [Apache NiFi](https://nifi.apache.org/) | O | коннекторный ingestion; `nifi`. Механизм и ограничения в CODE-REPORT. |
| 42 | [Apache InLong](https://inlong.apache.org/) | O | коннекторный ingestion; `inlong`. Механизм и ограничения в CODE-REPORT. |
| 43 | [Alibaba DataX](https://github.com/alibaba/DataX) | O | коннекторный ingestion; `datax`. Механизм и ограничения в CODE-REPORT. |
| 44 | [ChunJun, ранее FlinkX](https://github.com/DTStack/chunjun) | O | коннекторный ingestion; `chunjun`. Механизм и ограничения в CODE-REPORT. |
| 45 | [Apache Hop](https://hop.apache.org/) | O | коннекторный ingestion; `hop`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 46 | [Meltano](https://meltano.com/) | O/К | коннекторный ingestion; `meltano` / `singer-sdk`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). Дополнительная точечная проверка этого продукта — в заключительной ingestion-таблице ниже. |
| 47 | [dlt](https://dlthub.com/) | O/К | коннекторный ingestion; `dlt`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 48 | [Sling](https://slingdata.io/) | O/К | коннекторный ingestion; `sling`. Механизм и ограничения в CODE-REPORT. |
| 49 | [ingestr](https://github.com/bruin-data/ingestr) | O → FSL-1.1-ALv2; будущая Apache не означает текущую OSS-лицензию | коннекторный ingestion; `ingestr`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 50 | [Mage](https://www.mage.ai/) | O/К | коннекторный ingestion; `mage`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. Дополнительная точечная проверка этого продукта — в заключительной ingestion-таблице ниже. |
| 51 | [Embulk](https://www.embulk.org/) | O | коннекторный ingestion; `embulk` / `embulk-jdbc`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 52 | [Singer](https://www.singer.io/) | O | коннекторный ingestion; `singer-postgres` / `singer-sdk`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). Дополнительная точечная проверка этого продукта — в заключительной ingestion-таблице ниже. |
| 53 | [Apache Gobblin](https://github.com/apache/gobblin) | O | коннекторный ingestion; `gobblin`. Механизм и ограничения в CODE-REPORT. |
## 4. Специализированный CDC и гетерогенная репликация

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 54 | [Debezium](https://debezium.io/) | O | CDC / backfill; `debezium`. Механизм и ограничения в CODE-REPORT. |
| 55 | [Apache Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) | O | CDC / backfill; `flink-cdc`. Механизм и ограничения в CODE-REPORT. |
| 56 | [Qlik Replicate](https://www.qlik.com/us/products/qlik-replicate) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 57 | [Oracle GoldenGate](https://www.oracle.com/integration/goldengate/) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 58 | [IBM Data Replication](https://www.ibm.com/products/data-replication) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 59 | [Precisely Connect](https://www.precisely.com/solution/real-time-cdc-and-etl-solutions/) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 60 | [Striim](https://www.striim.com/) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 61 | [Fivetran HVR](https://fivetran.com/docs/hvr6) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 62 | [Quest SharePlex](https://www.quest.com/products/shareplex) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 63 | [Syniti Data Replication](https://www.syniti.com/solutions/data-replication/) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 64 | [SymmetricDS](https://symmetricds.org/) | O/К → GNU AGPLv3, README.md | CDC / backfill; `symmetricds`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 65 | [PeerDB](https://github.com/PeerDB-io/peerdb) | O | CDC / backfill; `peerdb`. Механизм и ограничения в CODE-REPORT. |
| 66 | [Sequin](https://sequinstream.com/) | O/К | CDC / backfill; `sequin`. Механизм и ограничения в CODE-REPORT. |
| 67 | [Streamkap](https://streamkap.com/) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 68 | [Artie](https://www.artie.com/product) | М | CDC / backfill; `artie`: исходники недоступны; error. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 69 | [DBConvert Streams](https://streams.dbconvert.com/) | К | CDC / backfill; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 70 | [dsync, Adiom](https://github.com/adiom-data/dsync) | O/К | CDC / backfill; `dsync`. Механизм и ограничения в CODE-REPORT. |
| 71 | [pgstream, Xata](https://github.com/xataio/pgstream) | O | CDC / backfill; `pgstream`. Механизм и ограничения в CODE-REPORT. |
| 72 | [Alibaba Canal](https://github.com/alibaba/canal) | O | CDC / backfill; `canal`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 73 | [Maxwell’s daemon](https://github.com/zendesk/maxwell) | O | CDC / backfill; `maxwell`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
## 5. Управляемые облачные CDC и миграции БД

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 74 | [AWS DMS](https://aws.amazon.com/dms/) | К | управляемая миграция / CDC; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 75 | [Google Cloud Datastream](https://cloud.google.com/datastream) | К | управляемая миграция / CDC; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 76 | [Google Database Migration Service](https://cloud.google.com/database-migration) | К | управляемая миграция / CDC; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 77 | [Azure Database Migration Service](https://azure.microsoft.com/en-us/products/database-migration) | К | управляемая миграция / CDC; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 78 | [Alibaba Cloud DTS](https://www.alibabacloud.com/en/product/data-transmission-service) | К | управляемая миграция / CDC; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 79 | [Tencent Cloud DTS](https://www.tencentcloud.com/document/product/571/18135?lang=en) | К | управляемая миграция / CDC; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 80 | [Huawei Cloud DRS](https://www.huaweicloud.com/intl/en-us/product/drs.html) | К | управляемая миграция / CDC; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 6. Облачные ETL и платформы загрузки в lakehouse

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 81 | [AWS Glue](https://aws.amazon.com/glue/) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 82 | [Amazon AppFlow](https://aws.amazon.com/appflow/) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 83 | [Azure Data Factory](https://azure.microsoft.com/en-us/products/data-factory) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 84 | [Fabric Data Factory](https://learn.microsoft.com/en-us/fabric/data-factory/data-factory-overview) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 85 | [Google Cloud Dataflow](https://cloud.google.com/products/dataflow) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 86 | [Google Cloud Data Fusion](https://cloud.google.com/data-fusion) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 87 | [Databricks Lakeflow](https://www.databricks.com/product/data-engineering) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 88 | [Snowflake Openflow](https://docs.snowflake.com/en/user-guide/data-integration/openflow/about) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 89 | [Cloudera Data Flow](https://www.cloudera.com/products/data-in-motion/dataflow.html) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 90 | [Alibaba DataWorks Data Integration](https://www.alibabacloud.com/help/en/dataworks/user-guide/what-is-dataworks) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 91 | [OCI Data Integration](https://www.oracle.com/integration/data-integration/) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 92 | [Huawei Cloud CDM](https://www.huaweicloud.com/intl/en-us/product/cdm.html) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 93 | [Qlik Open Lakehouse](https://www.qlik.com/us/products/qlik-open-lakehouse) | К | облачный ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 7. Специализированные ingestion-компоненты конкретных приёмников

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 94 | [ClickHouse ClickPipes](https://clickhouse.com/cloud/clickpipes) | К | приёмник / lakehouse ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 95 | [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview) | К | приёмник / lakehouse ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 96 | [SingleStore Flow, ранее BryteFlow](https://www.singlestore.com/blog/the-journey-from-bryteflow-to-singlestore-flow/) | К | приёмник / lakehouse ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 97 | [Redis Data Integration](https://redis.io/docs/latest/integrate/redis-data-integration/) | К | приёмник / lakehouse ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 98 | [Microsoft Fabric Mirroring](https://learn.microsoft.com/en-us/fabric/mirroring/overview) | К | приёмник / lakehouse ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 99 | [Amazon Data Firehose](https://aws.amazon.com/firehose/) | К | приёмник / lakehouse ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 100 | [Amazon OpenSearch Ingestion](https://aws.amazon.com/opensearch-service/features/ingestion/) | К | приёмник / lakehouse ingestion; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 101 | [Apache Hudi Streamer](https://hudi.apache.org/docs/hoodie_streaming_ingestion/) | O | приёмник / lakehouse ingestion; `hudi` / `spark`. Механизм и ограничения в CODE-REPORT. |
| 102 | [Apache Paimon](https://paimon.apache.org/) | O | приёмник / lakehouse ingestion; `paimon`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 103 | [Apache Fluss](https://fluss.apache.org/) | O | приёмник / lakehouse ingestion; `fluss`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
## 8. Нативная репликация и CDC отдельных СУБД

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 104 | [PostgreSQL logical replication](https://www.postgresql.org/docs/current/logical-replication.html) | O → PostgreSQL License, COPYRIGHT | нативная репликация БД; `postgres`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 105 | [pglogical](https://github.com/2ndQuadrant/pglogical) | O → PostgreSQL License, README.md | нативная репликация БД; `pglogical`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 106 | [wal2json](https://github.com/eulerto/wal2json) | O | нативная репликация БД; `wal2json`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 107 | [EDB Postgres Distributed](https://www.enterprisedb.com/docs/pgd/latest/) | К | нативная репликация БД; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 108 | [pgEdge](https://www.pgedge.com/) | М | нативная репликация БД; `spock`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 109 | [Bucardo](https://github.com/bucardo/bucardo) | O | нативная репликация БД; `bucardo`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 110 | [TiCDC](https://docs.pingcap.com/tidb/stable/ticdc-overview/) | O | нативная репликация БД; `ticdc`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 111 | [TiDB Data Migration](https://docs.pingcap.com/tidb/stable/dm-overview/) | O | нативная репликация БД; `tiflow` / `dumpling`. Механизм и ограничения в CODE-REPORT. |
| 112 | [MongoDB Mongosync](https://www.mongodb.com/docs/mongosync/current/) | К | нативная репликация БД; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 113 | [CockroachDB changefeeds](https://docs.cockroachlabs.com/docs/stable/change-data-capture-overview) | К | нативная репликация БД; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 114 | [YugabyteDB CDC](https://docs.yugabyte.com/stable/explore/change-data-capture/) | М → Apache-2.0 для компонентов ядра; другие компоненты по отдельным условиям LICENSE.md | нативная репликация БД; `yugabyte`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
## 9. Очереди, коннекторы и перенос сообщений

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 115 | [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) | O | очереди / connector framework; `kafka`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 116 | [Confluent Connectors](https://www.confluent.io/product/connectors/) | М/К → Confluent Community License | очереди / connector framework; `confluent-jdbc`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 117 | [Redpanda Connect, ранее Benthos](https://www.redpanda.com/connect) | М → Лицензии отдельных компонентов в licenses/; не весь продукт Apache | очереди / connector framework; `redpanda-connect`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 118 | [Bento](https://warpstreamlabs.github.io/bento/) | O | очереди / connector framework; `bento`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 119 | [Apache Pulsar IO / Functions](https://pulsar.apache.org/) | O | очереди / connector framework; `pulsar`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 120 | [Apache Camel](https://camel.apache.org/) | O | очереди / connector framework; `camel`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 121 | [Spring Cloud Stream](https://spring.io/projects/spring-cloud-stream/) | O | очереди / connector framework; `spring-stream`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 122 | [Apache Pekko Connectors](https://pekko.apache.org/docs/pekko-connectors/current/) | O | очереди / connector framework; `pekko`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 123 | [Akka / Alpakka](https://doc.akka.io/libraries/alpakka/current/) | М → BSL-1.1 по умолчанию, кроме явно переопределенных файлов | очереди / connector framework; `alpakka`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 124 | [Kafka MirrorMaker](https://kafka.apache.org/documentation/#georeplication) | O | очереди / connector framework; `kafka`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 125 | [RabbitMQ Shovel](https://www.rabbitmq.com/docs/shovel) | O → MPL-2.0 core; отдельные файлы Apache-2.0/BSD | очереди / connector framework; `rabbitmq`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 126 | [NATS JetStream mirrors/sources](https://docs.nats.io/learn/jetstream/mirrors-and-sources) | O | очереди / connector framework; `nats`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
## 10. Управляемые платформы потоковой интеграции

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 127 | [Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/overview.html) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 128 | [Ververica](https://www.ververica.com/) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 129 | [Decodable](https://www.decodable.co/) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 130 | [Aiven for Apache Kafka Connect](https://aiven.io/kafka-connect) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 131 | [StreamNative](https://streamnative.io/) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 132 | [Amazon Managed Service for Apache Flink](https://aws.amazon.com/managed-service-apache-flink/) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 133 | [Amazon MSK Connect](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect.html) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 134 | [Google Managed Kafka Connect](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/connect-cluster/create-connect-cluster) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 135 | [Azure Stream Analytics](https://azure.microsoft.com/en-us/products/stream-analytics) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 136 | [Fabric Eventstreams](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/overview) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 137 | [Alibaba Realtime Compute for Apache Flink](https://www.alibabacloud.com/en/product/realtime-compute) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 138 | [IBM Event Automation](https://www.ibm.com/products/event-automation) | К | управляемая потоковая платформа; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 11. Движки batch/stream processing и непрерывных вычислений

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 139 | [Apache Spark](https://spark.apache.org/) | O | вычислительный движок; `spark`. Механизм и ограничения в CODE-REPORT. |
| 140 | [Apache Flink](https://flink.apache.org/) | O | вычислительный движок; `flink` / `flink-jdbc`. Механизм и ограничения в CODE-REPORT. |
| 141 | [Apache Beam](https://beam.apache.org/) | O | вычислительный движок; `beam`. Механизм и ограничения в CODE-REPORT. |
| 142 | [Kafka Streams](https://kafka.apache.org/documentation/streams/) | O | вычислительный движок; `kafka`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 143 | [ksqlDB](https://www.confluent.io/product/ksqldb/) | S/К | вычислительный движок; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 144 | [RisingWave](https://risingwave.com/) | O/К | вычислительный движок; `risingwave`. Механизм и ограничения в CODE-REPORT. |
| 145 | [Materialize](https://materialize.com/) | S/К | вычислительный движок; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 146 | [Feldera](https://www.feldera.com/) | O/К → MIT open-source edition; enterprise-код отдельно | вычислительный движок; `feldera` / `feldera-etl`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 147 | [DeltaStream](https://www.deltastream.io/) | К | вычислительный движок; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 148 | [Timeplus](https://www.timeplus.com/) | М | вычислительный движок; `proton`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 149 | [Epsio](https://www.epsio.io/) | К | вычислительный движок; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 150 | [Quix Streams](https://github.com/quixio/quix-streams) | O | вычислительный движок; `quix`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 151 | [Pathway](https://github.com/pathwaycom/pathway) | М → BSL-1.1 | вычислительный движок; `pathway`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 152 | [Hazelcast](https://hazelcast.com/) | М → Apache-2.0 / Hazelcast Community License, смотреть заголовки файлов | вычислительный движок; `hazelcast`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 153 | [Apache Storm](https://storm.apache.org/) | O | вычислительный движок; `storm`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
## 12. IoT, MQTT и edge-интеграция

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 154 | [EMQX](https://www.emqx.com/en) | М → BSL-1.1; отдельные компоненты могут отличаться | MQTT / edge; `emqx`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 155 | [HiveMQ](https://www.hivemq.com/) | М | MQTT / edge; `hivemq`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 156 | [eKuiper](https://github.com/lf-edge/ekuiper) | O | MQTT / edge; `ekuiper`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 157 | [Node-RED](https://nodered.org/) | O | MQTT / edge; `node-red`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 158 | [Apache StreamPipes](https://streampipes.apache.org/) | O | MQTT / edge; `streampipes`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 159 | [Solace](https://solace.com/) | К | MQTT / edge; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
## 13. Логи, телеметрия и observability pipelines

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 160 | [Vector](https://vector.dev/) | O | телеметрия; `vector`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 161 | [Fluent Bit](https://fluentbit.io/) | O | телеметрия; `fluent-bit`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 162 | [Fluentd](https://www.fluentd.org/) | O | телеметрия; `fluentd`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 163 | [Logstash](https://www.elastic.co/logstash) | М → Apache-2.0 / совместимые лицензии; x-pack Elastic License | телеметрия; `logstash` / `logstash-jdbc`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 164 | [Cribl Stream](https://cribl.io/products/stream/) | К | телеметрия; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 165 | [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) | O | телеметрия; `otel` / `otel-contrib`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 166 | [Grafana Alloy](https://grafana.com/docs/alloy/latest/) | O | телеметрия; `alloy`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 167 | [Bindplane](https://bindplane.com/) | М | телеметрия; `bindplane` (архив v1.35.0). Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 168 | [Mezmo Telemetry Pipeline](https://www.mezmo.com/platform/telemetry-pipeline) | К | телеметрия; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 169 | [Edge Delta](https://edgedelta.com/) | К | телеметрия; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 170 | [Chronosphere](https://chronosphere.io/) | К | телеметрия; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 171 | [Tenzir](https://tenzir.com/) | М | телеметрия; `tenzir`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 172 | [Axoflow](https://axoflow.com/) | К | телеметрия; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 173 | [OpenSearch Data Prepper](https://docs.opensearch.org/latest/data-prepper/) | O | телеметрия; `data-prepper`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 174 | [syslog-ng](https://github.com/syslog-ng/syslog-ng) | O/К → GPL/LGPL по компонентам | телеметрия; `syslog-ng`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 175 | [rsyslog](https://www.rsyslog.com/) | O → GPLv3; отдельные части LGPL/Apache | телеметрия; `rsyslog`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
## 14. Customer events и CDP

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 176 | [RudderStack](https://www.rudderstack.com/) | М → Elastic License 2.0 | customer events; `rudderstack`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 177 | [Snowplow](https://snowplow.io/) | М | customer events; `snowplow`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 178 | [Twilio Segment](https://www.twilio.com/en-us/segment) | К | customer events; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 179 | [Tealium](https://tealium.com/) | К | customer events; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 180 | [Treasure AI, ранее Treasure Data](https://www.treasure.ai/) | К | customer events; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 181 | [Adobe Real-Time CDP](https://business.adobe.com/products/real-time-customer-data-platform/rtcdp.html) | К | customer events; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
## 15. Reverse ETL и активация данных

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 182 | [Hightouch](https://hightouch.com/) | К | reverse ETL; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 183 | [Multiwoven](https://www.multiwoven.com/) | O/К | reverse ETL; `multiwoven`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 184 | [GrowthLoop](https://www.growthloop.com/) | К | reverse ETL; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 185 | [Omnata](https://omnata.com/) | К | reverse ETL; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
## 16. Маркетинговый ELT и отраслевые коннекторы

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 186 | [Adverity](https://www.adverity.com/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 187 | [Funnel](https://funnel.io/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 188 | [Improvado](https://improvado.io/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 189 | [Supermetrics](https://supermetrics.com/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 190 | [Windsor.ai](https://windsor.ai/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 191 | [Renta](https://renta.im/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 192 | [Coupler.io](https://www.coupler.io/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 193 | [Dataslayer](https://www.dataslayer.ai/) | К | SaaS / маркетинг; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 194 | [OWOX](https://www.owox.com/) | М → MIT connectors; ELv2 platform; отдельная enterprise-лицензия | SaaS / маркетинг; `owox`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
## 17. iPaaS, API и интеграция бизнес-приложений

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 195 | [Boomi Enterprise Platform](https://boomi.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 196 | [MuleSoft Anypoint Platform](https://www.mulesoft.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 197 | [SnapLogic](https://www.snaplogic.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 198 | [Workato](https://www.workato.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 199 | [Jitterbit](https://www.jitterbit.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 200 | [Celigo](https://www.celigo.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 201 | [Tray.ai](https://tray.ai/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 202 | [n8n](https://n8n.io/) | S/К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 203 | [WSO2 Integrator](https://wso2.com/integration-platform/integrator/) | O/К | iPaaS / workflows; `wso2`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 204 | [IBM webMethods Integration](https://www.ibm.com/products/webmethods-integration) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 205 | [SAP Integration Suite](https://www.sap.com/products/technology-platform/integration-suite.html) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 206 | [Oracle Integration](https://www.oracle.com/integration/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 207 | [TIBCO Platform Integration / BusinessWorks](https://www.tibco.com/platform/integration) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 208 | [Frends](https://frends.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 209 | [Digibee](https://www.digibee.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 210 | [Flowgear](https://www.flowgear.net/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 211 | [Linx](https://linx.software/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 212 | [elastic.io](https://www.elastic.io/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 213 | [Make](https://www.make.com/en) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 214 | [Zapier](https://zapier.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 215 | [Activepieces](https://www.activepieces.com/) | O/К → MIT за пределами явно выделенных enterprise-каталогов | iPaaS / workflows; `activepieces`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 216 | [Pipedream](https://pipedream.com/) | М → Pipedream Source Available License | iPaaS / workflows; `pipedream`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 217 | [Zoho Flow](https://www.zoho.com/flow/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 218 | [Prismatic](https://prismatic.io/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 219 | [Paragon](https://www.useparagon.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 220 | [Cyclr](https://cyclr.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 221 | [Albato](https://albato.com/) | К | iPaaS / workflows; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
## 18. Российские платформы интеграции и ETL

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 222 | [Arenadata Streaming](https://arenadata.tech/ru/products/ads) | К, на OSS-компонентах (checkout базовых Apache Kafka/NiFi, не поставки ADS) | enterprise ETL; `kafka` / `nifi`. Механизм и ограничения в CODE-REPORT. |
| 223 | [Loginom](https://loginom.ru/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 224 | [DATAREON Platform](https://datareon.ru/solution/datareon-platform/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 225 | [Digital Q.DataFlows](https://q.diasoft.ru/mediacenter/news/v-platforme-digital-q-dataflows-dobavlena-vozmozhnost-ispolzovaniya-naborov-dannykh-v-etl-protsessakh/) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 226 | [Neoflex Datagram](https://www.neoflex.ru/publications/neoflex-datagram-etl-platform) | К | enterprise ETL; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 19. Массовое копирование, файловые переносы и миграционные утилиты

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 227 | [rclone](https://rclone.org/) | O | bulk / файлы / migration; `rclone`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 228 | [rsync](https://rsync.samba.org/) | O → GPLv3 с исключениями для библиотек | bulk / файлы / migration; `rsync`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 229 | [pgloader](https://pgloader.io/) | O | bulk / файлы / migration; `pgloader`. Механизм и ограничения в CODE-REPORT. |
| 230 | [pgcopydb](https://pgcopydb.readthedocs.io/en/latest/) | O | bulk / файлы / migration; `pgcopydb`. Механизм и ограничения в CODE-REPORT. |
| 231 | [Ora2Pg](https://ora2pg.darold.net/) | O | bulk / файлы / migration; `ora2pg`. Механизм и ограничения в CODE-REPORT. |
| 232 | [mydumper/myloader](https://github.com/mydumper/mydumper) | O | bulk / файлы / migration; `mydumper`. Механизм и ограничения в CODE-REPORT. |
| 233 | [MySQL Shell](https://github.com/mysql/mysql-shell) | O → GPLv2 и дополнительные разрешения/компоненты, полный LICENSE | bulk / файлы / migration; `mysql-shell`. Механизм и ограничения в CODE-REPORT. |
| 234 | [SQLines](https://sqlines.com/) | М | bulk / файлы / migration; `sqlines`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 235 | [AWS DataSync](https://aws.amazon.com/datasync/) | К | bulk / файлы / migration; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 236 | [Google Storage Transfer Service](https://cloud.google.com/storage-transfer-service) | К | bulk / файлы / migration; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 237 | [Azure Storage Mover](https://azure.microsoft.com/en-us/products/storage-mover) | К | bulk / файлы / migration; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 238 | [IBM Aspera](https://www.ibm.com/products/aspera) | К | bulk / файлы / migration; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 239 | [Progress MOVEit](https://www.progress.com/moveit) | К | bulk / файлы / migration; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 20. Оркестрация собственных pipelines

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 240 | [Apache Airflow](https://airflow.apache.org/) | O | оркестратор; `airflow`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 241 | [Dagster](https://dagster.io/) | O/К | оркестратор; `dagster`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 242 | [Prefect](https://www.prefect.io/) | O/К | оркестратор; `prefect`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 243 | [Kestra](https://kestra.io/) | O/К | оркестратор; `kestra`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 244 | [Apache DolphinScheduler](https://dolphinscheduler.apache.org/) | O | оркестратор; `dolphinscheduler`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 245 | [Argo Workflows](https://argo-workflows.readthedocs.io/en/latest/) | O | оркестратор; `argo`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 246 | [Flyte](https://flyte.org/) | O/К | оркестратор; `flyte`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 247 | [Luigi](https://github.com/spotify/luigi) | O | оркестратор; `luigi`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 248 | [Digdag](https://www.digdag.io/) | O | оркестратор; `digdag`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 249 | [Astronomer](https://www.astronomer.io/) | К | оркестратор; локальный engine checkout не приобретён. Основной объект — события/API/workflows; деление исходной SQL-таблицы не подтверждено одной этой ролью. Подтверждения/неопределённость — COMMERCIAL. |
| 250 | [Bruin](https://getbruin.com/) | O/К | оркестратор; `bruin`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 251 | [Apache StreamPark](https://github.com/apache/streampark) | O | оркестратор; `streampark`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 252 | [Dinky](https://github.com/DataLinkDC/dinky) | O | оркестратор; `dinky`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
| 253 | [Spring Cloud Data Flow](https://spring.io/blog/2025/04/21/spring-cloud-data-flow-commercial/) | К для дальнейшего развития | оркестратор; `spring-dataflow`. Индивидуальный intra-table splitter в этом реестре не установлен; очереди, workflow-параллелизм и batches его не доказывают. |
## 21. Вычислительные библиотеки и SQL-преобразования

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 254 | [Daft](https://www.daft.ai/) | O/К | библиотека / SQL processing; `daft`. Механизм и ограничения в CODE-REPORT. |
| 255 | [Ray Data](https://docs.ray.io/en/latest/data/data.html) | O | библиотека / SQL processing; `ray`. Механизм и ограничения в CODE-REPORT. |
| 256 | [Dask](https://www.dask.org/) | O | библиотека / SQL processing; `dask`. Механизм и ограничения в CODE-REPORT. |
| 257 | [Polars](https://pola.rs/) | O/К | библиотека / SQL processing; `polars` / `connectorx`. Механизм и ограничения в CODE-REPORT. |
| 258 | [DuckDB](https://duckdb.org/) | O | библиотека / SQL processing; `duckdb` / `duckdb-postgres`. Механизм и ограничения в CODE-REPORT. |
| 259 | [dbt](https://www.getdbt.com/) | O/К | библиотека / SQL processing; `dbt`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 260 | [SQLMesh](https://sqlmesh.readthedocs.io/en/stable/) | O | библиотека / SQL processing; `sqlmesh`. Конкретная единица работы и pinned API разобраны в заключительной таблице этого документа. |
| 261 | [Coalesce](https://coalesce.io/) | К | библиотека / SQL processing; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 22. Федерация и комплексные data platforms

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 262 | [Denodo](https://www.denodo.com/en) | К | федерация / data platform; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 263 | [Dremio](https://www.dremio.com/) | М → Apache-2.0 репозитория; default build включает не-OSS части, см. README OSS-only | федерация / data platform; `dremio`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 264 | [Trino](https://trino.io/) | O | федерация / data platform; `trino`. Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 265 | [Starburst](https://www.starburst.io/) | К | федерация / data platform; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 266 | [SAP Datasphere](https://www.sap.com/products/data-cloud/datasphere.html) | К | федерация / data platform; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 267 | [Palantir Foundry](https://www.palantir.com/platforms/foundry/) | К | федерация / data platform; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 268 | [Domo](https://www.domo.com/) | К | федерация / data platform; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
## 24. Архивные решения и проекты с неподтверждённой жизнеспособностью

| # | Продукт | Каталог → уточнение | Роль; локальные исходники и граница вывода |
|---:|---|---|---|
| 269 | [Talend Open Studio](https://community.qlik.com/t5/Installing-and-Upgrading/Talend-Open-studio-for-big-data-7-3-1/td-p/2458681) | историческое → No source acquired | историческое / статус не подтверждён; `talend`: исходники недоступны; error. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 270 | [Grouparoo](https://github.com/grouparoo/grouparoo) | историческое → MIT (верхнеуровневая лицензия) | историческое / статус не подтверждён; `grouparoo`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. Дополнительная точечная проверка этого продукта — в заключительной ingestion-таблице ниже. |
| 271 | [Apache Sqoop](https://attic.apache.org/projects/sqoop.html) | историческое → Apache-2.0 (верхнеуровневая лицензия) | историческое / статус не подтверждён; `sqoop`. Механизм и ограничения в CODE-REPORT. |
| 272 | [Apache Apex](https://attic.apache.org/projects/apex.html) | историческое → Apache-2.0 (верхнеуровневая лицензия); Apache-2.0 (верхнеуровневая лицензия) | историческое / статус не подтверждён; `apex` / `apex-malhar`. Механизм и ограничения в CODE-REPORT. |
| 273 | [Apache Samza](https://samza.apache.org/) | историческое → Apache-2.0 (верхнеуровневая лицензия) | историческое / статус не подтверждён; `samza`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 274 | [Apache Flume](https://flume.apache.org/) | историческое → Apache-2.0 (верхнеуровневая лицензия) | историческое / статус не подтверждён; `flume`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 275 | [Apache Heron](https://github.com/apache/incubator-heron) | историческое → Apache-2.0 (верхнеуровневая лицензия) | историческое / статус не подтверждён; `heron`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 276 | [Bytewax](https://github.com/bytewax/bytewax) | историческое → Apache-2.0 (верхнеуровневая лицензия) | историческое / статус не подтверждён; `bytewax`. Индивидуальный intra-table алгоритм в этом реестре не установлен; это не доказательство отсутствия функции. Дополнительные подтверждения — в кодовом аудите. |
| 277 | [pg_flo](https://github.com/pgflo/pg_flo) | историческое → Apache-2.0 (верхнеуровневая лицензия) | историческое / статус не подтверждён; `pg-flo` (архив v0.0.15). Подтверждённая единица чтения/разбиения и ограничения — [ADDITIONAL-CODE](ADDITIONAL-CODE.md). |
| 278 | [Equalum](https://www.equalum.io/) | историческое | историческое / статус не подтверждён; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |
| 279 | [Arcion](https://www.arcion.io/) | историческое | историческое / статус не подтверждён; локальный engine checkout не приобретён. Публичные описания и статус подтверждения — COMMERCIAL; закрытый алгоритм не приписывается по аналогии с OSS. |

## Связанный сервис, упомянутый внутри строки

| Продукт | Учёт |
|---|---|
| [Yandex Data Transfer](https://yandex.cloud/en/docs/data-transfer/) | К; связан с Transferia Go, но не считается идентичной поставкой. Исходники Go — `transferia-go`; управляемый сервис исследован отдельно по документации в COMMERCIAL. |

## 23. Старые названия: явные дубликаты и перенаправления

Эти девять строк не добавляют девять независимых современных движков. Описание перехода взято из исходного каталога; детали современных продуктов находятся в их основной строке.

| Старое имя | Куда отнесено / граница |
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

## Повторные упоминания без повторного подсчёта

- Polytomic, Weld, Integrate.io, DataChannel, RudderStack, Fivetran: повторное упоминание в reverse ETL; единственная основная строка сохранена.
- Yandex / Transferia: повторная ссылка в региональном разделе; сервис и Go-движок различены выше.
- Kafka Connect, Kafka Streams, MirrorMaker используют один checkout `kafka`, но это разные компоненты и разные строки каталога.
- Поддерживающие checkout `connectorx`, `duckdb-postgres`, `flink-jdbc`, `embulk-jdbc`, `logstash-jdbc`, `dumpling`, `apex-malhar`, `singer-sdk`, `singer-postgres`, `kestra-jdbc`, `otel-contrib` не считаются дополнительными конкурентами.

## Проверка полноты и воспроизводимость

- Продуктовых строк разделов 1–22 и 24: **279**; связанных сервисов внутри строки: **1**; строк старых имён: **9**.
- Извлечение учитывает также нестандартно оформленную жирную строку Apache Spark; пропуск её обычным шаблоном `- [` был бы ошибкой.
- Исходники не компилировались и не выполнялись; результаты — статический анализ конкретных commit и документированных версий.
- Недоступные GitHub-репозитории не заменены случайными форками: Artie и Talend остаются с явной ошибкой; pg_flo и Bindplane восстановлены через Go module proxy с фиксированной версией, commit и original_git_attempt в manifest.
- Для Git-checkout `actual_git_head` сверяется с metadata commit; для архивов Git HEAD отсутствует, поэтому значение `metadata_commit_matches_head` равно `null`, а происхождение фиксируется Origin из Go proxy.
- Относительные локальные пути manifest раскрываются от корня Transferia; они сознательно не обещают доступность всей истории или всех компонентов монорепозитория.

## Что именно делят смежные публичные компоненты

Ниже положительные подтверждения из конкретных API: что является единицей работы
и почему эту единицу нельзя автоматически считать частью исходной SQL-таблицы.
Это дополняет реестр: отсутствие SQL-splitter в конкретном API **не доказывает**
отсутствие такого коннектора во всей экосистеме. Для остальных представителей
группы роль в каталоге — классификация, а не перенос найденного алгоритма по аналогии.
Все ссылки закреплены на локально скачанные commit; исходники не исполнялись.
Большинство примеров — OSS; Confluent JDBC включён как source-available
компонент под Confluent Community License, а не как Apache-лицензированный Kafka Connect.

| Компонент | Что делится или планируется | Граница интерпретации | Код |
|---|---|---|---|
| Kafka Connect / Confluent JDBC | Connect делегирует раздачу work units конкретному Connector.taskConfigs(maxTasks); SourceRecord отдельно хранит source partition и source offset. Проверенный Confluent JDBC распределяет группы таблиц между задачами; query mode создаёт одну task-конфигурацию. | Количество Connect tasks не доказывает N непересекающихся диапазонов одной таблицы. В этом JDBC-компоненте различайте распределение таблиц и row-range partitioning. | [kafka:Connector.java:126](https://github.com/apache/kafka/blob/4c8d0ed26fd04a6ea585562e49cc375b423f6b38/connect/api/src/main/java/org/apache/kafka/connect/connector/Connector.java#L126), [kafka:SourceRecord.java:89](https://github.com/apache/kafka/blob/4c8d0ed26fd04a6ea585562e49cc375b423f6b38/connect/api/src/main/java/org/apache/kafka/connect/source/SourceRecord.java#L89), [confluent-jdbc:JdbcSourceConnector.java:210](https://github.com/confluentinc/kafka-connect-jdbc/blob/1d0d95032fe15c74e7b207907e8a8c2d8b3c4644/src/main/java/io/confluent/connect/jdbc/JdbcSourceConnector.java#L210) |
| Kafka Streams | TaskId определяется subtopology и partition ID потока. | Разделение исполнения по Kafka partitions; SQL-границы исходной таблицы формирует upstream source, а не этот TaskId. | [kafka:TaskId.java:28](https://github.com/apache/kafka/blob/4c8d0ed26fd04a6ea585562e49cc375b423f6b38/streams/src/main/java/org/apache/kafka/streams/processor/TaskId.java#L28) |
| NATS JetStream mirrors/sources | StreamSource задаёт исходный stream, начальную sequence/time, subject filter и преобразования subjects. | Это выбор событий и позиции чтения существующего stream. Sequence в журнале не эквивалентна диапазону PK таблицы. | [nats:stream.go:485](https://github.com/nats-io/nats-server/blob/8ad52657d1f3edb20155d9226dbb1fc2992148a2/server/stream.go#L485) |
| RabbitMQ Shovel | Источник Shovel задаётся queue, consumer settings и prefetch_count. | Prefetch ограничивает сообщения в полёте; он не генерирует запросы к SQL-таблице и не определяет snapshot boundaries. | [rabbitmq:rabbit_amqp091_shovel.erl:85](https://github.com/rabbitmq/rabbitmq-server/blob/169a13dc8a86383ca781a3d0be80acef461d0692/deps/rabbitmq_shovel/src/rabbit_amqp091_shovel.erl#L85) |
| Bento SQL Select | SQL input строит один SELECT с WHERE и выдаёт сообщение для каждой строки полученного result set. | Сообщения по строкам — формат потока, а не доказательство автоматического разбиения SELECT на параллельные диапазоны. Пользователь может отдельно построить несколько inputs/queries. | [bento:input_sql_select.go:25](https://github.com/warpstreamlabs/bento/blob/1c4fc2cb4a18c9f8c23f1d4354bc89d491921be6/internal/impl/sql/input_sql_select.go#L25), [bento:input_sql_select.go:234](https://github.com/warpstreamlabs/bento/blob/1c4fc2cb4a18c9f8c23f1d4354bc89d491921be6/internal/impl/sql/input_sql_select.go#L234) |
| Vector file source / Fluent Bit tail | Vector сохраняет позиции чтения файлов и fingerprint для их идентификации; Fluent Bit различает raw file offset и logical stream offset. | Единица возобновления — файл и смещение. Ротация/проверка файла имеют иной контракт, чем MVCC snapshot и PK identity. | [vector:file.rs:128](https://github.com/vectordotdev/vector/blob/c2d37ef05ddc2351333dc5a570b4f405db23034b/src/sources/file.rs#L128), [fluent-bit:tail_file_internal.h:44](https://github.com/fluent/fluent-bit/blob/177d6580836905ea990f999755b57822172524b0/plugins/in_tail/tail_file_internal.h#L44) |
| OpenTelemetry sqlqueryreceiver | В конфигурации перечисляются SQL queries; receiver регистрирует отдельные metric scrapers для соответствующих запросов. | Это пользовательские запросы метрик, а не обещание parallel full-table extract. Произвольный SQL может сам выбирать диапазон, но полнота и непересечение не выводятся из scheduler scrapers. | [otel-contrib:receiver.go:58](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/7d24eb88475392c2846f739d69860a7a6773308c/receiver/sqlqueryreceiver/receiver.go#L58) |
| Singer SDK REST / основанные на нём taps | REST stream хранит partition context и pagination token, paginator выдаёт следующую HTTP page. | API pagination и stream slicing зависят от tap/API. Не переносите эти свойства на SQL tap-postgres автоматически; его алгоритм отдельно в ADDITIONAL-CODE. | [singer-sdk:rest.py:641](https://github.com/meltano/sdk/blob/ce2e8f2b7fc145fd610965c713794839d137fe1d/singer_sdk/streams/rest.py#L641) |
| Dagster | Static/dynamic partitions и TimeWindowPartitionsDefinition описывают пользовательские ключи/окна; cron задаёт границы времени. | Это разбиение materialization/backfill jobs. SQL WHERE и согласованный snapshot должны обеспечить asset/IO manager/коннектор; time-window key сам их не создаёт. | [dagster:static.py:24](https://github.com/dagster-io/dagster/blob/6eb888da8cd57770e698a07de803d5b914ab04a2/python_modules/dagster/dagster/_core/definitions/partitions/definition/static.py#L24), [dagster:time_window.py:58](https://github.com/dagster-io/dagster/blob/6eb888da8cd57770e698a07de803d5b914ab04a2/python_modules/dagster/dagster/_core/definitions/partitions/definition/time_window.py#L58) |
| Argo Workflows | withItems/withParam разворачиваются в несколько parallel steps. | Fan-out запускает пользовательские работы; ни границы SQL range, ни отсутствие пропусков/пересечений из него не следуют. | [argo:steps.go:602](https://github.com/argoproj/argo-workflows/blob/5642521e2a1349803c097a00db2b08f0b96c10ca/workflow/controller/steps.go#L602) |
| Airflow SQL operator | SQLExecuteQueryOperator исполняет заданный SQL; split_statements разбивает текст SQL на statements. | Название split здесь особенно обманчиво: это несколько SQL statements, а не разбиение строк одной таблицы. Пользовательская оркестрация может запускать заранее заданные predicates. | [airflow:sql.py:475](https://github.com/apache/airflow/blob/fcdbf2d5b38f2775e39e00dd7e3ad9e25d5b838e/providers/common/sql/src/airflow/providers/common/sql/operators/sql.py#L475) |
| SQLMesh | Scheduler рассчитывает недостающие date intervals моделей и исполняет backfill по DAG с ограниченным числом concurrent queries. | Интервалы относятся к выполнению модели в warehouse. Это не универсальный splitter произвольной исходной БД и не автоматическое обещание единого MVCC snapshot. | [sqlmesh:scheduler.py:102](https://github.com/TobikoData/sqlmesh/blob/ad2377ea9f065b26d957207958a024aaebc0b70f/sqlmesh/core/scheduler.py#L102) |
| rclone | Multi-thread copy делит один object/file на byte chunks; calculateNumChunks связывает file size и chunkSize, worker читает byte range и пишет chunk. | Подходит для файлов/объектов. Разрез байтов может пересекать запись/row group; перенос SQL-семантики требует отдельного format-aware reader. | [rclone:multithread.go:67](https://github.com/rclone/rclone/blob/1c3e432c3fdbf98c5800c0390496d6944bac53b0/fs/operations/multithread.go#L67), [rclone:multithread.go:114](https://github.com/rclone/rclone/blob/1c3e432c3fdbf98c5800c0390496d6944bac53b0/fs/operations/multithread.go#L114) |
| rsync | sum_sizes_sqroot определяет block length и размеры checksums для delta-передачи файла. | Checksum blocks уменьшают передачу совпадающих частей; это другой механизм, чем параллельный SQL snapshot. Не считать количество блоков количеством независимых readers. | [rsync:generator.c:703](https://github.com/RsyncProject/rsync/blob/7c7e1bee860a97b1357ec29eac9656768beb2e63/generator.c#L703) |

Практический вывод: повторяемый шаблон «очередь независимых work units + offsets +
ограниченный параллелизм» полезен Transferia, но не решает центральную задачу:
кто строит **полное непересекающееся покрытие таблицы в согласованной версии данных**.
Её нельзя передать общему task scheduler без DB-specific boundary planner и
протокола snapshot/CDC. Файловые, брокерные и orchestration splits могут быть
полноценными и эффективными в собственной предметной области, оставаясь
неприменимыми как непосредственная замена SQL range splitting.

## Дополнительная проверка прямых ingestion-компонентов

У этих продуктов отдельно проверены доступные SQL-пути и места делегирования.
Слова «batch», «partition» и «parallel» интерпретируются по конкретному коду,
а не по названию платформы. CloudQuery — смешанная поставка; остальные строки
здесь относятся к указанным открытым компонентам и конкретным commit.

| Кто | Подтверждённый механизм | Граница и эффективность | Код |
|---|---|---|---|
| CloudQuery | CLI создаёт PluginClient и вызывает Sync RPC источника. README различает открытые framework/SDK/CLI/часть integrations и закрытые integrations; публикует отдельные архивы ранее открытого кода. На проверенном commit plugins/source содержит airtable, aws, bitbucket, hackernews, square, test, typeform, xkcd — PostgreSQL/MySQL source там отсутствуют. | Это подтверждение делегирования, не реконструкция SQL splitter. SQL-плагины отсутствуют именно в проверенном дереве; не утверждается, что они отсутствуют в продукте либо все версии закрыты. Отдельный legacy архив README не скачивался. | [cloudquery:README.md:53](https://github.com/cloudquery/cloudquery/blob/caff7202a009b24df229f35db0bf5c115272f6fb/README.md#L53), [cloudquery:sync_v3.go:366](https://github.com/cloudquery/cloudquery/blob/caff7202a009b24df229f35db0bf5c115272f6fb/cli/cmd/sync_v3.go#L366) |
| Mage PostgreSQL / generic SQL ingestion | PostgreSQL наследует SQL Source. load_data получает query._offset/_limit, считает offset = initial_offset + fetch_limit × loops и выдаёт subbatches. __fetch_rows добавляет LIMIT/OFFSET; ORDER BY формируется из bookmark properties, key properties и unique constraints, с fallback на доступные сортируемые колонки. | Положительно подтверждена offset-pagination и возможность передать границы subbatch. Отдельный parallel dispatcher и единый snapshot этим файлом не подтверждены. Большие OFFSET дороги; изменение данных/неуникальный порядок опасны без дополнительного snapshot-контракта. Это не keyset range partitioning. | [mage:__init__.py:36](https://github.com/mage-ai/mage-ai/blob/1912c297f3556913c56181a96e8c5253e0883bf0/mage_integrations/mage_integrations/sources/postgresql/__init__.py#L36), [mage:base.py:202](https://github.com/mage-ai/mage-ai/blob/1912c297f3556913c56181a96e8c5253e0883bf0/mage_integrations/mage_integrations/sources/sql/base.py#L202), [mage:base.py:273](https://github.com/mage-ai/mage-ai/blob/1912c297f3556913c56181a96e8c5253e0883bf0/mage_integrations/mage_integrations/sources/sql/base.py#L273), [mage:base.py:313](https://github.com/mage-ai/mage-ai/blob/1912c297f3556913c56181a96e8c5253e0883bf0/mage_integrations/mage_integrations/sources/sql/base.py#L313) |
| Meltano | SingerRunner запускает tap.invoke_async и target.invoke_async, передаёт stdout tap в stdin target. Планирование SQL чтения принадлежит выбранному tap; Meltano runner его не выводит из table metadata. | Возможности нельзя приписывать всему Meltano по одному tap. Для generic SQLStream и конкретного tap-postgres есть отдельный source-аудит. Runner-параллелизм и SQL range splitter — разные слои. | [meltano:singer.py:61](https://github.com/meltano/meltano/blob/f91d2e4f0d93065d7ca50ff775124382f4db2d0e/src/meltano/core/runner/singer.py#L61), [meltano:singer.py:87](https://github.com/meltano/meltano/blob/f91d2e4f0d93065d7ca50ff775124382f4db2d0e/src/meltano/core/runner/singer.py#L87) |
| Singer: протокол, SDK и tap — разные сущности | Generic Singer SDK SQLStream.get_records явно отвергает непустой partition context, затем выполняет build_query и отдаёт строки. Отдельный singer-io/tap-postgres sync_view читает server-side cursor с itersize; sync_table/full-table и incremental разобраны в ADDITIONAL-CODE. | Нельзя утверждать, что Singer в целом «не умеет» splitting: это расширяемая экосистема taps. Проверенный default SQLStream не превращает partition context в SQL predicates. Cursor fetch batch — не самостоятельный parallel table shard. | [singer-sdk:stream.py:267](https://github.com/meltano/sdk/blob/ce2e8f2b7fc145fd610965c713794839d137fe1d/singer_sdk/sql/stream.py#L267), [singer-sdk:stream.py:271](https://github.com/meltano/sdk/blob/ce2e8f2b7fc145fd610965c713794839d137fe1d/singer_sdk/sql/stream.py#L271), [singer-postgres:full_table.py:44](https://github.com/singer-io/tap-postgres/blob/821f1cfdc0942f3ca839d4cf5abacb18858ac64a/tap_postgres/sync_strategies/full_table.py#L44) |
| Grouparoo PostgreSQL query import | getRows принимает limit/offset от вызывающего слоя, дописывает их к пользовательскому scheduleOptions.query и исполняет connection.query. | Это конкретная OFFSET pagination, а не автоматические PK boundaries. Функция не добавляет ORDER BY и не показывает единый snapshot; стабильность порядка должна следовать из пользовательского query/вызывающего протокола. Размер страницы не подтверждает число concurrent readers. | [grouparoo:getRows.ts:20](https://github.com/grouparoo/grouparoo/blob/a453a40b16fbb8a9ab7a32b16a003fecb89e7bde/plugins/@grouparoo/postgres/src/lib/query-import/getRows.ts#L20) |
