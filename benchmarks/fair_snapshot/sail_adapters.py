"""Benchmark-only Sail adapters for the immutable five-column fixture.

ADBC buffers one PostgreSQL binary COPY partition into Arrow; its C/C++ driver is upstream,
not a Rust reimplementation. Both Sail source variants use the identical sink:
ADBC COPY+commit per partition for PG; synchronous Arrow HTTPS inserts for CH.
No row is converted to Python scalars. These adapters are not native Sail SQL
connectors and provide no cross-partition atomicity or retry deduplication.
"""
import itertools
from dataclasses import dataclass
import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceArrowWriter, InputPartition, WriterCommitMessage

COLUMNS=['id','group_id','amount','label','payload']

class RangePartition(InputPartition):
    def __init__(self, index, low, high):
        self.index,self.low,self.high=index,low,high

class AdbcReader(DataSourceReader):
    def __init__(self, options):
        self.options=dict(options)

    def partitions(self):
        n,p=int(self.options['rows']),int(self.options['parts'])
        return [RangePartition(i,1+i*n//p,1+(i+1)*n//p) for i in range(p)]

    def read(self, partition):
        from adbc_driver_postgresql import dbapi
        with dbapi.connect(self.options['uri']) as conn:
            with conn.cursor() as cursor:
                cursor.adbc_statement.set_options(**{'adbc.postgresql.batch_size_hint_bytes':'8388608'})
                query=f"SELECT id,group_id,amount,label,payload FROM {self.options['table']} WHERE id >= {partition.low} AND id < {partition.high}"
                print(f'SAIL_ADBC_PARTITION index={partition.index} low={partition.low} high={partition.high}',flush=True)
                cursor.execute(query)
                # Stock Sail 0.7.1 blocks on its bounded source queue while
                # retaining the GIL. A streaming Python sink can then deadlock.
                # One ready batch per partition avoids that boundary, at the
                # explicit cost of whole-partition buffering and consolidation.
                table=cursor.fetch_arrow_table().combine_chunks()
                batches=table.to_batches()
                if len(batches)!=1:
                    raise ValueError('Buffered workaround requires one batch per fixture partition')
                print(f'SAIL_ADBC_BUFFERED rows={table.num_rows} bytes={table.nbytes} batches={len(batches)}',flush=True)
                yield from batches

class AdbcSource(DataSource):
    @classmethod
    def name(cls):
        return 'benchmark_adbc'

    def schema(self):
        from adbc_driver_postgresql import dbapi
        with dbapi.connect(self.options['uri']) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT id,group_id,amount,label,payload FROM {self.options['table']} LIMIT 0")
                return cursor.fetch_record_batch().schema

    def reader(self, schema):
        return AdbcReader(self.options)

@dataclass
class CommittedRows(WriterCommitMessage):
    rows: int

class ArrowSinkWriter(DataSourceArrowWriter):
    def __init__(self, options):
        self.options=dict(options)

    def write(self, iterator):
        iterator=iter(iterator)
        first=next(iterator,None)
        if first is None:
            return CommittedRows(0)
        rows=0
        def checked_batches():
            nonlocal rows
            for batch in itertools.chain([first],iterator):
                if batch.schema.names != COLUMNS or any(column.null_count for column in batch.columns):
                    raise ValueError('Unexpected fixture fields or null values')
                rows+=batch.num_rows
                yield batch
        if self.options['route']=='pg-pg':
            from adbc_driver_postgresql import dbapi
            with dbapi.connect(self.options['uri']) as conn:
                with conn.cursor() as cursor:
                    reader=pa.RecordBatchReader.from_batches(first.schema,checked_batches())
                    affected=cursor.adbc_ingest(self.options['table'],reader,mode='append',db_schema_name='fair21')
                    if affected != rows:
                        raise ValueError('ADBC inserted-row count mismatch')
                conn.commit()
        else:
            import clickhouse_connect
            client=clickhouse_connect.get_client(host=self.options['host'],port=8443,username='user1',password=self.options['password'],database='db1',secure=True,verify=True,ca_cert='/cert/RootCA.crt',compress='lz4',settings={'async_insert':0})
            try:
                for batch in checked_batches():
                    for start in range(0,batch.num_rows,65536):
                        client.insert_arrow(self.options['table'],pa.Table.from_batches([batch.slice(start,65536)]))
            finally:
                client.close()
        print(f'SAIL_SINK_PARTITION_COMMITTED rows={rows}',flush=True)
        return CommittedRows(rows)

    def commit(self,messages):
        rows=sum(message.rows for message in messages)
        if rows != int(self.options['rows']):
            raise ValueError(f'Incomplete fixture: committed {rows} rows')
        print(f'SAIL_SINK_ALL_COMMITTED rows={rows} partitions={len(messages)}',flush=True)

class ArrowSink(DataSource):
    @classmethod
    def name(cls):
        return 'benchmark_arrow_sink'

    def writer(self,schema,overwrite):
        if overwrite:
            raise ValueError('Only append to precreated empty fixtures is allowed')
        return ArrowSinkWriter(self.options)
