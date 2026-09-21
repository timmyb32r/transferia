"""Optional database SQL-work counters; these are not database CPU measurements."""
from setup import connect

def capture():
    result={}
    for side in ('source','pg'):
        try:
            with connect(side) as c:
                row=c.execute("SELECT sum(calls),sum(total_exec_time),sum(rows),sum(shared_blks_hit),sum(shared_blks_read),sum(temp_blks_written),sum(wal_bytes) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND userid=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND query NOT ILIKE '%pg_stat_%'").fetchone()
                result[side]=dict(zip(('calls','execution_ms','rows','shared_blocks_hit','shared_blocks_read','temp_blocks_written','wal_bytes'),[float(v or 0) for v in row]))
        except Exception as e:result[side]={'unavailable':type(e).__name__}
    return result

def delta(before,after):
    return {side:({k:after[side][k]-v for k,v in vals.items()} if 'unavailable' not in vals and 'unavailable' not in after[side] else {'unavailable':True}) for side,vals in before.items()}
