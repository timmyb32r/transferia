"""Read-only source activity observer; observed concurrency is a lower bound."""
import threading,time
from setup import connect
class Observer:
    def __init__(self,dataset):
        self.dataset=dataset;self.stop_event=threading.Event();self.ready=threading.Event();self.samples=[];self.error=None
        self.thread=threading.Thread(target=self.loop,daemon=True)
    def loop(self):
        try:
            with connect('source') as c:
                self.ready.set()
                while not self.stop_event.is_set():
                    rows=c.execute("SELECT pid,state,wait_event_type,left(query,3000) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active' AND (query ILIKE %s OR query ILIKE %s)",('%fair21%'+self.dataset+'%','%fair21%"'+self.dataset+'"%')).fetchall()
                    self.samples.append({'monotonic':time.monotonic(),'queries':[{'pid':r[0],'state':r[1],'wait':r[2],'query':r[3]} for r in rows]})
                    self.stop_event.wait(.5)
        except Exception as e: self.error=type(e).__name__;self.ready.set()
    def start(self): self.thread.start();self.ready.wait(20)
    def stop(self): self.stop_event.set();self.thread.join(20)
