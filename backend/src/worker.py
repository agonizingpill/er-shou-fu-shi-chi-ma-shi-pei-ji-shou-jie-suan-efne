import os,time,pathlib,logging
import redis
from .database import Session
from .models import FitAssessment,Measurement,FitProfile,Remeasurement
from .common import lock,rows,get,dump
from .fit_model import predict
q=redis.from_url(os.environ['REDIS_URL'],decode_responses=True,socket_connect_timeout=1,socket_timeout=4)
heartbeat=pathlib.Path(os.environ.get('HEARTBEAT_PATH','/artifacts/resale-heartbeat'))
def recover():
 try:
  with Session() as s:
   lock(s);ids=[r.id for r in rows(s,Remeasurement) if r.state=='queued']
   if ids:q.rpush('resale:remeasure',*[str(i) for i in ids]);s.commit()
   logging.info('remeasure recovery requeued %s',ids)
 except Exception:logging.exception('remeasure recovery failed, will retry next cycle')
def fit_job(s,item):
 x=s.get(FitAssessment,int(item[1]))
 if not x or x.state!='queued':return
 try:
  m=next(m for m in rows(s,Measurement) if m.garment_id==x.garment_id);p=get(s,FitProfile,x.profile_id);x.result=predict(dump(m),dump(p));x.state='ready'
 except Exception as e:x.state='failed';x.error=str(e)[:500];logging.exception('fit failed')
def remeasure_job(s,item):
 from . import remeasure_service
 x=s.get(Remeasurement,int(item[1]))
 if not x or x.state!='queued':return
 try:
  p=get(s,FitProfile,x.profile_id);x=remeasure_service.dispose(s,x,predict(dump(x),dump(p)))
 except Exception as e:x.state='failed';x.error=str(e)[:500];logging.exception('remeasure failed')
def run(item):
 job=fit_job if item[0]=='resale:fit' else remeasure_job
 with Session() as s:
  lock(s);job(s,item);s.commit()
def main():
 recover()
 while True:
  heartbeat.write_text(str(time.time()))
  try:item=q.blpop(['resale:fit','resale:remeasure'],timeout=2)
  except redis.RedisError:
   logging.warning('Redis interrupted, retrying');time.sleep(1);recover();continue
  if not item:continue
  try:run(item)
  except Exception:
   logging.exception('job session failed, requeuing pending remeasurements')
   recover()
if __name__=='__main__':main()
