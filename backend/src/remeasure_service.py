from .models import *
from .common import *

def submit(s,d):
 lock(s);f=get(s,Fitting,d['fitting_id']);g=garment(s,f.garment_id);get(s,FitProfile,f.profile_id)
 if f.state!='reserved' or g.state!='held':fail('只有当前有效留衣可提交复量，已成交的请先按退货处理')
 if any(r.fitting_id==f.id and r.state in ['queued','failed'] for r in rows(s,Remeasurement)):fail('该留衣已有复量单，请按原单重试，不另开新记录')
 x=Remeasurement(fitting_id=f.id,garment_id=g.id,profile_id=f.profile_id,chest=d['chest'],shoulder=d['shoulder'],length=d['length'],elasticity=d['elasticity'],cut=d['cut'])
 s.add(x);f.state='rechecking';s.flush();return x
def retry(s,id):
 lock(s);x=get(s,Remeasurement,id);f=get(s,Fitting,x.fitting_id);g=garment(s,x.garment_id)
 if x.state!='failed':fail('只有计算失败的复量单可重试')
 if f.state!='rechecking' or g.state!='held':fail('原留衣已处置，复量单不可再重试')
 x.state='queued';x.error='';s.flush();return x
def dispose(s,x,predict):
 f=get(s,Fitting,x.fitting_id);g=garment(s,x.garment_id)
 if f.state!='rechecking' or g.state!='held':
  x.state='failed';x.error='原留衣已被处置，复量结果不再生效';return x
 x.result=predict;x.label=predict['label']
 if predict['label']=='合身':x.state='fitted';f.state='reserved'
 else:
  x.state='loose' if predict['label']=='偏松' else 'tight'
  f.state='cancelled';g.state='listed'
 return x
