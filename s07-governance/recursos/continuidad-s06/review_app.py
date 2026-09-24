"""Crea formulario privado para trazas S06 existentes; NO registra valoración humana."""
import json, os
from pathlib import Path
def create(experiment_id, trace_ids):
 if not experiment_id or not trace_ids:raise ValueError('Indica experimento y trazas propias ya existentes')
 p=Path(__file__).resolve().parent/'reports'/'review-session.json'
 if p.exists():raise ValueError('Ya hay sesión; reutiliza su URL o utiliza otra carpeta')
 import mlflow
 from databricks.sdk import WorkspaceClient
 from mlflow.genai import label_schemas as s, labeling
 remote=os.environ.get('S07_REMOTE') or os.environ.get('DATABRICKS_RUNTIME_VERSION')
 profile=os.environ.get('DATABRICKS_CONFIG_PROFILE','databricks-ai-engineer-aws')
 w=WorkspaceClient() if remote else WorkspaceClient(profile=profile)
 os.environ['DATABRICKS_HOST']=w.config.host
 mlflow.set_tracking_uri('databricks' if remote else 'databricks://'+profile)
 mlflow.set_experiment(experiment_id=experiment_id)
 definitions=[('s07_aprobacion_v1','feedback',s.InputCategorical(options=['Aprobada','Requiere corrección','No evaluable']),'¿La respuesta cumple la pregunta y la referencia?'),('s07_evidencia_v1','feedback',s.InputText(max_length=4000),'Cita SQL, fila, documento o criterio comprobado. No inventes evidencia.'),('s07_correccion_v1','expectation',s.InputText(max_length=4000),'Escribe la corrección esperada o indica que no requiere cambios.')]
 names=[]
 for name,typ,inp,instruction in definitions:
  try:s.get_label_schema(name)
  except Exception as e:
   # Solo crear si no existe; otros fallos deben mostrarse.
   if not ('NOT_FOUND' in str(e) or 'does not exist' in str(e) or 'not found' in str(e).lower()):raise
   s.create_label_schema(name=name,type=typ,input=inp,title=name,instruction=instruction)
  names.append(name)
 session=labeling.create_labeling_session(name='S07-continuidad-S06-revision-con-evidencia',assigned_users=[],label_schemas=[s.EXPECTED_RESPONSE,*names])
 session.add_traces([mlflow.get_trace(t) for t in trace_ids])
 evidence={'url':session.url,'mlflow_run_id':session.mlflow_run_id,'labeling_session_id':session.labeling_session_id,'trace_ids':trace_ids,'schemas':[s.EXPECTED_RESPONSE,*names],'status':'pending_human','human_assessments_created_by_script':0}
 p=Path(__file__).resolve().parent/'reports'/'review-session.json';p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(evidence,indent=2));print(json.dumps(evidence,indent=2));return evidence
if __name__=='__main__':
 import argparse
 parser=argparse.ArgumentParser();parser.add_argument('--experiment-id',required=True);parser.add_argument('--trace-id',action='append',required=True);args=parser.parse_args();create(args.experiment_id,args.trace_id)
