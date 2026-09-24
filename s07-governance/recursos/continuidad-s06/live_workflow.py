"""Serie docente aislada: captura → referencia → evaluación → cambio → rollback.
No actualiza ni borra espacios. Cada versión es un clon acumulativo conservado.
"""
import copy, datetime, importlib.util, json, re, uuid
from pathlib import Path

def read(path):
    return json.loads(Path(path).read_text())

def write(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
    temporary.replace(path)

class LiveSeries:
    def __init__(self, base, series_id, workspace=None):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}',series_id):
            raise ValueError('serie_id: 1–64 letras, números, guion o guion bajo')
        self.base=Path(base);self.root=self.base/'series'/series_id;self.series_id=series_id
        # Una instancia independiente evita dirigir el evaluador al histórico docente.
        spec=importlib.util.spec_from_file_location('benchmark_'+uuid.uuid4().hex,Path(__file__).with_name('benchmark.py'))
        self.b=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.b)
        self.b.ROOT=self.root;self.w=workspace
        if workspace is not None:self.b.client=lambda:workspace
    def workspace(self):
        if self.w is None:self.w=self.b.client()
        return self.w
    def state(self):
        return read(self.root/'series.json')
    def _event(self,state,action,**details):
        state['events'].append({'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'action':action,**details})
        write(self.root/'series.json',state)
    def _config(self,space):
        return self.workspace().genie.get_space(space,include_serialized_space=True).as_dict()
    def _fingerprint(self,cfg):
        # Títulos y timestamps no alteran el agente; instrucciones, tablas y warehouse sí.
        data=json.loads(cfg['serialized_space'])
        # Genie une los fragmentos de content al exportar: comparar el texto exacto, no su partición.
        for item in data.get('instructions',{}).get('text_instructions',[]):
            item['content']=[''.join(item['content'])]
        return self.b.sha({'warehouse_id':cfg['warehouse_id'],'config':data})
    def _verified(self,version):
        state=self.state();v=state['versions'][version];cfg=self._config(v['space_id'])
        if self._fingerprint(cfg)!=v['config_sha256']:
            raise ValueError('La configuración cambió fuera de esta serie: '+version+'. Conserva evidencia e inicia otra serie.')
        return cfg
    def _clone(self,cfg,version):
        result=self.workspace().genie.create_space(warehouse_id=cfg['warehouse_id'],serialized_space=cfg['serialized_space'],title=f'S07 {self.series_id} {version}',description='Clon de práctica en vivo. Original y evidencias conservados.')
        # Guardar el ID inmediatamente permite recuperar el clon si falla la lectura posterior.
        write(self.root/'reports'/f'{version}-created.json',{'space_id':result.space_id})
        return self._config(result.space_id)
    def prepare(self,source_space_id):
        if not source_space_id.strip():raise ValueError('Falta source_space_id')
        if self.root.exists():raise ValueError('Serie existente: reanuda desde su estado o usa otro serie_id; no se sobrescribe')
        cfg=self._config(source_space_id);serialized=json.loads(cfg['serialized_space']);bench=serialized.get('benchmarks',{})
        if len(bench.get('questions',[]))!=20:raise ValueError('Esta práctica exige 20 benchmarks')
        for q in bench['questions']:
            if len(q.get('answer',[]))!=1 or q['answer'][0].get('format')!='SQL':raise ValueError('Cada caso requiere un SQL de referencia')
        write(self.root/'source-config.json',cfg)
        clone=self._clone(cfg,'V0')
        if self._fingerprint(clone)!=self._fingerprint(cfg):raise ValueError('El clon V0 difiere del origen; revisar snapshots')
        write(self.root/'ianbal-v0-config.json',clone);write(self.root/'benchmark20-original.json',bench)
        state={'series_id':self.series_id,'source_space_id':source_space_id,'active_version':'V0','versions':{'V0':{'space_id':clone['space_id'],'parent':None,'change':'Baseline sin cambios','config_sha256':self._fingerprint(clone)}},'events':[]}
        self._event(state,'prepare',version='V0');return state
    def freeze(self):
        self._verified('V0')
        if (self.root/'gold20.json').exists():raise ValueError('Gold ya congelado; no repetir')
        self.b.freeze();self._event(self.state(),'freeze',dataset_sha256=read(self.root/'gold20.json')['dataset_sha256'])
    def report(self,version):
        return read(self.root/'reports'/f'{version}.json')
    def _complete(self,version):
        p=self.root/'reports'/f'{version}.json'
        if not p.exists() or len(read(p).get('cases',[]))!=20 or not (self.root/'reports'/f'{version}-verified.json').exists():
            raise ValueError('Primero termina y verifica los 20 casos de '+version)
    def evaluate(self,version):
        cfg=self._verified(version)
        if not (self.root/'gold20.json').exists():raise ValueError('Primero congelar el gold')
        if (self.root/'reports'/f'{version}.json').exists():raise ValueError('Ya hay evidencia, incluso parcial; no se sobrescribe. Usa otra serie.')
        self._event(self.state(),'evaluation_started',version=version)
        # El evaluador conserva sus 20 casos, juez, trazas, precheck SQL y guardado incremental.
        self.b.run(version,cfg['space_id'])
        self._verified(version)  # Detecta una edición de UI durante la corrida.
        write(self.root/'reports'/f'{version}-verified.json',{'config_stable':True,'config_sha256':self._fingerprint(cfg)})
        self._event(self.state(),'evaluation_finished',version=version,run_id=self.report(version)['run_id'])
        return self.report(version)
    def create_next(self,version,change):
        if not re.fullmatch(r'V[1-9][0-9]*',version):raise ValueError('Usa V1, V2, V3…')
        if not change.strip():raise ValueError('Escribe una hipótesis y cambio concretos')
        state=self.state();parent='V'+str(int(version[1:])-1)
        if version in state['versions'] or (self.root/'reports'/f'{version}-created.json').exists():raise ValueError('Versión existente; no se sobrescribe')
        self._complete(parent);cfg=self._verified(parent);updated=copy.deepcopy(cfg)
        data=json.loads(updated['serialized_space'])
        # Agrega SOLO el delta. Clonar el padre conserva los cambios V1 al crear V2.
        instructions=data.setdefault('instructions',{}).setdefault('text_instructions',[])
        # El protocolo Genie permite un solo contenedor; los deltas se agregan a content.
        # Preservar su ID evita convertir cada cambio pedagógico en otro objeto inválido.
        if instructions:
            instructions[0]['content'].append('\n\n'+change.strip())
        else:
            instructions.append({'id':uuid.uuid4().hex,'content':[change.strip()]})
        updated['serialized_space']=json.dumps(data)
        clone=self._clone(updated,version)
        if self._fingerprint(clone)!=self._fingerprint(updated):raise ValueError('Clon diferente del cambio solicitado')
        write(self.root/'reports'/f'{version}-creation.json',{'parent':parent,'change':change,'config':clone})
        state['versions'][version]={'space_id':clone['space_id'],'parent':parent,'change':change,'config_sha256':self._fingerprint(clone)}
        state['active_version']=version;self._event(state,'create',version=version,parent=parent);return state['versions'][version]
    def compare(self):
        self.b.compare()
        result=read(self.root/'reports/comparison.json')
        # El comparador histórico contiene límites de aquel ensayo; esta serie registra su propia procedencia.
        result['series_id']=self.series_id
        result['limitations']=['Known benchmark; not held-out generalization','Latency is descriptive, not a controlled performance benchmark']
        captured={}
        for version in self.state()['versions']:
            path=self.root/'reports'/f'{version}.json'
            if path.exists():captured[version]=read(path).get('runner_sha256_at_start')
        result['runner_sha256_at_start_by_version']=captured
        missing=[v for v,h in captured.items() if not h]
        if missing:result['limitations'].append('Runner hash not captured for: '+', '.join(missing))
        write(self.root/'reports/comparison.json',result)
        return result
    def activate(self,version):
        # Rollback de la demo: cambia el puntero; no modifica el original ni un endpoint.
        self._verified(version);state=self.state();previous=state['active_version'];state['active_version']=version
        self._event(state,'activate',previous=previous,version=version);return self.link(version)
    def link(self,version):
        return self.workspace().config.host.rstrip('/')+'/genie/rooms/'+self.state()['versions'][version]['space_id']
    def summary(self):
        if not (self.root/'series.json').exists():return []
        rows=[]
        for version,v in self.state()['versions'].items():
            p=self.root/'reports'/f'{version}.json';report=read(p) if p.exists() else None
            rows.append({'version':version,'parent':v['parent'],'space_id':v['space_id'],'active':version==self.state()['active_version'],'status':'capturada' if report is None else 'evaluada' if (self.root/'reports'/f'{version}-verified.json').exists() else 'parcial/no verificada',**(self.b.summarize(report['cases']) if report else {}),'run_id':report.get('run_id') if report else None})
        return rows
