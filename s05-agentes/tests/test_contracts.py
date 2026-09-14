"""Offline contract/loop tests. No Spark, credentials or network calls."""
import ast
import json
import socket
from pathlib import Path
from types import SimpleNamespace as NS
from contextlib import nullcontext
import unittest
import uuid
from jsonschema import validate

SOURCE = Path(__file__).parents[1] / 'notebook.py'
class FakeTrace:
    def trace(self, **kwargs):
        return lambda f: f
    def start_span(self, **kwargs):
        return nullcontext(NS(set_inputs=lambda x:None,set_outputs=lambda x:None))
class FakeAgent:
    def create_text_output_item(self, **kw): return kw
class FakeResponse:
    def __init__(self, **kw): self.__dict__.update(kw)
class FakeMessage:
    def __init__(self, calls=None, content='Listo'): self.tool_calls,self.content=calls,content
    def model_dump(self, **kwargs): return {'role':'assistant','content':self.content}

class ContractTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(SOURCE.read_text())
        subset = ast.Module(body=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in {'validar_llamada','ejecutar_herramienta','AgenteNeptuno','es_error_dns','texto_respuesta'}], type_ignores=[])
        self.env = {'socket':socket,'mlflow':FakeTrace(),'validate':validate,'ResponsesAgent':FakeAgent,'ResponsesAgentRequest':NS,'ResponsesAgentResponse':FakeResponse,
          'json':json,'uuid':uuid,'SYSTEM':'test','ENDPOINT':'fake','CATEGORIAS':['Bebidas'],'ANIOS':[2026],
          'UC_MAP':{'ventas':'c.s.ventas_categoria'},'EXTRA_HANDLERS':{},
          'TOOL_DEFINITIONS':{'ventas':{'function':{'parameters':{'type':'object','properties':{'p_categoria':{'type':'string'},'p_anio':{'type':'integer'}},'required':['p_categoria','p_anio']}}}}}
        exec(compile(subset,str(SOURCE),'exec'),self.env)
    def test_missing_argument_rejected(self):
        with self.assertRaises(Exception): self.env['validar_llamada']('ventas',{'p_categoria':'Bebidas'})
    def test_unknown_tool_rejected(self):
        with self.assertRaises(ValueError): self.env['validar_llamada']('delete',{})
    def test_extra_argument_rejected(self):
        with self.assertRaises(Exception): self.env['validar_llamada']('ventas',{'p_categoria':'Bebidas','p_anio':2026,'sql':'DROP'})
    def test_unavailable_year_rejected(self):
        with self.assertRaises(ValueError): self.env['validar_llamada']('ventas',{'p_categoria':'Bebidas','p_anio':1900})
    def test_tool_budget_stops_repeating_model(self):
        call = NS(id='a',function=NS(name='ventas',arguments='{"p_categoria":"Bebidas","p_anio":2026}'))
        self.env['llm']=NS(chat=NS(completions=NS(create=lambda **kw:NS(choices=[NS(message=FakeMessage([call]))]))))
        executed=[]
        self.env['ejecutar_herramienta']=lambda n,a: executed.append((n,a)) or {'ok':True}
        r=self.env['AgenteNeptuno']().predict(NS(input=[{'role':'user','content':'test'}]))
        self.assertEqual(4,len(executed));self.assertEqual('limite',r.custom_outputs['estado'])
    def test_empty_answer_without_tools_is_supported(self):
        self.env['llm']=NS(chat=NS(completions=NS(create=lambda **kw:NS(choices=[NS(message=FakeMessage(content='¿Qué año?'))]))))
        r=self.env['AgenteNeptuno']().predict(NS(input=[{'role':'user','content':'ventas'}]))
        self.assertEqual([],r.custom_outputs['herramientas'])
    def test_only_dns_is_classified_as_external_unavailable(self):
        nested=RuntimeError('outer')
        nested.__cause__=socket.gaierror(-3,'Temporary failure in name resolution')
        self.assertTrue(self.env['es_error_dns'](nested))
        self.assertFalse(self.env['es_error_dns'](ValueError('tool schema invalid')))
    def test_response_text_from_mlflow_dictionary_items(self):
        response=NS(output=[{'type':'message','content':[{'type':'output_text','text':'¿Qué año?'}]}])
        self.assertEqual('¿Qué año?',self.env['texto_respuesta'](response))
    def test_response_text_from_model_items(self):
        item=NS(model_dump=lambda **kw:{'type':'message','content':[{'type':'output_text','text':'No existen costos.'}]})
        self.assertEqual('No existen costos.',self.env['texto_respuesta'](NS(output=[item])))
    def test_every_executable_cell_has_teaching_predecessor(self):
        cells=SOURCE.read_text().split('# COMMAND ----------')
        for i,cell in enumerate(cells):
            if '# MAGIC %md' not in cell and cell.strip():
                self.assertGreater(i,0)
                self.assertIn('# MAGIC %md',cells[i-1],f'Cell {i} lacks teaching markdown')
    def test_widgets_do_not_validate_catalog(self):
        cells=SOURCE.read_text().split('# COMMAND ----------')
        widget=next(c for c in cells if 'dbutils.widgets.text("catalogo"' in c)
        self.assertIn('"catalogo", ""',widget)
        self.assertNotIn('raise ',widget)

if __name__ == '__main__': unittest.main()
