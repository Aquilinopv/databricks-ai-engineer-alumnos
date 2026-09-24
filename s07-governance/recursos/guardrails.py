"""Controles didácticos: no sustituyen ACLs, moderación ni auditoría de producción."""
import json
import re
from contextlib import contextmanager

class Blocked(ValueError):
    pass

PATTERNS = (r'ignora (todas |las )?(instrucciones|reglas)', r'ignore (all |previous )?instructions',
            r'(revela|muestra|imprime).{0,30}(token|secreto|system prompt)', r'drop\s+table')
SECRET = re.compile(r'\bdapi[a-zA-Z0-9]{20,}\b|\bBearer\s+[a-zA-Z0-9._-]{20,}', re.I)
EMAIL = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b')

def check_text(text, limit=2000):
    if not isinstance(text, str) or not text.strip():
        raise Blocked('entrada_vacia')
    if len(text) > limit:
        raise Blocked('limite_caracteres')
    if any(re.search(p, text, re.I) for p in PATTERNS):
        raise Blocked('patron_inyeccion_conocido')
    if SECRET.search(text):
        raise Blocked('posible_credencial')
    return text

def moderate(text, classifier):
    """classifier devuelve el formato nativo safe / unsafe + categorías de Llama Guard."""
    try:
        result = classifier(text).strip()
    except Exception as exc:
        raise Blocked('moderacion_no_disponible') from exc
    lines = result.splitlines()
    if not lines or lines[0].strip().lower() not in {'safe', 'unsafe'}:
        raise Blocked('moderacion_formato_invalido')
    if lines[0].strip().lower() == 'safe' and len(lines) != 1:
        raise Blocked('moderacion_formato_invalido')
    if lines[0].strip().lower() == 'unsafe':
        raise Blocked('moderacion_unsafe')
    return {'verdict': 'safe'}

def sanitize_output(text):
    if SECRET.search(text):
        raise Blocked('salida_con_credencial')
    return EMAIL.sub('[EMAIL OCULTO]', text)

@contextmanager
def tool_boundary(namespace, allowed, max_calls=4):
    """Notebook monohilo. Conserva validación S05; inspecciona tool result ANTES del LLM."""
    original = namespace['ejecutar_herramienta']
    calls = []
    def guarded(name, args):
        if name not in allowed:
            raise Blocked('tool_no_permitida')
        if len(calls) >= max_calls:
            raise Blocked('limite_tools')
        check_text(json.dumps(args, ensure_ascii=False), limit=2000)
        calls.append(name)
        result = original(name, args)
        serialized = json.dumps(result, ensure_ascii=False, default=str)
        check_text(serialized, limit=16000)
        return result
    namespace['ejecutar_herramienta'] = guarded
    try:
        yield calls
    finally:
        namespace['ejecutar_herramienta'] = original

def protected_ask(question, namespace, allowed, classifier=None):
    check_text(question)
    if classifier is not None:
        moderate(question, classifier)
    with tool_boundary(namespace, allowed) as calls:
        response = namespace['preguntar'](question)
        text = namespace['texto_respuesta'](response)
        if classifier is not None:
            moderate(text, classifier)
        return {'answer': sanitize_output(text), 'tool_calls': list(calls),
                'moderation': 'llama_guard_executed' if classifier else 'not_configured',
                'limitation': 'No devuelve audit crudo al usuario; no sanea trazas internas S05.'}
