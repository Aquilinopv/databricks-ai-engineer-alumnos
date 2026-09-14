"""MCP externo público, cliente fuera de Databricks. No usa credenciales."""
import argparse
import asyncio
from datetime import datetime, timezone
import importlib.metadata
import json
from pathlib import Path
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

async def run():
    async with streamablehttp_client('https://learn.microsoft.com/api/mcp') as (read,write,_):
        async with ClientSession(read,write) as session:
            await session.initialize()
            listed = await session.list_tools()
            names = [t.name for t in listed.tools]
            if 'microsoft_docs_search' not in names:
                raise RuntimeError('Contrato remoto cambió: inspecciona tools/list')
            result = await session.call_tool('microsoft_docs_search',{'query':'Azure Databricks Unity Catalog functions'})
            if result.isError or not result.content:
                raise RuntimeError('MCP tools/call no devolvió evidencia válida')
            return {'status':'PASS','execution_location':'LOCAL_PC','endpoint':'https://learn.microsoft.com/api/mcp',
                'executed_at':datetime.now(timezone.utc).isoformat(),'mcp_version':importlib.metadata.version('mcp'),
                'tools':names,'query':'Azure Databricks Unity Catalog functions',
                'result':result.model_dump(mode='json')}

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=Path('reports/mcp-external-local.json'))
    args=parser.parse_args()
    evidence=asyncio.run(asyncio.wait_for(run(),timeout=45))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in evidence.items() if k!='result'},ensure_ascii=False,indent=2))
    print('Evidencia:',args.output.resolve())
