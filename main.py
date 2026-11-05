from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import subprocess
import json
import os
import asyncio

app = FastAPI(title="Meu AI MCP Broker - Extreme Optimized")
SECRET_KEY = os.getenv("BROKER_SECRET", "my-super-secret-key")

MAX_CONCURRENT_PROCESSES = 5 
QUEUE_TIMEOUT = 240 

request_queue = asyncio.Queue()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_PROCESSES)

class ToolRequest(BaseModel):
    server_name: str
    tool_name: str
    arguments: dict

async def worker():
    while True:
        task_data = await request_queue.get()
        req, future = task_data
        
        async with semaphore:
            try:
                server_map = {
                    "digikala": "iran-mcp-digikala",
                    "divar": "iran-mcp-divar",
                    "torob": "iran-mcp-torob"
                }
                uvx_package = server_map.get(req.server_name)
                
                if not uvx_package:
                    future.set_exception(HTTPException(status_code=400, detail="Server not supported"))
                    continue

                rpc_request = {
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {"name": req.tool_name, "arguments": req.arguments},
                    "id": 1
                }
                
                process = subprocess.Popen(
                    ["uvx", uvx_package],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                stdout, stderr = process.communicate(input=json.dumps(rpc_request) + "\n", timeout=60)
                
                result_found = False
                for line in stdout.splitlines():
                    if line.strip().startswith("{"):
                        response = json.loads(line)
                        future.set_result({"result": response.get("result", {})})
                        result_found = True
                        break
                
                if not result_found:
                    future.set_exception(HTTPException(status_code=500, detail=f"Invalid response: {stderr[:100]}"))

            except subprocess.TimeoutExpired:
                future.set_exception(HTTPException(status_code=504, detail="Tool timeout"))
            except Exception as e:
                future.set_exception(HTTPException(status_code=500, detail=str(e)))
            finally:
                request_queue.task_done()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(worker())

@app.post("/call-tool")
async def call_tool(req: ToolRequest, authorization: str = Header(None)):
    if not authorization or authorization != f"Bearer {SECRET_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    loop = asyncio.get_running_loop()
    future = loop.create_future()
    
    await request_queue.put((req, future))
    
    try:
        result = await asyncio.wait_for(future, timeout=QUEUE_TIMEOUT)
        return result
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timeout")

@app.get("/")
def health_check():
    return {"status": "Meu AI Broker is Alive & Optimized!", "queue_size": request_queue.qsize()}
