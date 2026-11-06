"""
Meu AI MCP Broker
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Meu AI MCP Broker", version="0.2.0")

SERVER_MAP: dict[str, dict[str, Any]] = {
    "digikala": {
        "package": "digikala-mcp@0.5.2",
        "tools": {
            "dk_search", "dk_find_cheapest", "dk_best_for_budget", "dk_suggest",
            "dk_filters", "dk_categories", "dk_category_products", "dk_brand_products",
            "dk_seller", "dk_deals", "dk_best_sellers", "dk_fresh_search", "dk_product",
            "dk_price_history", "dk_reviews", "dk_questions", "dk_similar", "dk_compare",
            "dk_shortlist", "dk_installments", "dk_plus_plans", "dk_gold_prices", "dk_location",
        },
    },
    "divar": {
        "package": "divar-mcp@0.2.4",
        "tools": {
            "dv_categories", "dv_filters", "dv_filter_options", "dv_suggest", "dv_parse_query",
            "dv_market", "dv_appraise", "dv_status", "dv_cities", "dv_districts", "dv_neighbours",
            "dv_locate", "dv_place_search", "dv_place_mapping", "dv_boundary", "dv_post", "dv_posts",
            "dv_compare", "dv_seller", "dv_dealer", "dv_similar", "dv_search", "dv_count", "dv_cheapest",
            "dv_best_for_budget", "dv_find_deals", "dv_new_since", "dv_export", "dv_nearby", "dv_watch",
            "dv_price_trend",
        },
    },
    "torob": {
        "package": "torob-mcp@0.1.3",
        "tools": {
            "tb_cheapest", "tb_compare", "tb_best_value", "tb_deal_check", "tb_basket", "tb_product",
            "tb_sellers", "tb_stores", "tb_presence", "tb_variants", "tb_price_chart", "tb_price_history",
            "tb_similar", "tb_product_url", "tb_products_batch", "tb_search", "tb_suggest", "tb_filters",
            "tb_category", "tb_brands", "tb_trends", "tb_offers", "tb_cities", "tb_shop",
            "tb_find_shops", "tb_shop_products",
        },
    },
}

EXPECTED_API_KEY = os.getenv("BROKER_API_KEY", "").strip()
REQUEST_TIMEOUT = max(5, min(int(os.getenv("MCP_REQUEST_TIMEOUT", "45")), 120))
STARTUP_TIMEOUT = max(10, min(int(os.getenv("MCP_STARTUP_TIMEOUT", "30")), 90))
MAX_BODY_BYTES = 256 * 1024
TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,160}$")

class ToolRequest(BaseModel):
    server_name: str = Field(min_length=1, max_length=80)
    tool_name: str = Field(min_length=1, max_length=160)
    arguments: dict[str, Any] = Field(default_factory=dict)

@dataclass
class RunningServer:
    process: asyncio.subprocess.Process
    lock: asyncio.Lock
    initialized: bool = False

running: dict[str, RunningServer] = {}
running_guard = asyncio.Lock()
request_counter = 0

def auth_ok(authorization: str | None) -> bool:
    if not EXPECTED_API_KEY:
        return True
    return authorization == f"Bearer {EXPECTED_API_KEY}"

def json_line(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

async def read_json_message(reader: asyncio.StreamReader, timeout: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        remaining = max(0.1, deadline - time.monotonic())
        line = await asyncio.wait_for(reader.readline(), timeout=remaining)
        if not line:
            raise RuntimeError("MCP process closed stdout before returning a response")
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError("MCP process returned non-JSON stdout") from exc
        if isinstance(message, dict):
            return message

async def start_server(server_name: str) -> RunningServer:
    config = SERVER_MAP[server_name]
    package = config["package"]
    process = await asyncio.wait_for(
        asyncio.create_subprocess_exec(
            "uvx", "--quiet", package,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        ),
        timeout=STARTUP_TIMEOUT,
    )
    server = RunningServer(process=process, lock=asyncio.Lock())
    try:
        response = await rpc(server, "initialize", {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "Meu AI Broker", "version": app.version},
        })
        if response.get("error"):
            raise RuntimeError(str(response["error"]))
        await send_message(server, {"jsonrpc": "2.0", "method": "notifications/initialized"})
        server.initialized = True
        return server
    except Exception:
        await stop_server(server)
        raise

async def get_server(server_name: str) -> RunningServer:
    async with running_guard:
        existing = running.get(server_name)
        if existing and existing.process.returncode is None:
            return existing
        server = await start_server(server_name)
        running[server_name] = server
        return server

async def send_message(server: RunningServer, message: dict[str, Any]) -> None:
    if not server.process.stdin:
        raise RuntimeError("MCP process stdin is unavailable")
    server.process.stdin.write(json_line(message))
    await server.process.stdin.drain()

async def rpc(server: RunningServer, method: str, params: dict[str, Any]) -> dict[str, Any]:
    global request_counter
    request_counter += 1
    request_id = request_counter
    await send_message(server, {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
    while True:
        message = await read_json_message(server.process.stdout, REQUEST_TIMEOUT)  # type: ignore[arg-type]
        if message.get("id") == request_id:
            return message

async def stop_server(server: RunningServer) -> None:
    process = server.process
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=3)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()

async def call_tool(request: ToolRequest) -> dict[str, Any]:
    server_name = request.server_name.strip().lower()
    tool_name = request.tool_name.strip()
    config = SERVER_MAP.get(server_name)
    if not config:
        raise HTTPException(status_code=403, detail="MCP server is not allowed")
    if not TOOL_NAME_RE.fullmatch(tool_name) or tool_name not in config["tools"]:
        raise HTTPException(status_code=403, detail="MCP tool is not allowed for this server")

    server = await get_server(server_name)
    async with server.lock:
        try:
            response = await asyncio.wait_for(
                rpc(server, "tools/call", {"name": tool_name, "arguments": request.arguments}),
                timeout=REQUEST_TIMEOUT,
            )
        except asyncio.TimeoutError as exc:
            await stop_server(server)
            running.pop(server_name, None)
            raise HTTPException(status_code=504, detail="MCP tool timed out") from exc
        except Exception as exc:
            await stop_server(server)
            running.pop(server_name, None)
            raise HTTPException(status_code=502, detail=f"MCP process failed: {exc}") from exc

    if response.get("error"):
        return {"error": response["error"], "server_name": server_name, "tool_name": tool_name}
    return response.get("result", response)

@app.get("/")
async def health() -> dict[str, Any]:
    return {
        "status": "Meu AI Broker is Alive & Optimized!",
        "servers": list(SERVER_MAP),
        "queue_size": sum(1 for item in running.values() if item.lock.locked()),
    }

@app.post("/call-tool")
async def call_tool_endpoint(
    request: ToolRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if not auth_ok(authorization):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await call_tool(request)

@app.on_event("shutdown")
async def shutdown() -> None:
    await asyncio.gather(*(stop_server(server) for server in list(running.values())), return_exceptions=True)
    running.clear()
