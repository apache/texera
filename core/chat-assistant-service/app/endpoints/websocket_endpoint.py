# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import logging
import uuid
from typing import Any, Dict
from agents import trace

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.agent_as_tools.service import AgentService


async def _new_session(
    sessions: Dict[str, AgentService],
    rid_to_session: Dict[str, str],
) -> str:
    """Create a fresh chat session bound to the current websocket’s maps."""
    sid = str(uuid.uuid4())

    # open one OpenTelemetry span for the lifetime of this session
    span_cm = trace(f"Texera Workflow Builder [{sid}]")
    span_cm.__enter__()  # keep it open

    service = AgentService(
        session_id=sid,
        rid_registry=rid_to_session,
        trace_cm=span_cm,  # pass the context manager
    )
    sessions[sid] = service
    return sid


router = APIRouter()
logger = logging.getLogger("websocket_endpoint")


async def handle_create_session(ws, payload, sessions, rid_to_session):
    sid = await _new_session(sessions, rid_to_session)
    await ws.send_json({"type": "CreateSessionResponse", "sessionId": sid})


async def handle_heartbeat(
    ws: WebSocket, payload: Dict[str, Any], sessions, rid_to_session, **_
) -> None:
    """Simply echo a heartbeat acknowledgement; extra args are ignored."""
    await ws.send_json({"type": "HeartBeatResponse"})


async def handle_operator_schema_response(
    ws: WebSocket,
    payload: Dict[str, Any],
    sessions: Dict[str, AgentService],
    rid_to_session: Dict[str, str],
    **_,
) -> None:
    rid = payload.get("requestId")
    sid = rid_to_session.pop(rid, None)
    if not sid:
        return
    service = sessions.get(sid)
    if not service:
        return
    schema = payload.get("schema")
    fut = service._schema_futures.get(rid)
    if fut and not fut.done():
        fut.set_result(schema)


async def handle_add_operator_response(
    ws: WebSocket, payload: Dict[str, Any], sessions, rid_to_session, **_
) -> None:
    rid = payload.get("requestId")
    sid = rid_to_session.pop(rid, None)
    if not sid:
        return
    service = sessions.get(sid)
    if not service:
        return
    status = payload.get("status")
    fut = service._add_op_futures.get(rid)
    if fut and not fut.done():
        fut.set_result(status)


async def handle_chat_user_message(
    ws: WebSocket, payload: Dict[str, Any], sessions, rid_to_session, **_
) -> None:
    sid = payload.get("sessionId")
    if not sid or sid not in sessions:
        await ws.send_json({"type": "Error", "error": "Invalid sessionId"})
        return
    service = sessions[sid]
    msg = payload.get("message", "").strip()
    if not msg:
        await ws.send_json({"type": "Error", "error": "Empty chat message."})
        return

    async def _stream():
        # stream deltas and then completion
        async for delta in service.stream_chat(ws, msg):
            await ws.send_json({"type": "ChatStreamResponseEvent", "delta": delta})
        await ws.send_json({"type": "ChatStreamResponseComplete"})

    asyncio.create_task(_stream())


_HANDLER_MAP = {
    "CreateSessionRequest": handle_create_session,  # NEW
    "HeartBeatRequest": handle_heartbeat,
    "OperatorSchemaResponse": handle_operator_schema_response,
    "AddOperatorAndLinksResponse": handle_add_operator_response,
    "ChatUserMessageRequest": handle_chat_user_message,
}


@router.websocket("/chat-assistant")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    sessions: Dict[str, AgentService] = {}
    rid_to_session: Dict[str, str] = {}
    logger.info("WS connected")

    try:
        while True:
            payload = await ws.receive_json()
            req_type = payload.get("type")
            handler = _HANDLER_MAP.get(req_type)
            if not handler:
                await ws.send_json(
                    {"type": "Error", "error": f"Unknown type {req_type}"}
                )
                continue

            await handler(ws, payload, sessions, rid_to_session)
    except WebSocketDisconnect:
        logger.info("WS disconnected")
    finally:
        for s in sessions.values():
            if hasattr(s, "close"):
                s.close()
        sessions.clear()
        rid_to_session.clear()
