from __future__ import annotations

import asyncio
import base64
import json
import logging
import mimetypes
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
    cache_video_from_bytes,
)

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "ws://127.0.0.1:9094/ws"


def _load_hermes_env_defaults() -> None:
    """Load ~/.hermes/.env into os.environ if the launcher did not do it."""
    try:
        from hermes_constants import get_hermes_home
        env_path = get_hermes_home() / ".env"
    except Exception:
        env_path = Path.home() / ".hermes" / ".env"

    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return
    except UnicodeDecodeError:
        try:
            lines = env_path.read_text(encoding="latin-1").splitlines()
        except Exception:
            return
    except Exception:
        return

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_hermes_env_defaults()


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _bridge_ws_url(config: Any = None) -> str:
    extra = getattr(config, "extra", {}) or {}
    return (
        os.getenv("WECHAT_BRIDGE_WS_URL")
        or extra.get("bridge_ws_url")
        or extra.get("ws_url")
        or DEFAULT_WS_URL
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _mime_to_type(mime: str, name: str = "") -> MessageType:
    lower_mime = (mime or "").lower()
    guessed, _ = mimetypes.guess_type(name or "")
    if not lower_mime and guessed:
        lower_mime = guessed.lower()
    lower_name = (name or "").lower()
    if lower_mime.startswith("image/") or lower_name.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")):
        return MessageType.PHOTO
    if lower_mime.startswith("audio/") or lower_name.endswith((".mp3", ".wav", ".amr", ".flac", ".aac", ".ogg", ".m4a", ".opus")):
        return MessageType.AUDIO
    if lower_mime.startswith("video/") or lower_name.endswith((".mp4", ".avi", ".mov", ".mkv", ".webm")):
        return MessageType.VIDEO
    return MessageType.DOCUMENT


def _cache_media_bytes(data: bytes, mime: str, name: str) -> Tuple[str, MessageType]:
    msg_type = _mime_to_type(mime, name)
    ext = Path(name or "").suffix
    if not ext:
        ext = mimetypes.guess_extension(mime or "") or ".bin"

    if msg_type == MessageType.PHOTO:
        return cache_image_from_bytes(data, ext=ext), msg_type
    if msg_type == MessageType.AUDIO:
        return cache_audio_from_bytes(data, ext=ext), msg_type
    if msg_type == MessageType.VIDEO:
        return cache_video_from_bytes(data, ext=ext), msg_type
    return cache_document_from_bytes(data, name or f"wechat_{uuid.uuid4().hex}{ext}"), msg_type


def _decode_media_data(raw: str) -> bytes:
    value = str(raw or "").strip()
    if "," in value and value.lower().startswith("data:"):
        value = value.split(",", 1)[1]
    return base64.b64decode(value)


def _clean_media_caption(caption: Optional[str]) -> str:
    text = str(caption or "").strip()
    if not text:
        return ""

    # Hermes BasePlatformAdapter passes markdown image alt text as caption.
    # Generated-image tools commonly emit placeholders like image_1, image_2;
    # those should not become visible WeChat text bubbles before the image.
    if re.fullmatch(r"(?i)(?:image|img|图片|图像)[_\-\s]*\d+", text):
        return ""
    return text


class WeChatBridgeAdapter(BasePlatformAdapter):
    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("wechat"))
        self.ws_url = _bridge_ws_url(config)
        self.reconnect_enabled = _truthy(os.getenv("WECHAT_BRIDGE_RECONNECT") or "true")
        self.reconnect_delay_seconds = float(os.getenv("WECHAT_BRIDGE_RECONNECT_DELAY") or "2")
        self.send_wait_seconds = float(os.getenv("WECHAT_BRIDGE_SEND_WAIT_SECONDS") or "5")
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._connect_task: Optional[asyncio.Task] = None
        self._send_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._stopping = False

    @property
    def name(self) -> str:
        return "WeChat"

    async def connect(self) -> bool:
        if self._connect_task and not self._connect_task.done():
            return True
        self._stopping = False
        self._connect_task = asyncio.create_task(self._connect_loop())
        return True

    async def disconnect(self) -> None:
        self._stopping = True
        self._ready.clear()
        if self._connect_task and not self._connect_task.done():
            self._connect_task.cancel()
            try:
                await self._connect_task
            except asyncio.CancelledError:
                pass
        self._connect_task = None
        await self._close_ws()
        self._mark_disconnected()

    async def _close_ws(self) -> None:
        ws = self._ws
        self._ws = None
        if ws is not None and not ws.closed:
            try:
                await ws.close()
            except Exception:
                pass
        session = self._session
        self._session = None
        if session is not None and not session.closed:
            try:
                await session.close()
            except Exception:
                pass

    async def _connect_loop(self) -> None:
        while not self._stopping:
            try:
                timeout = aiohttp.ClientTimeout(total=None, connect=15)
                self._session = aiohttp.ClientSession(timeout=timeout)
                logger.info("WeChat bridge connecting: %s", self.ws_url)
                async with self._session.ws_connect(
                    self.ws_url,
                    heartbeat=None,
                    autoping=False,
                    receive_timeout=None,
                    max_msg_size=100 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    self._ready.set()
                    self._mark_connected()
                    logger.info("WeChat bridge connected: %s", self.ws_url)
                    await self._receive_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._stopping:
                    logger.warning("WeChat bridge disconnected: %s", exc)
            finally:
                self._ready.clear()
                await self._close_ws()
                if not self._stopping:
                    self._mark_disconnected()

            if self._stopping or not self.reconnect_enabled:
                break
            await asyncio.sleep(max(self.reconnect_delay_seconds, 0.5))

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    frame = json.loads(msg.data)
                except json.JSONDecodeError:
                    logger.warning("WeChat bridge received invalid JSON frame")
                    continue
                await self._handle_frame(frame)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def _handle_frame(self, frame: Dict[str, Any]) -> None:
        event = frame.get("event")
        if event == "ping":
            await self._send_frame("pong", {})
            return
        if event == "pong":
            return
        if event != "inbound_message":
            logger.debug("WeChat bridge ignored event: %s", event)
            return

        payload = frame.get("payload") if isinstance(frame.get("payload"), dict) else {}
        await self._dispatch_inbound(payload)

    async def _dispatch_inbound(self, payload: Dict[str, Any]) -> None:
        if not self._message_handler:
            return

        chat_id = str(payload.get("from") or payload.get("chat_id") or "").strip()
        if not chat_id:
            logger.warning("WeChat inbound message missing chat id")
            return

        text = str(payload.get("content") or payload.get("text") or "").strip()
        is_group = bool(payload.get("isGroup") or payload.get("is_group"))
        sender_id = str(payload.get("senderId") or payload.get("sender_id") or chat_id)
        sender_name = str(payload.get("senderName") or payload.get("fromName") or payload.get("from_name") or sender_id)
        chat_name = str(payload.get("groupName") or payload.get("chatName") or payload.get("fromName") or chat_id)
        message_id = str(payload.get("msg_id") or payload.get("message_id") or "") or None

        media_urls, media_types, message_type = self._extract_media(payload)
        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type="group" if is_group else "dm",
            user_id=sender_id,
            user_name=sender_name,
            message_id=message_id,
        )
        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            raw_message=payload,
            message_id=message_id,
            media_urls=media_urls,
            media_types=media_types,
        )
        await self.handle_message(event)

    def _extract_media(self, payload: Dict[str, Any]) -> Tuple[List[str], List[str], MessageType]:
        media = payload.get("media")
        if not isinstance(media, dict):
            return [], [], MessageType.TEXT

        mime = str(media.get("mime") or media.get("contentType") or media.get("type") or "")
        name = str(media.get("name") or media.get("filename") or "")
        try:
            if media.get("data"):
                local_path, msg_type = _cache_media_bytes(_decode_media_data(media["data"]), mime, name)
                return [local_path], [msg_type.value], msg_type
        except Exception as exc:
            logger.warning("WeChat bridge failed to decode media payload: %s", exc)
            return [], [], MessageType.TEXT

        path_value = str(media.get("local_path") or media.get("path") or "").strip()
        if path_value and os.path.exists(path_value):
            msg_type = _mime_to_type(mime, name or path_value)
            return [path_value], [msg_type.value], msg_type

        url_value = str(media.get("url") or media.get("mediaUrl") or path_value).strip()
        if url_value:
            msg_type = _mime_to_type(mime, name or url_value)
            return [url_value], [msg_type.value], msg_type

        return [], [], MessageType.TEXT

    async def _wait_ready(self) -> bool:
        if self._ws is not None and not self._ws.closed:
            return True
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=max(self.send_wait_seconds, 0.1))
        except asyncio.TimeoutError:
            return False
        return self._ws is not None and not self._ws.closed

    async def _send_frame(self, event: str, payload: Dict[str, Any]) -> bool:
        if not await self._wait_ready():
            return False
        frame = {
            "direction": "hermes_to_bridge",
            "event": event,
            "payload": payload,
            "ts": _now_ms(),
        }
        try:
            async with self._send_lock:
                assert self._ws is not None
                await self._ws.send_str(json.dumps(frame, ensure_ascii=False))
            return True
        except Exception as exc:
            logger.warning("WeChat bridge send failed: %s", exc)
            return False

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        ok = await self._send_frame("outbound_text", {"to": chat_id, "text": content or "", "type": "text"})
        return SendResult(success=ok, message_id=str(_now_ms()) if ok else None, error=None if ok else "WeChat bridge is not connected", retryable=not ok)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_media(chat_id, image_url, caption, audio_as_voice=False)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media(chat_id, image_path, caption, audio_as_voice=False)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media(chat_id, file_path, caption, audio_as_voice=False)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media(chat_id, audio_path, caption, audio_as_voice=True)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media(chat_id, video_path, caption, audio_as_voice=False)

    async def _send_media(self, chat_id: str, media_url: str, caption: Optional[str], *, audio_as_voice: bool) -> SendResult:
        payload = {
            "to": chat_id,
            "type": "media",
            "text": _clean_media_caption(caption),
            "mediaUrl": media_url,
        }
        if audio_as_voice:
            payload["audioAsVoice"] = True
        ok = await self._send_frame("outbound_media", payload)
        return SendResult(success=ok, message_id=str(_now_ms()) if ok else None, error=None if ok else "WeChat bridge is not connected", retryable=not ok)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        chat_id = str(chat_id or "")
        return {"name": chat_id, "type": "group" if chat_id.endswith("@chatroom") else "dm"}


def check_requirements() -> bool:
    return True


def validate_config(config) -> bool:
    return bool(_bridge_ws_url(config))


def is_connected(config) -> bool:
    return bool(_bridge_ws_url(config))


def _env_enablement() -> dict | None:
    ws_url = _bridge_ws_url()
    seed = {"bridge_ws_url": ws_url}
    home = os.getenv("WECHAT_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("WECHAT_HOME_CHANNEL_NAME", home),
        }
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    ws_url = _bridge_ws_url(pconfig)
    frames: List[Dict[str, Any]] = []
    media_files = media_files or []
    for media in media_files:
        frames.append({"event": "outbound_media", "payload": {"to": chat_id, "type": "media", "text": "", "mediaUrl": media}})
    if message:
        frames.append({"event": "outbound_text", "payload": {"to": chat_id, "text": message, "type": "text"}})
    if not frames:
        return {"error": "WeChat standalone send: empty message"}

    try:
        timeout = aiohttp.ClientTimeout(total=20, connect=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(ws_url, heartbeat=None, autoping=False) as ws:
                for item in frames:
                    frame = {
                        "direction": "hermes_to_bridge",
                        "event": item["event"],
                        "payload": item["payload"],
                        "ts": _now_ms(),
                    }
                    await ws.send_str(json.dumps(frame, ensure_ascii=False))
        return {"success": True, "message_id": str(_now_ms())}
    except Exception as exc:
        return {"error": f"WeChat standalone send failed: {exc}"}


def register(ctx):
    ctx.register_platform(
        name="wechat",
        label="WeChat",
        adapter_factory=lambda cfg: WeChatBridgeAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[],
        install_hint="Start aibot with HermesBridge listening on port 9094.",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="WECHAT_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="WECHAT_ALLOWED_USERS",
        allow_all_env="WECHAT_ALLOW_ALL_USERS",
        max_message_length=4000,
        platform_hint=(
            "You are chatting through WeChat via a local aibot bridge. "
            "Use concise plain text. You may return media or file paths when "
            "appropriate; the bridge can deliver images, documents, audio, "
            "video, cards, and emoji markers back to WeChat."
        ),
    )
