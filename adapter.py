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
from urllib.parse import quote, unquote, urlparse

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


def _bridge_media_settings(config: Any = None) -> Tuple[str, str]:
    extra = getattr(config, "extra", {}) or {}
    base_url = str(
        os.getenv("WECHAT_BRIDGE_MEDIA_URL")
        or extra.get("bridge_media_url")
        or extra.get("bridge_media_base_url")
        or ""
    ).strip().rstrip("/")
    token = str(
        os.getenv("WECHAT_BRIDGE_MEDIA_TOKEN")
        or extra.get("bridge_media_token")
        or ""
    ).strip()
    return base_url, token


def _is_http_url(value: str) -> bool:
    return str(value or "").strip().lower().startswith(("http://", "https://"))


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


_LOCAL_ATTACHMENT_EXTS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
    ".mp4", ".avi", ".mov", ".mkv", ".webm", ".3gp",
    ".mp3", ".wav", ".amr", ".flac", ".aac", ".ogg", ".m4a", ".opus",
    ".md", ".txt", ".csv", ".pdf", ".epub",
    ".zip", ".rar", ".7z",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".apk", ".ipa",
)


def _coerce_media_entry(media: Any) -> Tuple[str, bool]:
    if isinstance(media, (list, tuple)):
        media_path = str(media[0] if media else "").strip()
        is_voice = bool(media[1]) if len(media) > 1 else False
    else:
        media_path = str(media or "").strip()
        is_voice = False
    return media_path, is_voice


def _normalize_local_attachment_path(raw_path: str) -> Optional[str]:
    path = str(raw_path or "").strip().strip("`\"'")
    if not path:
        return None
    if path.startswith("file://"):
        path = path[len("file://"):]
    expanded = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(expanded):
        return None
    if Path(expanded).suffix.lower() not in _LOCAL_ATTACHMENT_EXTS:
        return None
    return expanded


def _extract_local_attachment_paths(content: str) -> Tuple[List[str], str]:
    """Detect bare/backticked local document paths not covered by Hermes core."""
    text = str(content or "")
    if not text:
        return [], text

    ext_part = "|".join(re.escape(ext.lstrip(".")) for ext in _LOCAL_ATTACHMENT_EXTS)
    patterns = [
        re.compile(
            r"(?P<quote>[`\"'])(?P<path>(?:file://)?(?:~/|/)[^`\"']+?\.(?:"
            + ext_part
            + r"))(?P=quote)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?<![/:\w.])(?P<path>(?:file://)?(?:~/|/)[^\s`\"',;:)\]}]+?\.(?:"
            + ext_part
            + r"))\b",
            re.IGNORECASE,
        ),
    ]

    found: List[str] = []
    seen = set()
    spans: List[Tuple[int, int]] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            normalized = _normalize_local_attachment_path(match.group("path"))
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            found.append(normalized)
            spans.append(match.span())

    if not found:
        return [], text

    cleaned_parts: List[str] = []
    pos = 0
    for start, end in sorted(spans):
        if start < pos:
            continue
        cleaned_parts.append(text[pos:start])
        pos = end
    cleaned_parts.append(text[pos:])
    cleaned = "".join(cleaned_parts)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return found, cleaned


def _build_media_payload(chat_id: str, media_path: str, caption: Optional[str], *, audio_as_voice: bool) -> Dict[str, Any]:
    raw_path = str(media_path or "").strip()
    parsed = urlparse(raw_path) if _is_http_url(raw_path) else None
    name = unquote(os.path.basename((parsed.path if parsed else raw_path).rstrip("/"))) or "attachment"
    mime, _ = mimetypes.guess_type(name)
    payload: Dict[str, Any] = {
        "to": chat_id,
        "type": "media",
        "text": _clean_media_caption(caption),
        "mediaUrl": media_path,
        "name": name,
        "filename": name,
        "mime": mime or "application/octet-stream",
    }
    if audio_as_voice:
        payload["audioAsVoice"] = True
    return payload


async def _upload_media_file(
    file_path: str,
    config: Any = None,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[str]:
    """Upload a Hermes-local file to AiBot and return its signed URL."""
    base_url, token = _bridge_media_settings(config)
    if not base_url or not token:
        return None

    raw_path = str(file_path or "").strip()
    if raw_path.startswith("file://"):
        raw_path = raw_path[len("file://"):]
    source = Path(os.path.expanduser(raw_path))
    if not source.is_file():
        raise FileNotFoundError(f"media file does not exist: {source}")

    content_type = mimetypes.guess_type(source.name)[0] or "application/octet-stream"
    headers = {
        "X-AiBot-Media-Token": token,
        "X-AiBot-Media-Filename": quote(source.name, safe=""),
        "Content-Type": content_type,
    }
    own_session = session is None or session.closed
    upload_session = session
    if own_session:
        upload_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180, connect=15))

    try:
        with source.open("rb") as file:
            async with upload_session.put(
                f"{base_url}/api/bridge-media",
                data=file,
                headers=headers,
            ) as response:
                if response.status != 200:
                    detail = (await response.text())[:240]
                    raise RuntimeError(f"AiBot media upload failed ({response.status}): {detail}")
                result = await response.json()
        media_url = str(result.get("url") or "").strip()
        if not media_url:
            raise RuntimeError("AiBot media upload response did not include a URL")
        return media_url
    finally:
        if own_session and upload_session is not None:
            await upload_session.close()


def _resolve_send_chat_id(chat_id: str) -> str:
    """Keep bare send_message(target='wechat') inside the active WeChat chat."""
    target = str(chat_id or "").strip()
    configured_home = os.getenv("WECHAT_HOME_CHANNEL", "").strip()
    if not target or not configured_home or target != configured_home:
        return target

    current_home = _current_wechat_home_channel()
    current_chat_id = (current_home or {}).get("chat_id", "").strip()
    if current_chat_id and current_chat_id != target:
        logger.info(
            "WeChat bridge remapped configured home send to active chat: %s -> %s",
            target,
            current_chat_id,
        )
        return current_chat_id
    return target


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
        self._dispatch_task: Optional[asyncio.Task] = None
        self._dispatch_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._send_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._stopping = False

    @property
    def name(self) -> str:
        return "WeChat"

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        # Hermes Gateway 在启动及重连时都会传入 is_reconnect；
        # 微信桥接自身统一由 _connect_loop 处理，冷启动与重连无需区别逻辑。
        del is_reconnect
        if self._connect_task and not self._connect_task.done():
            return True
        self._stopping = False
        self._connect_task = asyncio.create_task(self._connect_loop())
        return True

    async def disconnect(self) -> None:
        self._stopping = True
        self._ready.clear()
        if self._dispatch_task and not self._dispatch_task.done():
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                pass
        self._dispatch_task = None
        self._clear_dispatch_queue()
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
        self._ensure_dispatch_task()
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

    def _ensure_dispatch_task(self) -> None:
        if self._dispatch_task and not self._dispatch_task.done():
            return
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    def _clear_dispatch_queue(self) -> None:
        while True:
            try:
                self._dispatch_queue.get_nowait()
                self._dispatch_queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def _dispatch_loop(self) -> None:
        while True:
            payload = await self._dispatch_queue.get()
            try:
                await self._dispatch_inbound(payload)
            finally:
                self._dispatch_queue.task_done()

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
        self._ensure_dispatch_task()
        self._dispatch_queue.put_nowait(payload)

    async def _dispatch_inbound(self, payload: Dict[str, Any]) -> None:
        if not self._message_handler:
            return

        chat_id = str(payload.get("from") or payload.get("chat_id") or "").strip()
        if not chat_id:
            logger.warning("WeChat inbound message missing chat id")
            return

        text = str(payload.get("content") or payload.get("text") or "").strip()
        route_type = str(payload.get("routeType") or payload.get("chatType") or "").strip().lower()
        is_group = (
            route_type == "group"
            or bool(payload.get("isGroup") or payload.get("is_group"))
            or chat_id.startswith(("group__", "group:"))
            or chat_id.endswith("@chatroom")
        )
        sender_id = str(payload.get("senderId") or payload.get("sender_id") or chat_id)
        sender_name = str(payload.get("senderName") or payload.get("fromName") or payload.get("from_name") or sender_id)
        chat_name = str(
            payload.get("groupName")
            or payload.get("chatName")
            or payload.get("displayThreadId")
            or payload.get("fromName")
            or chat_id
        )
        message_id = str(payload.get("msg_id") or payload.get("message_id") or "") or None

        media_urls, media_types, message_type = await self._extract_media(payload)
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

    async def _extract_media(self, payload: Dict[str, Any]) -> Tuple[List[str], List[str], MessageType]:
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
            if _is_http_url(url_value):
                try:
                    local_path, downloaded_type = await self._download_media_url(url_value, mime, name)
                    return [local_path], [downloaded_type.value], downloaded_type
                except Exception as exc:
                    logger.warning("WeChat bridge failed to download media URL: %s", exc)
                    return [], [], MessageType.TEXT
            msg_type = _mime_to_type(mime, name or url_value)
            return [url_value], [msg_type.value], msg_type

        return [], [], MessageType.TEXT

    async def _download_media_url(
        self,
        media_url: str,
        mime: str,
        name: str,
    ) -> Tuple[str, MessageType]:
        parsed = urlparse(media_url)
        url_name = unquote(os.path.basename(parsed.path.rstrip("/")))
        filename = name or url_name or "wechat_media"
        session = self._session
        own_session = session is None or session.closed
        if own_session:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180, connect=15))

        try:
            async with session.get(media_url) as response:
                if response.status != 200:
                    detail = (await response.text())[:240]
                    raise RuntimeError(f"HTTP {response.status}: {detail}")
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > 200 * 1024 * 1024:
                    raise RuntimeError("media exceeds the 200 MiB download limit")
                chunks: List[bytes] = []
                total_size = 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    total_size += len(chunk)
                    if total_size > 200 * 1024 * 1024:
                        raise RuntimeError("media exceeds the 200 MiB download limit")
                    chunks.append(chunk)
                data = b"".join(chunks)
                if not data:
                    raise RuntimeError("media response was empty")
                response_mime = response.headers.get("Content-Type", "").split(";", 1)[0].strip()
                local_path, msg_type = _cache_media_bytes(data, response_mime or mime, filename)
                logger.debug("Downloaded bridge media to Hermes cache: %s", local_path)
                return local_path, msg_type
        finally:
            if own_session and session is not None:
                await session.close()

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
        chat_id = _resolve_send_chat_id(chat_id)
        text = content or ""
        media_files, cleaned_text = self.extract_media(text)
        local_files, cleaned_text = self.extract_local_files(cleaned_text)
        attachment_files, cleaned_text = _extract_local_attachment_paths(cleaned_text)

        pending_media: List[Tuple[str, bool]] = []
        pending_media.extend(media_files or [])
        pending_media.extend((path, False) for path in (local_files or []))
        pending_media.extend((path, False) for path in (attachment_files or []))

        last_message_id: Optional[str] = None
        for media_path, is_voice in pending_media:
            result = await self._send_media(chat_id, media_path, None, audio_as_voice=is_voice)
            if not result.success:
                return result
            last_message_id = result.message_id

        cleaned_text = cleaned_text.strip()
        if cleaned_text:
            ok = await self._send_frame("outbound_text", {"to": chat_id, "text": cleaned_text, "type": "text"})
            return SendResult(success=ok, message_id=str(_now_ms()) if ok else last_message_id, error=None if ok else "WeChat bridge is not connected", retryable=not ok)

        if pending_media:
            return SendResult(success=True, message_id=last_message_id or str(_now_ms()))

        ok = await self._send_frame("outbound_text", {"to": chat_id, "text": text, "type": "text"})
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
        target_url = str(media_url or "").strip()
        if target_url and not _is_http_url(target_url):
            try:
                uploaded_url = await _upload_media_file(target_url, self.config, self._session)
            except Exception as exc:
                logger.warning("WeChat bridge media upload failed, using original path: %s", exc)
            else:
                if uploaded_url:
                    target_url = uploaded_url
                    logger.debug("Uploaded Hermes media through AiBot HTTP service: %s", media_url)

        payload = _build_media_payload(chat_id, target_url, caption, audio_as_voice=audio_as_voice)
        ok = await self._send_frame("outbound_media", payload)
        return SendResult(success=ok, message_id=str(_now_ms()) if ok else None, error=None if ok else "WeChat bridge is not connected", retryable=not ok)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        chat_id = str(chat_id or "")
        is_group = chat_id.startswith(("group__", "group:")) or chat_id.endswith("@chatroom")
        return {"name": chat_id, "type": "group" if is_group else "dm"}


def check_requirements() -> bool:
    return True


def validate_config(config) -> bool:
    return bool(_bridge_ws_url(config))


def is_connected(config) -> bool:
    return bool(_bridge_ws_url(config))


def _current_wechat_home_channel() -> Optional[Dict[str, str]]:
    """Use the active WeChat chat as the implicit send_message home target."""
    try:
        from gateway.session_context import get_session_env
    except Exception:
        return None

    platform = get_session_env("HERMES_SESSION_PLATFORM", "").strip().lower()
    chat_id = get_session_env("HERMES_SESSION_CHAT_ID", "").strip()
    if platform != "wechat" or not chat_id:
        return None

    return {
        "chat_id": chat_id,
        "name": get_session_env("HERMES_SESSION_CHAT_NAME", "").strip() or chat_id,
    }


def _env_enablement() -> dict | None:
    ws_url = _bridge_ws_url()
    seed = {"bridge_ws_url": ws_url}
    current_home = _current_wechat_home_channel()
    if current_home:
        seed["home_channel"] = current_home
    else:
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
    chat_id = _resolve_send_chat_id(chat_id)
    frames: List[Dict[str, Any]] = []
    media_files = media_files or []
    for media in media_files:
        media_path, is_voice = _coerce_media_entry(media)
        if media_path:
            target_url = media_path
            if not _is_http_url(target_url):
                try:
                    target_url = await _upload_media_file(target_url, pconfig) or target_url
                except Exception as exc:
                    logger.warning("WeChat standalone media upload failed, using original path: %s", exc)
            frames.append({
                "event": "outbound_media",
                "payload": _build_media_payload(
                    chat_id,
                    target_url,
                    "",
                    audio_as_voice=is_voice,
                ),
            })
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
            "video, cards, and emoji markers back to WeChat. When sending a "
            "file or media to this current WeChat chat, put MEDIA:/absolute/path "
            "in your final response instead of calling send_message; the final "
            "response is already delivered to the current chat, and send_message "
            "is only for cross-channel delivery."
        ),
    )
