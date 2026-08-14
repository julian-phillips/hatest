"""UniFi Protect camera event listener.

Turns UniFi Protect camera activity into normalized `Event` records (see
event.py) by keeping the Protect controller's private-API websocket open.

Requires: pip install uiprotect
(https://github.com/uilibs/uiprotect - the maintained successor to
pyunifiprotect, and what the Home Assistant unifiprotect integration is
built on). Target runtime is the Home Assistant VM (Linux) on Unraid, which
matches uiprotect's supported platform (it does not support native Windows).

The `front_door` camera is a UniFi G6/G3 Entry (doorbell + access reader).
Protect still models it as a plain `Camera` device (with `has_fingerprint_
sensor` / `support_nfc` flags) and reports its card/fingerprint/door-release
activity as ordinary `Event` records tied to that camera_id, so no special
casing was needed beyond adding those EventTypes below.

Docs consulted for this implementation (source-verified, not guessed):
- README quickstart: ProtectApiClient, protect.update(), bootstrap.cameras,
  subscribe_websocket(callback) / WSSubscriptionMessage.
- uiprotect/data/types.py: EventType, SmartDetectObjectType, StateType enums.
- uiprotect/data/nvr.py: Event model fields (type, start, end, score,
  camera_id, smart_detect_types, thumbnail_id, metadata, get_thumbnail(),
  get_smart_detect_track()); EventMetadata (reason, nfc, fingerprint).
- uiprotect/data/devices.py: Camera has_fingerprint_sensor / support_nfc.
- uiprotect/data/websocket.py: WSAction, WSSubscriptionMessage(action,
  new_update_id, changed_data, new_obj, old_obj).

This feed has a 2-minute SLA, so correctness/completeness (an extra API
round trip per smart-detect event to pull zone/line detail, saving
thumbnails to disk) is favored over minimizing latency.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from uiprotect import ProtectApiClient
from uiprotect.data import (
    Camera,
    EventType,
    StateType,
    WSAction,
    WSSubscriptionMessage,
)
from uiprotect.data import Event as ProtectEvent

from event import Event

_LOGGER = logging.getLogger("protect_listener")

SOURCE = "protect"

# ---------------------------------------------------------------------------
# Camera name -> canonical entity slug (the 10 camera entities).
# Match is case-insensitive on the camera's Protect display name. Any camera
# not listed here still gets reported, using a slugified version of its
# Protect name, so nothing is silently dropped.
# ---------------------------------------------------------------------------
DEFAULT_CAMERA_ENTITY_MAP: dict[str, str] = {
    "porch east": "porch_east",
    "front door": "front_door",
    "front porch": "front_porch",
    "porch west": "porch_west",
    "gate east": "gate_east",
    "side door": "side_door",
    "backyard": "backyard",
    "patio": "patio",
    "courtyard": "courtyard",
    "gate west": "gate_west",
}

# Protect EventType -> normalized event_type vocabulary. motion/smart_detect/
# smart_detect_line map onto the supplied vocabulary 1:1. The rest are
# camera-native detection types with no exact match in that list; kept
# distinct rather than dropped (see chat reply for the open question).
EVENT_TYPE_MAP: dict[EventType, str] = {
    EventType.SMART_DETECT: "smart_detection",
    EventType.SMART_DETECT_LINE: "line_crossing",
    EventType.MOTION: "motion",
    EventType.SMART_DETECT_LOITER: "loitering",
    EventType.SMART_AUDIO_DETECT: "audio_detection",
    EventType.RING: "doorbell_ring",
    # G6/G3 Entry (front_door) access-reader activity.
    EventType.NFC_CARD_SCANNED: "nfc_scanned",
    EventType.FINGERPRINT_IDENTIFIED: "fingerprint_identified",
    EventType.DOOR_ACCESS: "door_access",
}

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    return _SLUG_RE.sub("_", name.strip().lower()).strip("_")


def parse_camera_map(raw: str) -> dict[str, str]:
    """Parse 'Camera Name=slug;Other Cam=slug2' into a lookup dict."""
    result: dict[str, str] = {}
    for pair in raw.split(";"):
        if "=" not in pair:
            continue
        name, slug = pair.split("=", 1)
        if name.strip():
            result[name.strip().lower()] = slug.strip()
    return result


class ProtectListener:
    """Keeps a Protect websocket open and emits normalized `Event` records.

    Two ways for another module to consume events, usable together:
    - callback: `unsubscribe = listener.subscribe(my_callback)`
    - pull: `event = await listener.get_event()` in a loop
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        *,
        verify_ssl: bool = False,
        camera_entity_map: dict[str, str] | None = None,
        thumbnail_dir: str | Path | None = "thumbnails",
        fetch_smart_detect_detail: bool = True,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self.protect = ProtectApiClient(
            host, port, username, password, verify_ssl=verify_ssl
        )
        self._camera_entity_map = {
            **DEFAULT_CAMERA_ENTITY_MAP,
            **(camera_entity_map or {}),
        }
        self._thumbnail_dir = Path(thumbnail_dir) if thumbnail_dir else None
        self._fetch_smart_detect_detail = fetch_smart_detect_detail
        self._subscribers: list[Callable[[Event], None]] = [on_event] if on_event else []
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._unsub: Any = None
        self._tasks: set[asyncio.Task[None]] = set()

    def subscribe(self, callback: Callable[[Event], None]) -> Callable[[], None]:
        """Register a callback for every emitted Event; returns an unsubscribe function."""
        self._subscribers.append(callback)
        _LOGGER.debug("Subscriber added: %r (total=%d)", callback, len(self._subscribers))

        def unsubscribe() -> None:
            if callback in self._subscribers:
                self._subscribers.remove(callback)
                _LOGGER.debug("Subscriber removed: %r (total=%d)", callback, len(self._subscribers))

        return unsubscribe

    async def get_event(self) -> Event:
        """Await the next emitted Event (pull-style, for a consumer in another file)."""
        return await self._queue.get()

    def resolve_entity(self, camera: Camera | None, fallback_name: str | None = None) -> str:
        name = (camera.name if camera and camera.name else fallback_name) or "unknown_camera"
        return self._camera_entity_map.get(name.strip().lower(), slugify(name))

    async def start(self) -> None:
        _LOGGER.debug("Connecting to Protect controller %s:%s", self._host, self._port)
        try:
            await self.protect.update()  # loads bootstrap + opens the websocket
        except Exception:
            _LOGGER.exception(
                "Failed to connect/bootstrap Protect controller %s:%s", self._host, self._port
            )
            raise
        cameras = self.protect.bootstrap.cameras
        unknown = sorted(
            camera.name
            for camera in cameras.values()
            if camera.name and camera.name.strip().lower() not in self._camera_entity_map
        )
        if unknown:
            _LOGGER.warning(
                "No entity mapping for these Protect cameras (falling back to "
                "slugified names): %s",
                unknown,
            )
        self._unsub = self.protect.subscribe_websocket(self._on_ws_message)
        _LOGGER.info(
            "Listening for Protect camera events (%s:%s, %d cameras)",
            self._host, self._port, len(cameras),
        )

    async def stop(self) -> None:
        _LOGGER.info("Stopping Protect listener (%s:%s)", self._host, self._port)
        if self._unsub is not None:
            self._unsub()
            self._unsub = None
        pending = list(self._tasks)
        if pending:
            _LOGGER.debug("Cancelling %d in-flight event-enrichment task(s)", len(pending))
        for task in pending:
            task.cancel()
        try:
            await self.protect.close_session()
        except Exception:
            _LOGGER.exception("Error closing Protect session")

    # -- websocket dispatch --------------------------------------------------

    def _on_ws_message(self, message: WSSubscriptionMessage) -> None:
        try:
            new_obj = message.new_obj
            _LOGGER.debug(
                "ws message action=%s new_obj=%s",
                message.action, type(new_obj).__name__ if new_obj is not None else None,
            )
            if isinstance(new_obj, ProtectEvent):
                self._handle_protect_event(message, new_obj)
            elif isinstance(new_obj, Camera):
                self._handle_camera_state(message, new_obj)
        except Exception:
            _LOGGER.exception(
                "Failed to process Protect websocket message (action=%s)",
                getattr(message, "action", "?"),
            )

    def _handle_protect_event(self, message: WSSubscriptionMessage, protect_event: ProtectEvent) -> None:
        # React only when a detection starts; the later UPDATE (setting
        # `end`/score) would otherwise double-report the same detection.
        if message.action is not WSAction.ADD:
            _LOGGER.debug(
                "Skipping non-ADD action %s for event %s", message.action, protect_event.id
            )
            return
        event_type = EVENT_TYPE_MAP.get(protect_event.type)
        if event_type is None:
            _LOGGER.debug(
                "Ignoring unmapped EventType %s (event %s, camera_id %s)",
                protect_event.type, protect_event.id, protect_event.camera_id,
            )
            return
        _LOGGER.debug(
            "Dispatching event %s type=%s camera_id=%s",
            protect_event.id, event_type, protect_event.camera_id,
        )
        task = asyncio.create_task(self._process_smart_event(protect_event, event_type))
        self._tasks.add(task)
        task.add_done_callback(lambda t: self._on_task_done(t, protect_event.id))

    def _on_task_done(self, task: asyncio.Task[None], event_id: str) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            _LOGGER.error(
                "Event-enrichment task for %s failed", event_id, exc_info=task.exception()
            )

    def _handle_camera_state(self, message: WSSubscriptionMessage, camera: Camera) -> None:
        old = message.old_obj
        old_state = old.state if isinstance(old, Camera) else None
        if old_state == camera.state:
            return
        if camera.state is StateType.CONNECTED:
            event_type = "camera_online"
        elif camera.state is StateType.DISCONNECTED:
            event_type = "camera_offline"
        else:
            return  # CONNECTING is transient, not a reportable event
        _LOGGER.info("Camera %r state %s -> %s", camera.name, old_state, camera.state)
        self._emit(
            Event(
                timestamp=datetime.now(timezone.utc),
                source=SOURCE,
                entity=self.resolve_entity(camera),
                event_type=event_type,
                attributes={},
            )
        )

    # -- event enrichment -----------------------------------------------------

    async def _process_smart_event(self, protect_event: ProtectEvent, event_type: str) -> None:
        entity = self.resolve_entity(protect_event.camera)
        attributes = await self._build_attributes(protect_event)
        self._emit(
            Event(
                timestamp=protect_event.start,
                source=SOURCE,
                entity=entity,
                event_type=event_type,
                attributes=attributes,
            )
        )

    async def _build_attributes(self, protect_event: ProtectEvent) -> dict[str, Any]:
        attributes: dict[str, Any] = {"event_id": protect_event.id}
        if protect_event.smart_detect_types:
            attributes["object_type"] = ",".join(
                t.value for t in protect_event.smart_detect_types
            )
        if protect_event.score:
            attributes["confidence"] = protect_event.score

        thumbnail_path = await self._maybe_save_thumbnail(protect_event)
        if thumbnail_path:
            attributes["thumbnail_path"] = str(thumbnail_path)

        if self._fetch_smart_detect_detail and protect_event.type in (
            EventType.SMART_DETECT,
            EventType.SMART_DETECT_LINE,
        ):
            zone_ids, line_ids = await self._fetch_zone_line(protect_event)
            if zone_ids:
                attributes["zone"] = zone_ids
            if line_ids:
                attributes["line"] = line_ids
        # "direction" is intentionally omitted - see the open question in
        # the chat reply, Protect's API does not expose it directly.

        # G6/G3 Entry access events: surface whatever identity/result info
        # Protect already attached, rather than a second lookup - see the
        # open question in the chat reply about resolving this to a person.
        if protect_event.metadata is not None:
            if protect_event.metadata.reason:
                attributes["reason"] = protect_event.metadata.reason
            if protect_event.metadata.nfc is not None:
                attributes["ulp_id"] = protect_event.metadata.nfc.ulp_id
            elif protect_event.metadata.fingerprint is not None:
                attributes["ulp_id"] = protect_event.metadata.fingerprint.ulp_id
        return attributes

    async def _maybe_save_thumbnail(self, protect_event: ProtectEvent) -> Path | None:
        if not self._thumbnail_dir or not protect_event.thumbnail_id:
            return None
        try:
            data = await protect_event.get_thumbnail()
        except Exception:
            _LOGGER.exception("Failed to fetch thumbnail for event %s", protect_event.id)
            return None
        if not data:
            return None
        self._thumbnail_dir.mkdir(parents=True, exist_ok=True)
        path = self._thumbnail_dir / f"{protect_event.id}.jpg"
        path.write_bytes(data)
        return path

    async def _fetch_zone_line(self, protect_event: ProtectEvent) -> tuple[list[int], list[int]]:
        try:
            track = await protect_event.get_smart_detect_track()
        except Exception:
            _LOGGER.debug(
                "No smart-detect track available for event %s", protect_event.id, exc_info=True
            )
            return [], []
        zone_ids: set[int] = set()
        line_ids: set[int] = set()
        for item in track.payload:
            zone_ids.update(item.zone_ids)
            if item.lines:
                line_ids.update(item.lines)
        return sorted(zone_ids), sorted(line_ids)

    def _emit(self, event: Event) -> None:
        self._queue.put_nowait(event)
        _LOGGER.debug(
            "Emitted %s/%s (queue_size=%d, subscribers=%d)",
            event.entity, event.event_type, self._queue.qsize(), len(self._subscribers),
        )
        if not self._subscribers:
            _LOGGER.info("%s", event)
        for callback in list(self._subscribers):
            try:
                callback(event)
            except Exception:
                _LOGGER.exception("Subscriber %r raised while handling %s/%s", callback, event.entity, event.event_type)


async def main() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if log_level.upper() == "DEBUG":
        # surface uiprotect's own websocket/reconnect diagnostics too
        logging.getLogger("uiprotect").setLevel(logging.DEBUG)

    host = os.environ["UFP_ADDRESS"]
    port = int(os.environ.get("UFP_PORT", "443"))
    username = os.environ["UFP_USERNAME"]
    password = os.environ["UFP_PASSWORD"]
    verify_ssl = os.environ.get("UFP_SSL_VERIFY", "false").lower() in ("1", "true", "yes")
    camera_map = parse_camera_map(os.environ.get("PROTECT_CAMERA_MAP", ""))

    _LOGGER.info(
        "Starting Protect listener: host=%s port=%s user=%s verify_ssl=%s",
        host, port, username, verify_ssl,
    )
    listener = ProtectListener(
        host, port, username, password, verify_ssl=verify_ssl, camera_entity_map=camera_map
    )
    await listener.start()
    try:
        await asyncio.Event().wait()  # run until interrupted (Ctrl+C)
    finally:
        await listener.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
