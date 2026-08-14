"""Coordinates the UniFi Protect listener with its event consumers.

Currently wires: ProtectListener --get_event()--> InfluxEventWriter.
Add further consumers here as they're built (more `writer.run(listener)`
style pull tasks, or `listener.subscribe(callback)` for push-style ones).

Env vars:
  UFP_ADDRESS, UFP_USERNAME, UFP_PASSWORD (required)
  UFP_PORT (default 443), UFP_SSL_VERIFY (default false)
  PROTECT_CAMERA_MAP  ("Name=slug;Other Cam=slug2", optional overrides)
  INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET (required)
  LOG_LEVEL (default INFO)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from influx_writer import InfluxEventWriter
from protect_listener import ProtectListener, parse_camera_map

_LOGGER = logging.getLogger("main")


def _configure_logging() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if log_level.upper() == "DEBUG":
        # surface uiprotect's own websocket/reconnect diagnostics too
        logging.getLogger("uiprotect").setLevel(logging.DEBUG)


async def main() -> None:
    _configure_logging()

    listener = ProtectListener(
        os.environ["UFP_ADDRESS"],
        int(os.environ.get("UFP_PORT", "443")),
        os.environ["UFP_USERNAME"],
        os.environ["UFP_PASSWORD"],
        verify_ssl=os.environ.get("UFP_SSL_VERIFY", "false").lower() in ("1", "true", "yes"),
        camera_entity_map=parse_camera_map(os.environ.get("PROTECT_CAMERA_MAP", "")),
    )
    await listener.start()

    writer = InfluxEventWriter(
        os.environ["INFLUX_URL"],
        os.environ["INFLUX_TOKEN"],
        os.environ["INFLUX_ORG"],
        os.environ["INFLUX_BUCKET"],
    )
    influx_task = asyncio.create_task(writer.run(listener))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
    except NotImplementedError:
        pass  # e.g. Windows; Ctrl+C still works via KeyboardInterrupt below

    _LOGGER.info("main: running (Ctrl+C / SIGTERM to stop)")
    try:
        await stop.wait()
    finally:
        _LOGGER.info("main: shutting down")
        influx_task.cancel()
        try:
            await influx_task
        except asyncio.CancelledError:
            pass
        await listener.stop()
        await writer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
