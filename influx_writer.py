"""Writes normalized `Event` records (see event.py) into InfluxDB 2.x.

Requires: pip install "influxdb-client[async]"
(https://github.com/influxdata/influxdb-client-python - official client;
the [async] extra pulls in aiohttp for the asyncio-native write API used
here, so writes don't block the same event loop the listeners run on).

Meant to be wired up by a coordinating main.py as a pull-loop task alongside
a listener (e.g. ProtectListener), consuming via `get_event()`:

    listener = ProtectListener(...)
    writer = InfluxEventWriter(url, token, org, bucket)  # inside async main()
    await listener.start()
    asyncio.create_task(writer.run(listener))

All attributes on an Event become InfluxDB fields (not tags), since their
cardinality/shape varies per event_type. source/entity/event_type are tags
(low cardinality, good for filtering in Grafana/Flux).
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from influxdb_client import Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from event import Event

_LOGGER = logging.getLogger("influx_writer")

MEASUREMENT = "home_events"


class EventSource(Protocol):
    async def get_event(self) -> Event: ...


def _to_field_value(value: Any) -> str | int | float | bool:
    if isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list | tuple | set):
        return ",".join(str(v) for v in value)
    return str(value)


class InfluxEventWriter:
    """Writes `Event` records to an InfluxDB 2.x bucket.

    Must be constructed inside a running event loop (e.g. inside an
    `async def main()`), not at module import time.
    """

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._bucket = bucket
        self._client = InfluxDBClientAsync(url=url, token=token, org=org)
        self._write_api = self._client.write_api()
        _LOGGER.info("InfluxDB writer targeting %s (org=%s, bucket=%s)", url, org, bucket)

    async def write(self, event: Event) -> None:
        point = (
            Point(MEASUREMENT)
            .tag("source", event.source)
            .tag("entity", event.entity)
            .tag("event_type", event.event_type)
            .field("value", 1)
            .time(event.timestamp, WritePrecision.NS)
        )
        for key, value in event.attributes.items():
            point.field(key, _to_field_value(value))
        try:
            await self._write_api.write(bucket=self._bucket, record=point)
        except Exception:
            _LOGGER.exception(
                "Failed to write %s/%s to InfluxDB", event.entity, event.event_type
            )
            return
        _LOGGER.debug(
            "Wrote %s/%s to InfluxDB (%d attributes)",
            event.entity, event.event_type, len(event.attributes),
        )

    async def run(self, source: EventSource) -> None:
        """Pull events from `source.get_event()` forever and write each one."""
        _LOGGER.info("InfluxDB writer pull-loop started")
        while True:
            event = await source.get_event()
            await self.write(event)

    async def close(self) -> None:
        await self._client.close()
