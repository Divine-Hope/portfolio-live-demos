"""A stand-in for Wikimedia EventStreams, for offline development and tests.

It speaks the same protocol: SSE with Wikimedia-style ids, resume with Last-Event-ID
(by timestamp, inclusive, so the seam really does repeat events), `?since=`, limited
retention, and connections that drop on a timer. Because it generates every event it
also knows the ground truth, which is what the resume proof test checks against.

    python -m livedemos.devtools.fake_eventstreams --port 8090 --rate 30
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import random
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

TRACKED_WIKIS = ("enwiki", "ptwiki", "dewiki")
OTHER_WIKIS = ("wikidatawiki", "commonswiki", "frwiki")
TRACKED_TYPES = ("edit", "new")
OTHER_TYPES = ("log", "categorize")
TITLES = {
    "enwiki": ["Portugal", "Apache Kafka", "Lisbon", "Data engineering", "ClickHouse",
               "Porto", "Wikipedia", "Football", "Weather", "Python (programming language)"],
    "ptwiki": ["Lisboa", "Porto", "Portugal", "Brasil", "Futebol Clube do Porto",
               "Rio Douro", "Fado", "Coimbra"],
    "dewiki": ["Deutschland", "Berlin", "Portugal", "Apache Kafka", "Wikipedia", "Hamburg",
               "Fußball", "München"],
}  # fmt: skip


@dataclass(slots=True)
class Stored:
    ts_ms: int
    sse_id: str
    data: str
    tracked: bool
    event_id: str


@dataclass
class FakeStream:
    rate: float = 30.0
    history_s: float = 7_200.0
    drop_after_s: float | None = None
    # Like the real stream: some events are dated earlier than their place in the
    # stream (late events), so stream order and event-time order differ.
    disorder_s: float = 0.0
    seed: int | None = None
    events: list[Stored] = field(default_factory=list)
    ts: list[int] = field(default_factory=list)  # events[i].ts_ms, kept in step for bisect
    paused: bool = False
    _rng: random.Random = field(default_factory=random.Random)
    _cond: asyncio.Condition = field(default_factory=asyncio.Condition)
    _last_ms: int = 0

    def __post_init__(self) -> None:
        if self.seed is not None:
            self._rng.seed(self.seed)

    # generation -----------------------------------------------------------------
    def make_event(self) -> Stored:
        now_ms = max(int(time.time() * 1000), self._last_ms + 1)  # strictly increasing
        self._last_ms = now_ms
        late_ms = 0
        if self.disorder_s and self._rng.random() < 0.3:
            late_ms = int(self._rng.uniform(0, self.disorder_s) * 1000)
        dt = datetime.fromtimestamp((now_ms - late_ms) / 1000, UTC)
        roll = self._rng.random()
        if roll < 0.01:
            wiki, kind = "enwiki", "edit"
            domain = "canary"
        else:
            wiki = self._rng.choice(TRACKED_WIKIS if roll < 0.55 else OTHER_WIKIS)
            kind = self._rng.choice(TRACKED_TYPES * 4 + OTHER_TYPES)
            domain = f"{wiki.removesuffix('wiki')}.wikipedia.org"
        titles = TITLES.get(wiki, ["Q42", "File:Example.jpg", "Paris"])
        # A skewed pick so a few articles are clearly "most edited".
        title = titles[min(int(self._rng.expovariate(0.6)), len(titles) - 1)]
        event_id = str(uuid.UUID(int=self._rng.getrandbits(128), version=4))
        event = {
            "$schema": "/mediawiki/recentchange/1.0.0",
            "meta": {
                "uri": f"https://{domain}/wiki/{title.replace(' ', '_')}",
                "id": event_id,
                "dt": dt.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "domain": domain,
                "stream": "mediawiki.recentchange",
                "topic": "eqiad.mediawiki.recentchange",
                "partition": 0,
            },
            "id": self._rng.randint(1, 2_000_000_000),
            "type": kind,
            "namespace": 0 if self._rng.random() < 0.7 else self._rng.choice([1, 2, 4]),
            "title": title,
            "timestamp": now_ms // 1000,
            "user": "ExampleUser",
            "bot": self._rng.random() < 0.2,
            "minor": self._rng.random() < 0.3,
            "server_url": f"https://{domain}",
            "server_name": domain,
            "wiki": wiki,
        }
        sse_id = json.dumps(
            [
                {"topic": "eqiad.mediawiki.recentchange", "partition": 0, "timestamp": now_ms},
                {"topic": "codfw.mediawiki.recentchange", "partition": 0, "timestamp": now_ms},
            ],
            separators=(",", ":"),
        )
        tracked = domain != "canary" and wiki in TRACKED_WIKIS and kind in TRACKED_TYPES
        return Stored(now_ms, sse_id, json.dumps(event), tracked, event_id)

    async def generate(self) -> None:
        interval = 1.0 / self.rate
        while True:
            await asyncio.sleep(interval)
            if self.paused:
                continue
            stored = self.make_event()
            async with self._cond:
                self.events.append(stored)
                self.ts.append(stored.ts_ms)
                cutoff = stored.ts_ms - int(self.history_s * 1000)
                # Prune in one go about once a minute rather than on every event.
                if self.ts[0] < cutoff - 60_000:
                    keep = bisect.bisect_left(self.ts, cutoff)
                    del self.events[:keep]
                    del self.ts[:keep]
                self._cond.notify_all()

    # serving ----------------------------------------------------------------------
    def start_index(self, last_event_id: str | None, since: str | None) -> int:
        from_ms: int | None = None
        if last_event_id:
            # Real ids mix entries with a timestamp and entries with an offset
            # (`{"topic": "codfw...", "partition": 0, "offset": -1}`).
            try:
                positions = json.loads(last_event_id)
                from_ms = max(int(p["timestamp"]) for p in positions if "timestamp" in p)
            except (ValueError, KeyError, TypeError):
                from_ms = None
        elif since:
            from_ms = _parse_since(since)
        if from_ms is None:
            return len(self.events)  # live tail
        # Inclusive, like the real thing: the bookmarked event comes back once more.
        return bisect.bisect_left(self.ts, from_ms)

    async def stream(self, start: int) -> AsyncIterator[bytes]:
        opened = time.monotonic()
        # Keep our place by timestamp, because pruning shifts list indexes.
        next_ms = self.events[start].ts_ms if start < len(self.events) else self._last_ms + 1
        yield b":ok\n\n"
        while True:
            if self.drop_after_s and time.monotonic() - opened > self.drop_after_s:
                return
            async with self._cond:
                idx = bisect.bisect_left(self.ts, next_ms)
                if idx >= len(self.events):
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self._cond.wait(), timeout=1.0)
                    continue
                chunk = self.events[idx : idx + 500]
            for e in chunk:
                yield f"event: message\nid: {e.sse_id}\ndata: {e.data}\n\n".encode()
            next_ms = chunk[-1].ts_ms + 1

    def truth(self, from_ms: int, to_ms: int) -> dict[str, Any]:
        ids = [e.event_id for e in self.events if e.tracked and from_ms <= e.ts_ms <= to_ms]
        return {"tracked_events": len(ids), "event_ids": ids}


def _parse_since(since: str) -> int | None:
    if since.isdigit():
        return int(since)
    try:
        return int(datetime.fromisoformat(since.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def create_app(fake: FakeStream) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = asyncio.create_task(fake.generate())
        yield
        task.cancel()

    app = FastAPI(title="fake-eventstreams", lifespan=lifespan)

    @app.get("/v2/stream/recentchange")
    async def recentchange(request: Request, since: str | None = None) -> StreamingResponse:
        start = fake.start_index(request.headers.get("last-event-id"), since)
        return StreamingResponse(fake.stream(start), media_type="text/event-stream")

    @app.get("/_control/truth")
    async def truth(from_ms: int = 0, to_ms: int = 2**62) -> dict[str, Any]:
        return fake.truth(from_ms, to_ms)

    @app.post("/_control/pause")
    async def pause() -> dict[str, Any]:
        fake.paused = True
        last = fake.events[-1].ts_ms if fake.events else None
        return {"paused": True, "last_ts_ms": last}

    @app.post("/_control/resume")
    async def resume() -> dict[str, bool]:
        fake.paused = False
        return {"paused": False}

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")  # noqa: S104 - dev tool in a container
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--rate", type=float, default=30.0, help="events per second")
    parser.add_argument("--history-s", type=float, default=7_200.0, help="retention")
    parser.add_argument("--drop-after-s", type=float, default=None, help="close streams after N s")
    parser.add_argument("--disorder-s", type=float, default=0.0, help="max lateness of events")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    fake = FakeStream(
        rate=args.rate,
        history_s=args.history_s,
        drop_after_s=args.drop_after_s,
        disorder_s=args.disorder_s,
        seed=args.seed,
    )
    uvicorn.run(create_app(fake), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
