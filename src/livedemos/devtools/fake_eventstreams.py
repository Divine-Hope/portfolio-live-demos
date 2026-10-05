"""A stand-in for Wikimedia EventStreams, for offline development and tests.

It speaks the same protocol: SSE with Wikimedia-style ids, resume with Last-Event-ID,
`?since=`, limited retention, and connections that drop on a timer. Because it generates
every event it also knows the ground truth, which is what the resume proof test checks.

The parts that make resuming hard are modelled on purpose:

- Two topics (eqiad and codfw), each event from one of them. The SSE id carries a
  position per topic, and the two advance independently: codfw runs behind by a skew.
- Resume seeks each topic to its own timestamp, inclusive, like Kafka's offsetsForTimes.
  Events that share a millisecond with the bookmark come back, and so can events the
  client already had from the other topic.
- Several events can share one millisecond.
- Some events are dated earlier than their place in the stream (`--disorder-s`).

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


TOPICS = ("eqiad.mediawiki.recentchange", "codfw.mediawiki.recentchange")


@dataclass(slots=True)
class Stored:
    seq: int  # global stream order
    topic: str
    ts_ms: int  # the topic's position for this event
    sse_id: str
    data: str
    tracked: bool
    event_id: str


@dataclass(frozen=True, slots=True)
class Cursor:
    """Where a connection starts: from `next_seq` on, the events `wants` accepts."""

    next_seq: int
    from_ms: dict[str, int | None]  # per topic; None: that topic's live tail only
    tail_seq: int  # the first event generated after the connection opened

    def wants(self, e: Stored) -> bool:
        from_ms = self.from_ms.get(e.topic)
        return e.seq >= self.tail_seq if from_ms is None else e.ts_ms >= from_ms


@dataclass
class FakeStream:
    rate: float = 30.0
    history_s: float = 7_200.0
    drop_after_s: float | None = None
    # Like the real stream: some events are dated earlier than their place in the
    # stream (late events), so stream order and event-time order differ.
    disorder_s: float = 0.0
    seed: int | None = None
    # How far codfw's positions run behind eqiad's, and how often events share a ms.
    topic_skew_ms: int = 1_500
    same_ms_share: float = 0.2
    events: list[Stored] = field(default_factory=list)
    seqs: list[int] = field(default_factory=list)  # events[i].seq, kept in step for bisect
    paused: bool = False
    _rng: random.Random = field(default_factory=random.Random)
    _cond: asyncio.Condition = field(default_factory=asyncio.Condition)
    _next_seq: int = 0
    _positions: dict[str, int] = field(default_factory=dict)  # topic -> last ts_ms

    def __post_init__(self) -> None:
        if self.seed is not None:
            self._rng.seed(self.seed)

    # generation -----------------------------------------------------------------
    def make_event(self) -> Stored:
        topic = TOPICS[0] if self._rng.random() < 0.8 else TOPICS[1]
        last = self._positions.get(topic, 0)
        now_ms = int(time.time() * 1000) - (self.topic_skew_ms if topic == TOPICS[1] else 0)
        if last and self._rng.random() < self.same_ms_share:
            now_ms = last  # shares a millisecond with the topic's previous event
        now_ms = max(now_ms, last)  # each topic's timestamps never go backwards
        self._positions[topic] = now_ms
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
                "topic": topic,
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
        # Every topic's position as of this event. A topic not seen yet has no timestamp,
        # just an offset, like the real ids.
        sse_id = json.dumps(
            [
                {"topic": t, "partition": 0, "timestamp": self._positions[t]}
                if t in self._positions
                else {"topic": t, "partition": 0, "offset": -1}
                for t in TOPICS
            ],
            separators=(",", ":"),
        )
        tracked = domain != "canary" and wiki in TRACKED_WIKIS and kind in TRACKED_TYPES
        seq, self._next_seq = self._next_seq, self._next_seq + 1
        return Stored(seq, topic, now_ms, sse_id, json.dumps(event), tracked, event_id)

    @property
    def positions(self) -> dict[str, int]:
        return dict(self._positions)

    def append(self, stored: Stored) -> None:
        self.events.append(stored)
        self.seqs.append(stored.seq)

    async def generate(self) -> None:
        interval = 1.0 / self.rate
        while True:
            await asyncio.sleep(interval)
            if self.paused:
                continue
            stored = self.make_event()
            async with self._cond:
                self.append(stored)
                cutoff = stored.ts_ms - int(self.history_s * 1000)
                # Prune in one go about once a minute rather than on every event.
                if self.events[0].ts_ms < cutoff - 60_000:
                    keep = next(i for i, e in enumerate(self.events) if e.ts_ms >= cutoff)
                    del self.events[:keep]
                    del self.seqs[:keep]
                self._cond.notify_all()

    # serving ----------------------------------------------------------------------
    def cursor(self, last_event_id: str | None, since: str | None) -> Cursor:
        tail = self._next_seq
        from_ms: dict[str, int | None] = dict.fromkeys(TOPICS)
        if last_event_id:
            # Each topic seeks to its own timestamp. An entry with an offset instead
            # (`{"topic": "codfw...", "partition": 0, "offset": -1}`) means that topic's tail.
            try:
                for position in json.loads(last_event_id):
                    if "timestamp" in position:
                        from_ms[position["topic"]] = int(position["timestamp"])
            except (ValueError, KeyError, TypeError):
                pass
        elif since:
            since_ms = _parse_since(since)
            from_ms = dict.fromkeys(TOPICS, since_ms)
        if all(v is None for v in from_ms.values()):
            return Cursor(tail, from_ms, tail)
        cursor = Cursor(0, from_ms, tail)
        first = next((e.seq for e in self.events if cursor.wants(e)), tail)
        return Cursor(first, from_ms, tail)

    def replay(self, cursor: Cursor) -> list[Stored]:
        """What a connection with this cursor would be sent from the current buffer."""
        return [e for e in self.events if e.seq >= cursor.next_seq and cursor.wants(e)]

    async def stream(self, cursor: Cursor) -> AsyncIterator[bytes]:
        opened = time.monotonic()
        next_seq = cursor.next_seq  # by seq, because pruning shifts list indexes
        yield b":ok\n\n"
        while True:
            if self.drop_after_s and time.monotonic() - opened > self.drop_after_s:
                return
            async with self._cond:
                idx = bisect.bisect_left(self.seqs, next_seq)
                if idx >= len(self.events):
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self._cond.wait(), timeout=1.0)
                    continue
                chunk = self.events[idx : idx + 500]
            for e in chunk:
                if cursor.wants(e):
                    yield f"event: message\nid: {e.sse_id}\ndata: {e.data}\n\n".encode()
            next_seq = chunk[-1].seq + 1

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
        cursor = fake.cursor(request.headers.get("last-event-id"), since)
        return StreamingResponse(fake.stream(cursor), media_type="text/event-stream")

    @app.get("/_control/truth")
    async def truth(from_ms: int = 0, to_ms: int = 2**62) -> dict[str, Any]:
        return fake.truth(from_ms, to_ms)

    @app.post("/_control/pause")
    async def pause() -> dict[str, Any]:
        fake.paused = True
        last = max(fake.positions.values(), default=None)
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
    parser.add_argument("--topic-skew-ms", type=int, default=1_500, help="codfw lag behind eqiad")
    parser.add_argument("--same-ms-share", type=float, default=0.2, help="share of shared-ms ids")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    fake = FakeStream(
        rate=args.rate,
        history_s=args.history_s,
        drop_after_s=args.drop_after_s,
        disorder_s=args.disorder_s,
        topic_skew_ms=args.topic_skew_ms,
        same_ms_share=args.same_ms_share,
        seed=args.seed,
    )
    uvicorn.run(create_app(fake), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
