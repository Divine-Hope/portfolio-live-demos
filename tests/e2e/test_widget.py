"""The widget in a real browser, against a running stack.

Start the stack first (`make up-offline`), then `make e2e`. CI runs the same thing in
the stack job. Web fonts are stubbed so the result doesn't depend on internet access.
"""

from __future__ import annotations

import itertools
import os
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

pytest.importorskip("playwright.sync_api", reason="needs the e2e group: uv sync --group e2e")

from axe_playwright_python.sync_playwright import Axe
from playwright.sync_api import Browser, Page, Route, expect, sync_playwright

pytestmark = pytest.mark.e2e

WIDGET_URL = os.environ.get("LIVEDEMOS_E2E_URL", "http://localhost:8080/embed/wikipedia/")
HOST_URL = WIDGET_URL.removesuffix("embed/wikipedia/")  # the demo host page around it
LIVE_JSON = re.compile(r"/v1/wikipedia/live\.json")
BROWSER = os.environ.get("LIVEDEMOS_E2E_BROWSER", "chromium")  # chromium, firefox or webkit
ARTICLE_URL = re.compile(r"^https://(en|pt|de)\.wikipedia\.org/wiki/\S+$")
LIVE = re.compile(r"^Live · last event \d+s ago$")
POLL_S = 2
FONTS = re.compile(r"^https://fonts\.(googleapis|gstatic)\.com/")
FOCUSED_KEY = "document.activeElement?.closest('#list li')?.dataset.key ?? null"


@dataclass
class Widget:
    page: Page
    errors: list[str] = field(default_factory=list)

    def open(self, query: str = "lang=all&theme=light") -> None:
        self.page.goto(f"{WIDGET_URL}?{query}")
        expect(self.page.locator("#status-text")).to_have_text(LIVE, timeout=30_000)

    def tab_into_list(self) -> str:
        """Press Tab until focus reaches an article link. Returns that row's key."""
        for _ in range(20):
            self.page.keyboard.press("Tab")
            key: str | None = self.page.evaluate(FOCUSED_KEY)
            if key:
                return key
        pytest.fail("couldn't reach the article list with Tab")


def chart_label_for(payload: dict[str, Any]) -> str:
    """What the chart's aria-label should say for this payload (lang=all)."""
    series = payload["langs"]["all"]["per_minute"]
    full = [m for m in series if not m.get("partial") and m.get("edits") is not None]
    if not full:  # a fresh stack has no complete minute yet
        return "Edits per minute over the last hour. Not enough data yet."
    return f"Edits per minute over the last hour. Last full minute: {full[-1]['edits']:,} edits."


def assert_chart_label_matches_data(page: Page) -> None:
    """The label describes the data on screen. Retried, as a poll can land in between."""
    label = None
    for _ in range(5):
        payload = page.evaluate("fetch('/v1/wikipedia/live.json').then(r => r.json())")
        label = page.locator("#bars").get_attribute("aria-label")
        if label == chart_label_for(payload):
            return
        page.wait_for_timeout(1_000)
    pytest.fail(f"chart label doesn't match the data: {label!r}")


@pytest.fixture(scope="module")
def browser() -> Iterator[Browser]:
    with sync_playwright() as playwright:
        browser = getattr(playwright, BROWSER).launch()
        yield browser
        browser.close()


@pytest.fixture
def widget(browser: Browser) -> Iterator[Widget]:
    context = browser.new_context(viewport={"width": 900, "height": 900})
    page = context.new_page()
    widget = Widget(page)
    page.on("console", lambda m: widget.errors.append(m.text) if m.type == "error" else None)
    page.on("pageerror", lambda exc: widget.errors.append(str(exc)))
    page.route(FONTS, lambda route: route.fulfill(status=200, content_type="text/css", body=""))
    yield widget
    # A route handler can still be mid-fetch when a test ends (the widget polls every 2 s).
    # Drop handlers first, ignoring those in flight, so their errors can't surface in the
    # next test's browser calls.
    page.unroute_all(behavior="ignoreErrors")
    context.close()


def test_shows_live_numbers(widget: Widget) -> None:
    page = widget.page
    widget.open()

    for metric in ("#m-edits", "#m-pages", "#m-bots"):
        expect(page.locator(metric)).to_have_text(re.compile(r"^\d[\d,]*%?$"))
    expect(page.locator("#bars .bar")).to_have_count(60)
    assert_chart_label_matches_data(page)
    expect(page.locator("#list a").first).to_be_visible()
    hrefs: list[str] = page.locator("#list a").evaluate_all("links => links.map(a => a.href)")
    assert hrefs
    assert all(ARTICLE_URL.match(href) for href in hrefs), hrefs
    assert widget.errors == []


@pytest.mark.parametrize("width", [320, 768, 1280])
def test_fits_phone_tablet_and_desktop(widget: Widget, width: int) -> None:
    page = widget.page
    page.set_viewport_size({"width": width, "height": 900})
    widget.open()

    assert page.evaluate("document.documentElement.scrollWidth") <= width, "sideways scroll"
    for pill in page.locator(".pill").all():
        expect(pill).to_be_in_viewport()
        box = pill.bounding_box()
        assert box is not None
        assert box["height"] >= 24, "tap target too small"
    lefts = [card.bounding_box()["x"] for card in page.locator(".metric").all()]  # type: ignore[index]
    if width < 480:
        assert len(set(lefts)) == 1, "metrics should stack on a phone"
    else:
        assert len(set(lefts)) == 3, "metrics should sit side by side"
    assert widget.errors == []


def test_pills_and_definitions_work_from_the_keyboard(widget: Widget) -> None:
    page = widget.page
    widget.open()

    page.keyboard.press("Tab")
    expect(page.locator(".pill[data-lang=all]")).to_be_focused()
    page.keyboard.press("Tab")
    english = page.locator(".pill[data-lang=en]")
    expect(english).to_be_focused()
    page.keyboard.press("Space")
    expect(english).to_have_attribute("aria-pressed", "true")
    expect(page.locator(".pill[data-lang=all]")).to_have_attribute("aria-pressed", "false")
    expect(page.locator("#list .lang-tag:visible")).to_have_count(0)

    for _ in range(3):
        page.keyboard.press("Tab")
    info = page.get_by_role("button", name="What counts as an edit")
    expect(info).to_be_focused()
    page.keyboard.press("Enter")
    expect(info).to_have_attribute("aria-expanded", "true")
    expect(page.locator("#def-edits")).to_be_visible()
    page.keyboard.press("Enter")
    expect(page.locator("#def-edits")).to_be_hidden()
    assert widget.errors == []


@pytest.mark.parametrize("move", ["moveBefore", "insertBefore fallback"])
def test_keyboard_focus_survives_live_updates(widget: Widget, move: str) -> None:
    """A keyboard user on an article link stays there while the list updates."""
    page = widget.page
    if move != "moveBefore":
        page.add_init_script("delete Element.prototype.moveBefore")
    # Real data, but the top articles rotate and their counts grow on every poll, so the
    # list is guaranteed to reorder (live Wikipedia can sit still for longer than the test).
    polls = itertools.count(1)

    def churn(route: Route) -> None:
        response = route.fetch()
        body = response.json()
        n = next(polls)
        for summary in body.get("langs", {}).values():
            items = summary["top_articles"]
            if items:
                k = n % len(items)
                rotated = items[k:] + items[:k]
                summary["top_articles"] = [{**item, "edits": item["edits"] + n} for item in rotated]
        route.fulfill(response=response, json=body)

    page.route(LIVE_JSON, churn)
    widget.open()
    key = widget.tab_into_list()
    page.evaluate(
        """() => {
          window.__listChanges = 0;
          new MutationObserver(() => window.__listChanges++).observe(
            document.getElementById('list'),
            {childList: true, characterData: true, subtree: true},
          );
        }"""
    )

    page.wait_for_timeout(POLL_S * 3 * 1000)

    assert page.evaluate("window.__listChanges") > 0, "the list never updated: nothing tested"
    # The same articles, reordered: focus must still be on the one the user was on.
    assert page.evaluate(FOCUSED_KEY) == key
    assert widget.errors == []


def test_screen_readers_hear_changes_not_ticks(widget: Widget) -> None:
    page = widget.page
    widget.open()
    assert page.locator("#status").get_attribute("aria-live") is None
    expect(page.locator("#status-live")).to_have_text("Live.")
    page.evaluate(
        """() => {
          window.__announced = [];
          new MutationObserver(() => window.__announced.push(
            document.getElementById('status-live').textContent,
          )).observe(document.getElementById('status-live'),
            {childList: true, characterData: true, subtree: true});
        }"""
    )

    page.wait_for_timeout(5_000)

    assert page.evaluate("window.__announced") == []


def test_old_data_says_paused(widget: Widget) -> None:
    """Data that is 5 minutes old must never be shown as live, wherever it came from."""
    page = widget.page

    def five_minutes_old(route: Route) -> None:
        response = route.fetch()
        body = response.json()
        body["as_of"] = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        route.fulfill(response=response, json=body)

    page.route(re.compile(r"/v1/wikipedia/live\.json"), five_minutes_old)
    page.goto(f"{WIDGET_URL}?lang=all&theme=light")

    expect(page.locator("#status-text")).to_have_text(
        re.compile(r"^Paused · last event 5 min ago\."), timeout=10_000
    )
    expect(page.locator("#status-live")).to_have_text("Paused. Last event 5 min ago.")
    assert page.locator("#dot").get_attribute("class") == "dot paused"


@pytest.mark.parametrize(("motion", "animation"), [("reduce", "none"), ("no-preference", "pulse")])
def test_live_dot_respects_reduced_motion(widget: Widget, motion: str, animation: str) -> None:
    page = widget.page
    page.emulate_media(reduced_motion=motion)  # type: ignore[arg-type]
    widget.open()
    assert page.locator("#dot").evaluate("el => getComputedStyle(el).animationName") == animation


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_no_accessibility_violations(widget: Widget, theme: str) -> None:
    page = widget.page
    widget.open(f"lang=all&theme={theme}")
    page.get_by_role("button", name="What counts as an edit").click()  # include an open definition
    tags = ["wcag2a", "wcag2aa", "wcag21aa", "best-practice"]
    results = Axe().run(page, options={"runOnly": {"type": "tag", "values": tags}})
    assert results.violations_count == 0, results.generate_report()


def test_a_hung_request_does_not_stop_polling(widget: Widget) -> None:
    """The first request never answers. The widget gives up on it and carries on."""
    page = widget.page
    calls = 0

    def first_one_hangs(route: Route) -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            route.continue_()
        # else: never fulfilled, like a half-open connection

    page.route(LIVE_JSON, first_one_hangs)
    page.goto(f"{WIDGET_URL}?lang=all&theme=light")

    expect(page.locator("#status-text")).to_have_text(LIVE, timeout=20_000)
    assert calls >= 2


def _short_staleness(route: Route) -> None:
    """Real data, but with a 5 s staleness threshold, so an outage shows up quickly."""
    response = route.fetch()
    body = response.json()
    body["stale_after_s"] = 5
    route.fulfill(response=response, json=body)


def test_an_outage_after_live_data_ends_in_paused_on_widget_and_host(widget: Widget) -> None:
    page = widget.page
    page.route(LIVE_JSON, _short_staleness)
    page.goto(HOST_URL)
    fresh = page.locator("#fresh")
    expect(fresh).to_have_text(re.compile(r"^Live · "), timeout=30_000)

    page.unroute(LIVE_JSON)
    page.route(LIVE_JSON, lambda route: route.fulfill(status=503, body="down"))

    frame = page.frame_locator("#widget")
    expect(frame.locator("#status-text")).to_have_text(re.compile(r"^Paused · "), timeout=15_000)
    expect(fresh).to_have_text(re.compile(r"^Paused · "), timeout=5_000)


def test_the_host_stops_saying_live_when_the_widget_goes_quiet(widget: Widget) -> None:
    """No more messages at all (the frame is gone): the host ages its last report itself."""
    page = widget.page
    page.route(LIVE_JSON, _short_staleness)
    page.goto(HOST_URL)
    fresh = page.locator("#fresh")
    expect(fresh).to_have_text(re.compile(r"^Live · "), timeout=30_000)

    page.locator("#widget").evaluate("frame => { frame.src = 'about:blank'; }")

    expect(fresh).to_have_text(re.compile(r"^Paused · "), timeout=15_000)
