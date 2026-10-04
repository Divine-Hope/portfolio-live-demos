// Embeddable widget. A thin client: it polls one cached JSON snapshot and renders it.
// The API does the work once per second for everyone; this file only draws.
(() => {
  "use strict";

  const POLL_MS = 2000;
  const STALE_AFTER_S = 60;
  const LANGS = ["all", "en", "pt", "de"];
  const params = new URLSearchParams(location.search);
  // `?api=` is a development convenience only. On any real host the API comes from the
  // page's own config, so a crafted link can't point the widget at someone else's data.
  const isLocal = ["localhost", "127.0.0.1"].includes(location.hostname);
  const apiBase = ((isLocal && params.get("api")) || document.querySelector('meta[name="livedemos-api"]')?.content || "").replace(/\/$/, "");
  const url = `${apiBase}/v1/wikipedia/live.json`;
  const fmt = new Intl.NumberFormat("en-US");
  const timeFmt = new Intl.DateTimeFormat("en-GB", { hour: "2-digit", minute: "2-digit", timeZone: "UTC" });

  const state = {
    lang: LANGS.includes(params.get("lang")) ? params.get("lang") : "all",
    payload: null,
    ageAtReceipt: null, // seconds, from the server, corrected for CDN cache age
    receivedAt: 0, // performance.now() when it arrived
    failures: 0,
  };

  const $ = (id) => document.getElementById(id);
  setTheme(params.get("theme"));

  // Language pills ------------------------------------------------------------------
  document.querySelectorAll(".pill").forEach((pill) => {
    pill.addEventListener("click", () => {
      state.lang = pill.dataset.lang;
      render();
      notifyParent();
    });
  });

  // Definitions: visible on demand, keyboard friendly ------------------------------
  document.querySelectorAll(".info").forEach((button) => {
    button.addEventListener("click", () => {
      const target = $(button.getAttribute("aria-controls"));
      const open = button.getAttribute("aria-expanded") === "true";
      button.setAttribute("aria-expanded", String(!open));
      target.hidden = open;
    });
  });

  // The host page can switch the theme without reloading us.
  window.addEventListener("message", (event) => {
    if (event.source !== window.parent || !event.data || typeof event.data !== "object") return;
    if (event.data.type === "livedemos:theme") setTheme(event.data.theme);
  });

  function setTheme(theme) {
    document.documentElement.dataset.theme = theme === "dark" ? "dark" : "light";
  }

  // Polling ---------------------------------------------------------------------------
  async function poll() {
    try {
      const response = await fetch(url, { headers: { Accept: "application/json" } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      state.payload = payload;
      state.ageAtReceipt = ageOfNewestEvent(payload, response.headers);
      state.receivedAt = performance.now();
      state.failures = 0;
      render();
      notifyParent();
    } catch (error) {
      state.failures += 1;
      renderStatus();
    } finally {
      // Back off a little while the API is unreachable, then return to normal.
      const delay = state.failures ? Math.min(POLL_MS * 2 ** state.failures, 30000) : POLL_MS;
      setTimeout(poll, delay);
    }
  }

  // How old is the newest event, right now? Measured against the server's clock
  // (Date header, plus Age if a cache held the response), never the viewer's. A copy
  // served from a cache or a fallback still carries its original `as_of`, so it can't
  // pass itself off as live.
  function ageOfNewestEvent(payload, headers) {
    if (!payload.as_of) return null;
    const date = Date.parse(headers.get("Date") || "");
    if (Number.isNaN(date)) return payload.last_event_age_s; // header hidden: best effort
    const serverNow = date + Number(headers.get("Age") || 0) * 1000;
    return Math.max(0, (serverNow - Date.parse(payload.as_of)) / 1000);
  }

  function articleUrl(lang, title) {
    // Built here from allowlisted parts, not taken from the payload.
    return `https://${lang}.wikipedia.org/wiki/${encodeURIComponent(title.replace(/ /g, "_"))}`;
  }

  function currentAge() {
    if (state.ageAtReceipt == null) return null;
    return state.ageAtReceipt + (performance.now() - state.receivedAt) / 1000;
  }

  // Rendering -------------------------------------------------------------------------
  function render() {
    document.querySelectorAll(".pill").forEach((pill) => {
      pill.setAttribute("aria-pressed", String(pill.dataset.lang === state.lang));
    });
    const data = state.payload?.langs?.[state.lang];
    if (!data) {
      renderStatus();
      return;
    }
    $("m-edits").textContent = fmt.format(data.edits_5m);
    $("m-pages").textContent = fmt.format(data.pages_5m);
    $("m-bots").textContent = data.bot_share_5m == null ? "n/a" : `${Math.round(data.bot_share_5m * 100)}%`;
    renderBars(data.per_minute);
    renderList(data.top_articles);
    renderStatus();
  }

  function renderBars(series) {
    const bars = $("bars");
    const known = series.filter((m) => m.edits != null && !m.partial).map((m) => m.edits);
    const max = Math.max(1, ...known, ...series.filter((m) => m.partial && m.edits != null).map((m) => m.edits));
    bars.replaceChildren(
      ...series.map((m) => {
        const bar = document.createElement("span");
        const at = timeFmt.format(new Date(m.t * 1000));
        if (m.edits == null) {
          bar.className = "bar unknown";
          bar.title = `${at} UTC: no data`;
        } else {
          bar.className = m.partial ? "bar partial" : "bar";
          bar.style.height = `${Math.max(2, (m.edits / max) * 100)}%`;
          bar.title = `${at} UTC: ${fmt.format(m.edits)} edits${m.partial ? " so far" : ""}`;
        }
        return bar;
      }),
    );
    const lastFull = [...series].reverse().find((m) => !m.partial && m.edits != null);
    bars.setAttribute(
      "aria-label",
      lastFull
        ? `Edits per minute over the last hour. Last full minute: ${fmt.format(lastFull.edits)} edits.`
        : "Edits per minute over the last hour. Not enough data yet.",
    );
  }

  // The list updates in place, keyed by article. Rebuilding it every poll would throw
  // away keyboard focus and make screen readers start the list over every 2 seconds.
  function renderList(items) {
    const list = $("list");
    const rows = items.filter((item) => LANGS.includes(item.lang) && item.lang !== "all");
    const hadFocus = list.contains(document.activeElement);
    const focusedKey = hadFocus ? document.activeElement.closest("li")?.dataset.key : undefined;

    if (!rows.length) {
      if (list.children.length !== 1 || !list.firstElementChild.classList.contains("empty")) {
        const empty = document.createElement("li");
        empty.className = "empty";
        empty.textContent = "No article edits in this window.";
        list.replaceChildren(empty);
      }
      keepFocus(list, hadFocus, focusedKey);
      return;
    }

    const keys = rows.map((item) => `${item.lang}:${item.title}`);
    const wanted = new Set(keys);
    for (const li of [...list.children]) {
      if (!wanted.has(li.dataset.key)) li.remove();
    }
    const existing = new Map([...list.children].map((li) => [li.dataset.key, li]));
    rows.forEach((item, i) => {
      const li = existing.get(keys[i]) ?? articleRow(item, keys[i]);
      updateRow(li, item);
      const slot = list.children[i] ?? null;
      if (slot === li) return;
      // moveBefore keeps focus on a row that moves. Older browsers fall back to
      // insertBefore, and keepFocus puts focus back.
      if (li.isConnected && typeof list.moveBefore === "function") list.moveBefore(li, slot);
      else list.insertBefore(li, slot);
    });
    keepFocus(list, hadFocus, focusedKey);
  }

  function articleRow(item, key) {
    const li = document.createElement("li");
    li.dataset.key = key;
    const left = document.createElement("span");
    left.className = "article";
    const link = document.createElement("a");
    link.href = articleUrl(item.lang, item.title);
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = `${item.title} ↗`;
    const tag = document.createElement("span");
    tag.className = "lang-tag";
    tag.textContent = item.lang;
    left.append(link, tag);
    const count = document.createElement("span");
    count.className = "count";
    li.append(left, count);
    return li;
  }

  function updateRow(li, item) {
    li.querySelector(".lang-tag").hidden = state.lang !== "all";
    const count = li.querySelector(".count");
    const text = `${fmt.format(item.edits)} ${item.edits === 1 ? "edit" : "edits"}`;
    if (count.textContent !== text) count.textContent = text;
  }

  function keepFocus(list, hadFocus, focusedKey) {
    if (!hadFocus || list.contains(document.activeElement)) return;
    const row = focusedKey ? [...list.children].find((li) => li.dataset.key === focusedKey) : null;
    (row?.querySelector("a") ?? list).focus({ preventScroll: true });
  }

  // The visible line ticks every second. Screen readers only hear about real changes
  // (live, paused, unreachable), through a separate live region.
  let announced = null;
  function announce(kind, message) {
    if (kind === announced) return;
    announced = kind;
    $("status-live").textContent = message;
  }

  function renderStatus() {
    const dot = $("dot");
    const text = $("status-text");
    const age = currentAge();
    if (state.failures > 0 && !state.payload) {
      dot.className = "dot";
      text.textContent = "Can't reach the data API yet. Retrying.";
      announce("unreachable", text.textContent);
      return;
    }
    if (!state.payload) return;
    if (state.payload.status === "empty" || age == null) {
      dot.className = "dot";
      text.textContent = "Waiting for the first events.";
      announce("waiting", text.textContent);
      return;
    }
    if (age > STALE_AFTER_S) {
      dot.className = "dot paused";
      text.textContent = `Paused · last event ${describeAge(age)} ago. Nothing here is invented to fill the gap.`;
      announce("paused", `Paused. Last event ${describeAge(age)} ago.`);
      return;
    }
    dot.className = "dot live";
    text.textContent = `Live · last event ${Math.max(0, Math.round(age))}s ago`;
    announce("live", "Live.");
  }

  function describeAge(seconds) {
    if (seconds < 90) return `${Math.round(seconds)}s`;
    if (seconds < 5400) return `${Math.round(seconds / 60)} min`;
    return `${Math.round(seconds / 3600)} h`;
  }

  // Tell the host page what we're showing, so it can mirror freshness in its own UI.
  function notifyParent() {
    if (window.parent === window) return;
    window.parent.postMessage(
      { type: "livedemos:state", dataset: "wikipedia", lang: state.lang, status: state.payload?.status ?? null, last_event_age_s: currentAge() },
      "*",
    );
  }

  setInterval(renderStatus, 1000); // the age keeps counting between polls
  render();
  poll();
})();
