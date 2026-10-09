# Design

The widget follows the portfolio site's design: its colours, type, spacing and copy. The site and its design prototype live in a separate, private repository; this repo only holds the live demo itself.

What the widget takes from the design, and where it deliberately differs:

- **No sample data.** Every number comes from the API.
- **Freshness** is measured from the source timestamp against the server's clock, not simulated.
- **Missing minutes** draw as an empty slot instead of a bar, so a gap never looks like a quiet minute.
- **Definitions** sit behind one keyboard-accessible "What these numbers mean" button rather than a hover-only tooltip. One button, not one per number, so it's an easy target on a phone.
- **Phone first.** The three numbers stay side by side at every width, the language pills stay on one row, and every control and article row is at least 44 px tall.
- **Routes are versioned:** `/v1/wikipedia/live.json` and `/v1/wikipedia/activity`.

`web/index.html` shows the widget inside a stand-in host app ("Northwind"), the way the live demos page presents it.
