# Figures

`desk.html` is the figure the home page mounts in its plate: a wooden in-tray, a
punch clock and a line of tags. It is one self-contained HTML file — no build
step, no request, no dependency — and it draws itself.

## What it shows

| Object | In moontask |
|:--|:--|
| the tray of cards | a queue: messages waiting for a worker |
| the clock and its press | a worker: claim, run, settle — and the beat that sends on a rhythm |
| the line of tags | what finished, in order; three of them are the parts of a chord |

Each button is one call the library makes:

- **Send a card** — `publish`: one message on the queue, waiting.
- **Run the worker** — `claim` then `settle`: the card is taken off the queue,
  stamped, and either sealed and hung on the line or sent back with an eta from
  the backoff. The first card of the demo is the one that fails, so the retry is
  something you see rather than something you are told.
- **Take a tag off** — the tag leaves the line; the result stays in the store.

The figure plays those moves by itself until someone touches it, then hands over.

## How it is drawn

Not a 3D engine: every solid is three flat quads — its top, its +x face and its
+y face — emitted in the one order this camera sees them, so the file's order is
the depth order and nothing can be drawn through anything else. A point is
`((x - y)·cos30, (x + y)·0.5 - z)`; the whole scene is built from `box()` and a
`disc()`, and the finished drawing is framed from its own `getBBox()`.

Colours are CSS custom properties, so the figure follows the page's theme —
`prefers-color-scheme` by default, or the light/dark switch in the site header,
which writes one key that this file reads too. Paper stays paper in both themes;
only the desk, the case and the inks change.

To change the scene, edit the constants at the top of the script
(`BOARD`, `TRAY`, `CLOCK`, `SHELF`, `LINE`) and the poses in `poses`. The demo's
choreography lives in `send()`, `run()` and `take()`.
