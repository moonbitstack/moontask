# Figures

`floor.html` is the figure the home page mounts in its plate: a workbench in
perspective — a tray of cards, a press, a line of finished cards, and a dial that
keeps the run's own clock. One self-contained HTML file: no build step, no
network request, one script beside it.

## What it shows

| Object | In moontask |
|:--|:--|
| the tray of cards | a queue: messages waiting for a worker |
| the press | a worker: claim, run, settle — and the stamp is the stored result |
| the return rail | a retry: the card goes back with an eta from the backoff |
| the line of hanging cards | what finished, in the order it finished |
| the dial | the beat, and the clock the run was driven by |

## Where the movement comes from

Not from this file. `src/examples/05-floor` drives a fixed scenario on the real
library — a card that runs, a card that fails once and comes back on a one-second
backoff, three cards at once, and a card revoked before anyone runs it — waits for
each state the store reports, and prints one event per line.
`scripts/gen_floor.py` runs that example and writes `docs/assets/floor.json` and
`figures/floor-data.js`; the figure replays those events and nothing else. CI
re-runs the generator, so a page that drifts from the library fails the build.

## How it is drawn

A canvas and about four hundred lines: a camera on a sphere around the bench, a
perspective projection, boxes as six quads each, and a painter's algorithm that
sorts every face by its depth. The whole scene is `box()` and `quad()`; a card's
position is a pure function of the recording's clock, so a still of any instant is
the same picture as watching it arrive (`?at=<ms>` seeks to one).

Colours are CSS custom properties, so the figure follows the page's theme —
`prefers-color-scheme` by default, or the switch in the site header, which writes
one key that this file reads too. Paper stays paper in both themes; only the bench
and the inks change.
