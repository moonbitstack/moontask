// Written by scripts/gen_showcase.py from a run of the examples. Do not edit.
window.TRANSCRIPTS = {
  "queue": {
    "title": "A queue end to end",
    "command": "moon run src/examples/01-queue",
    "lines": [
      "queued  greet -> 1700000000000:1:greet",
      "answer  hello ada, attempt 1",
      "queued  flaky -> 1700000000000:2:flaky",
      "state   RETRY",
      "state   SUCCESS",
      "attempts 2"
    ]
  },
  "canvas": {
    "title": "A chain, a group and a chord",
    "command": "moon run src/examples/02-canvas",
    "lines": [
      "chain   30",
      "order   step 1, step 2, step 3",
      "group   [40,50]",
      "chord   60"
    ]
  },
  "fanout": {
    "title": "A map, a starmap and a chunk",
    "command": "moon run src/examples/07-fanout",
    "lines": [
      "map     [10,20,30]",
      "starmap [3,7]",
      "chunks  [[10,20],[30,40],[50]]",
      "order   1, 2, 3, 1, 2, 3, 4, 5"
    ]
  }
};
