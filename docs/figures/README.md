# Figures

`desk.html` is the figure the home page mounts in its plate: a wooden in-tray, a
punch clock and a line of tags, drawn isometrically and pressable.

It is a [Colorfigures](https://april-zhu.com/colorfigures) page — one HTML file
with the drawing engine inlined — so the whole thing is ~90 KB and makes no
requests. Turn it by dragging, switch **See-through** to watch the mainspring,
the gear train and the escape wheel work, and use the three buttons to send a
card, punch it and take one off the line.

What the objects stand for:

| Object | In moontask |
|:--|:--|
| the tray | a queue: messages waiting for a worker |
| the clock and its press | a worker, and the beat that sends on a rhythm |
| the line of tags | a chain, a group, a chord — work that finished, in order |

## Editing it

`desk.src.html` is the source; it loads `core.js` next to it. After a change,
build the single file again:

```sh
python3 /path/to/colorfigures/scripts/build.py desk.src.html desk.html
```

`core.js` is the unmodified Colorfigures engine (MIT), kept here so the source
page runs straight from `file://`. The built page has it inlined and does not
need it.

The figure accepts `?embed=1` (drops its own header and credit, for the plate),
`?xr=1&labels=1` (see-through with labels), `?yaw=…&pitch=…` and
`?press=Label@ms,…` to drive it from a URL, which is how it is checked.

Drawn with Colorfigures, MIT licensed.
