#!/usr/bin/env python3
"""Render docs/api.html from the generated interfaces and the source comments.

The input is `pkg.generated.mbti`, which `moon info` writes next to each
package, plus the `///` blocks in the sources: the interface says what exists and
the comment says why. Nothing here reads the implementation.

Run from the repository root:

    python3 scripts/gen_api.py
"""

from __future__ import annotations

import html
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGES = [
    ("moontask", "src/pkg.generated.mbti", "src"),
    ("moontask/spec", "src/spec/pkg.generated.mbti", "src/spec"),
    ("moontask/schedule", "src/schedule/pkg.generated.mbti", "src/schedule"),
    ("moontask/flow", "src/flow/pkg.generated.mbti", "src/flow"),
    ("moontask/wait", "src/wait/pkg.generated.mbti", "src/wait"),
]

SECTIONS = [
    ("// Values", "Functions and values"),
    ("// Errors", "Errors"),
    ("// Types and methods", "Types and methods"),
    ("// Type aliases", "Type aliases"),
    ("// Traits", "Traits"),
]

DECL = re.compile(
    r"^(?:pub(?:\([a-z]+\))?\s+)?(fn|suberror|struct|enum|type|trait|impl|let|const|using)\b\s*(.*)$"
)
MEMBER = re.compile(r"^(\w+)\s*:\s*(.+)$")


def comments_by_name(sources: list[Path]) -> dict[str, str]:
    """The `///` block that precedes each declaration, keyed by what it declares."""
    out: dict[str, str] = {}
    for source in sources:
        lines = source.read_text(encoding="utf-8").split("\n")
        block: list[str] = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("///"):
                block.append(stripped[3:].lstrip())
                continue
            if stripped.startswith("#"):
                # An attribute sits between the comment and the declaration, and
                # is not part of either.
                continue
            if stripped and block:
                name = declared_name(stripped)
                if name and name not in out:
                    out[name] = "\n".join(block).strip()
                block = []
            elif not stripped:
                block = []
    return out


def declared_name(line: str) -> str | None:
    match = DECL.match(line)
    if not match:
        return None
    kind, rest = match.group(1), match.group(2).strip()
    if kind in ("struct", "enum", "suberror", "trait"):
        return rest.split()[0].rstrip("{").strip() if rest else None
    if kind in ("fn", "let", "const"):
        rest = re.sub(r"^\[[^\]]*\]\s*", "", rest)
        name = re.split(r"[(=\s]", rest, maxsplit=1)[0]
        return name.strip()
    return None


def parse_mbti(path: Path) -> list[tuple[str, list[str], str]]:
    """(section, signature lines, doc key) for every public entry."""
    entries: list[tuple[str, list[str], str]] = []
    section = ""
    lines = path.read_text(encoding="utf-8").split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        for marker, title in SECTIONS:
            if line.startswith(marker):
                section = title
        if line.startswith("pub "):
            chunk = [line]
            # A struct, enum or error carries its members; every other
            # declaration is one line, and the next one follows it directly.
            if line.rstrip().endswith("{"):
                i += 1
                while i < len(lines) and not lines[i].startswith("}"):
                    chunk.append(lines[i])
                    i += 1
                if i < len(lines):
                    chunk.append(lines[i])
            entries.append((section, chunk, key_of(chunk)))
            i += 1
            continue
        i += 1
    return entries


def key_of(chunk: list[str]) -> str:
    first = chunk[0]
    match = DECL.match(first)
    if not match:
        return ""
    kind, rest = match.group(1), match.group(2).strip()
    if kind in ("struct", "enum", "suberror", "trait"):
        return rest.split()[0].rstrip("{").strip()
    if kind in ("fn", "let", "const"):
        rest = re.sub(r"^\[[^\]]*\]\s*", "", rest)
        return re.split(r"[(=\s]", rest, maxsplit=1)[0]
    return ""


def render_entry(section: str, chunk: list[str], doc: str) -> str:
    head = chunk[0]
    body = chunk[1:]
    match = DECL.match(head)
    kind = match.group(1) if match else "fn"
    out: list[str] = ['<article class="api-entry">']
    if kind in ("struct", "enum", "suberror"):
        out.append(f'<pre class="sig">{html.escape(head)}</pre>')
        if body:
            members = []
            for line in body:
                member = MEMBER.match(line.strip())
                if member:
                    members.append(
                        f'<li><code>{html.escape(member.group(1))}</code>'
                        f'<span>{html.escape(member.group(2))}</span></li>'
                    )
                else:
                    members.append(f'<li><code>{html.escape(line.strip())}</code></li>')
            out.append('<ul class="members">' + "".join(members) + "</ul>")
    else:
        signature = html.escape(head)
        for line in body:
            if line.startswith("pub "):
                break
            signature += "\n" + html.escape(line)
        out.append(f'<pre class="sig">{signature}</pre>')
    if doc:
        out.append(f'<div class="doc">{markdownish(doc)}</div>')
    out.append("</article>")
    return "\n".join(out)


def markdownish(text: str) -> str:
    paragraphs = []
    for block in text.split("\n\n"):
        block = block.strip()
        if not block:
            continue
        if block.startswith("- "):
            items = [
                "<li>" + inline(line[2:].strip()) + "</li>"
                for line in block.split("\n")
                if line.strip().startswith("- ")
            ]
            paragraphs.append("<ul>" + "".join(items) + "</ul>")
            continue
        if block.startswith("```"):
            body = "\n".join(block.split("\n")[1:]).rstrip("`").rstrip()
            paragraphs.append(f"<pre class=\"doc-code\">{html.escape(body)}</pre>")
            continue
        paragraphs.append("<p>" + inline(block.replace("\n", " ")) + "</p>")
    return "\n".join(paragraphs)


def inline(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    escaped = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', escaped)
    return escaped


def verify(total: int, rendered: str) -> None:
    """Every public declaration in the interfaces must reach the page."""
    declared = 0
    for _, mbti, _ in PACKAGES:
        declared += sum(
            1 for line in (ROOT / mbti).read_text(encoding="utf-8").split("\n") if line.startswith("pub ")
        )
    if declared != total:
        raise SystemExit(f"{declared} public declarations but {total} entries rendered")
    missing = [
        line
        for _, mbti, _ in PACKAGES
        for line in (ROOT / mbti).read_text(encoding="utf-8").split("\n")
        if line.startswith("pub ") and html.escape(line) not in rendered
    ]
    if missing:
        raise SystemExit("not on the page://n" + "\n".join(missing[:10]))


def main() -> int:
    total = 0
    parts: list[str] = []
    for name, mbti, source in PACKAGES:
        docs = comments_by_name(sorted((ROOT / source).glob("*.mbt")))
        entries = parse_mbti(ROOT / mbti)
        total += len(entries)
        grouped: dict[str, list[str]] = {}
        for section, chunk, key in entries:
            grouped.setdefault(section, []).append(render_entry(section, chunk, docs.get(key, "")))
        parts.append(f'<section class="api-package" id="pkg-{name.replace("/", "-")}">')
        parts.append(f"<h2>{html.escape(name)}</h2>")
        for section, rendered in grouped.items():
            parts.append(f'<h3>{html.escape(section)}</h3>')
            parts.extend(rendered)
        parts.append("</section>")

    page = (ROOT / "docs" / "api.html").read_text(encoding="utf-8") if (ROOT / "docs" / "api.html").exists() else SHELL
    body = "\n".join(parts)
    page = re.sub(
        r'<!-- BEGIN API -->.*?<!-- END API -->',
        "<!-- BEGIN API -->\n" + body + "\n<!-- END API -->",
        page,
        flags=re.S,
    )
    verify(total, page)
    (ROOT / "docs" / "api.html").write_text(page, encoding="utf-8", newline="\n")
    print(f"{total} public entries in {len(PACKAGES)} packages -> docs/api.html")
    return 0


SHELL = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>moontask · API</title>
<link rel="stylesheet" href="assets/style.css">
</head>
<body>
<header class="top">
  <a class="brand" href="./index.html">moontask</a>
  <nav><a href="./index.html">Home</a><a href="./guide.html">Guide</a><a href="./api.html" aria-current="page">API</a><a href="./showcase.html">Showcase</a><a href="https://github.com/moonbitstack/moontask">GitHub</a></nav>
</header>
<main class="api">
<h1>API reference</h1>
<p class="lede">Every public item of the module, generated from the interfaces <code>moon info</code> writes.</p>
<!-- BEGIN API -->
<!-- END API -->
</main>
</body>
</html>
"""

if __name__ == "__main__":
    sys.exit(main())
