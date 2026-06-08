#!/usr/bin/env python3
from __future__ import annotations

import argparse
import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def read_section(title: str, path: Path) -> list[str]:
    if not path.exists():
        return [f"{title}: missing {path}"]
    body = path.read_text(errors="replace").strip()
    lines = [title, "-" * len(title)]
    lines.extend(body.splitlines() if body else ["<empty>"])
    return lines


def wrap_lines(lines: list[str], width: int) -> list[str]:
    wrapped: list[str] = []
    for line in lines:
        if len(line) <= width:
            wrapped.append(line)
            continue
        wrapped.extend(textwrap.wrap(line, width=width, replace_whitespace=False) or [""])
    return wrapped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--title", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("sections", nargs="+", help="title=path")
    args = parser.parse_args()

    font = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 15)
    title_font = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 22)
    width = 1500
    margin = 32
    line_h = 22

    lines = [args.title, ""]
    for section in args.sections:
        title, raw_path = section.split("=", 1)
        lines.extend(read_section(title, Path(raw_path)))
        lines.append("")

    lines = wrap_lines(lines, 150)
    height = max(600, margin * 2 + 34 + len(lines) * line_h)
    image = Image.new("RGB", (width, height), "#f8fafc")
    draw = ImageDraw.Draw(image)

    y = margin
    draw.text((margin, y), args.title, font=title_font, fill="#0f172a")
    y += 38
    for line in lines[2:]:
        fill = "#0f172a"
        if set(line) == {"-"}:
            fill = "#64748b"
        elif line.startswith("{"):
            fill = "#14532d"
        elif "Running" in line or "Ready" in line or "active" in line:
            fill = "#1d4ed8"
        draw.text((margin, y), line, font=font, fill=fill)
        y += line_h

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    image.save(args.output)


if __name__ == "__main__":
    main()
