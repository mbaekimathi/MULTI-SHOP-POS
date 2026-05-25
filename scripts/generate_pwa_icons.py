"""
Generate PWA PNG icons (192, 512) for the brand.

Why: Chrome's `beforeinstallprompt` installability checks reliably trigger
only when the manifest exposes raster PNG icons at 192x192 and 512x512.
SVG-only manifests work in some flows but are inconsistent.

Run with:  python scripts/generate_pwa_icons.py
Writes:    static/icons/app-icon-192.png
           static/icons/app-icon-512.png
           static/icons/app-icon-192-maskable.png
           static/icons/app-icon-512-maskable.png
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "static" / "icons"


# Brand palette (matches templates/base.html defaults).
PRIMARY = (249, 115, 22, 255)   # orange-500
ACCENT = (251, 146, 60, 255)    # orange-400
DEEP = (234, 88, 12, 255)       # orange-600
INK = (11, 18, 32, 235)         # near-black ink


def _gradient_bg(size: int) -> Image.Image:
    """Diagonal brand gradient on full canvas."""
    img = Image.new("RGBA", (size, size), PRIMARY)
    grad = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    px = grad.load()
    for y in range(size):
        for x in range(size):
            t = (x + y) / (2 * size)
            r = int(ACCENT[0] * (1 - t) + DEEP[0] * t)
            g = int(ACCENT[1] * (1 - t) + DEEP[1] * t)
            b = int(ACCENT[2] * (1 - t) + DEEP[2] * t)
            px[x, y] = (r, g, b, 255)
    img.alpha_composite(grad)

    highlight = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    hl = ImageDraw.Draw(highlight)
    cx, cy = int(size * 0.30), int(size * 0.22)
    max_r = int(size * 0.85)
    step = max(2, size // 256)
    for r in range(max_r, 0, -step):
        alpha = int(80 * (1 - r / max_r))
        if alpha <= 0:
            continue
        hl.ellipse((cx - r, cy - r, cx + r, cy + r), fill=(255, 255, 255, alpha))
    highlight = highlight.filter(ImageFilter.GaussianBlur(radius=size * 0.04))
    img.alpha_composite(highlight)
    return img


def _rounded_mask(size: int, radius: int) -> Image.Image:
    mask = Image.new("L", (size, size), 0)
    md = ImageDraw.Draw(mask)
    md.rounded_rectangle((0, 0, size - 1, size - 1), radius=radius, fill=255)
    return mask


def _load_bold_font(size_px: int) -> ImageFont.ImageFont:
    candidates = [
        "arialbd.ttf",
        "Arial Bold.ttf",
        "ArialBd.ttf",
        "DejaVuSans-Bold.ttf",
        "LiberationSans-Bold.ttf",
        "seguibl.ttf",
        "Helvetica-Bold.ttf",
    ]
    for name in candidates:
        try:
            return ImageFont.truetype(name, size_px)
        except OSError:
            continue
    return ImageFont.load_default()


def _draw_letter(img: Image.Image, letter: str, scale: float) -> None:
    """Draw centered bold letter into img using the given scale of canvas size."""
    size = img.size[0]
    draw = ImageDraw.Draw(img)
    font = _load_bold_font(int(size * scale))
    bbox = draw.textbbox((0, 0), letter, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    x = (size - text_w) // 2 - bbox[0]
    y = (size - text_h) // 2 - bbox[1] - int(size * 0.02)

    shadow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    sdraw = ImageDraw.Draw(shadow)
    sdraw.text((x + 4, y + 6), letter, fill=(0, 0, 0, 90), font=font)
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=size * 0.012))
    img.alpha_composite(shadow)

    draw.text((x, y), letter, fill=INK, font=font)


def render_any(size: int) -> Image.Image:
    """Rounded-rect 'any' icon — design inside ~80% of canvas with rounded corners."""
    canvas = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    inner_size = size  # background fills the canvas for visual richness
    bg = _gradient_bg(inner_size)
    radius = int(size * 0.22)
    mask = _rounded_mask(inner_size, radius)
    canvas.paste(bg, (0, 0), mask)
    _draw_letter(canvas, "P", scale=0.62)
    return canvas


def render_maskable(size: int) -> Image.Image:
    """Maskable icon — full-bleed background, design in 60% safe zone (centered)."""
    canvas = _gradient_bg(size)
    safe = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    _draw_letter(safe, "P", scale=0.46)  # ~60% safe-zone friendly
    canvas.alpha_composite(safe)
    return canvas


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outputs = []
    for size in (192, 512):
        any_img = render_any(size)
        any_path = OUT_DIR / f"app-icon-{size}.png"
        any_img.save(any_path, format="PNG", optimize=True)
        outputs.append(any_path)

        m_img = render_maskable(size)
        m_path = OUT_DIR / f"app-icon-{size}-maskable.png"
        m_img.save(m_path, format="PNG", optimize=True)
        outputs.append(m_path)

    for p in outputs:
        rel = p.relative_to(REPO_ROOT)
        kb = p.stat().st_size / 1024
        print(f"  wrote {rel}  ({kb:.1f} KB)")
    print(f"Done. {len(outputs)} icons in {OUT_DIR.relative_to(REPO_ROOT)}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
