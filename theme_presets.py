"""Site-wide UI theme presets and font catalog for portal appearance.

Each preset defines:
- dark / light surface tokens (eye-friendly contrast, tinted canvases)
- brand_primary / brand_accent (suggested button & link colors)
- swatch chips for the settings picker
"""

from __future__ import annotations

from typing import Any

DEFAULT_THEME_PRESET = "eye-comfort"
DEFAULT_FONT_FAMILY = "Plus Jakarta Sans"

# category: Modern | Professional | Friendly | Technical | Accessible | System
FONT_CATALOG: dict[str, dict[str, Any]] = {
    "Plus Jakarta Sans": {
        "stack": '"Plus Jakarta Sans", system-ui, sans-serif',
        "google": "Plus+Jakarta+Sans:wght@400;500;600;700;800",
        "category": "Modern",
        "tagline": "Default — rounded & clear",
    },
    "Inter": {
        "stack": '"Inter", system-ui, sans-serif',
        "google": "Inter:wght@400;500;600;700",
        "category": "Modern",
        "tagline": "Dense UI standard",
    },
    "DM Sans": {
        "stack": '"DM Sans", system-ui, sans-serif',
        "google": "DM+Sans:wght@400;500;600;700",
        "category": "Modern",
        "tagline": "Geometric & clean",
    },
    "Outfit": {
        "stack": '"Outfit", system-ui, sans-serif',
        "google": "Outfit:wght@400;500;600;700",
        "category": "Modern",
        "tagline": "Contemporary retail",
    },
    "Figtree": {
        "stack": '"Figtree", system-ui, sans-serif',
        "google": "Figtree:wght@400;500;600;700;800",
        "category": "Modern",
        "tagline": "Soft geometric",
    },
    "Manrope": {
        "stack": '"Manrope", system-ui, sans-serif',
        "google": "Manrope:wght@400;500;600;700;800",
        "category": "Modern",
        "tagline": "Bold headlines",
    },
    "Sora": {
        "stack": '"Sora", system-ui, sans-serif',
        "google": "Sora:wght@400;500;600;700",
        "category": "Modern",
        "tagline": "Tech product feel",
    },
    "Poppins": {
        "stack": '"Poppins", system-ui, sans-serif',
        "google": "Poppins:wght@400;500;600;700",
        "category": "Friendly",
        "tagline": "Popular & approachable",
    },
    "Nunito Sans": {
        "stack": '"Nunito Sans", system-ui, sans-serif',
        "google": "Nunito+Sans:wght@400;500;600;700;800",
        "category": "Friendly",
        "tagline": "Soft terminals",
    },
    "Lato": {
        "stack": '"Lato", system-ui, sans-serif',
        "google": "Lato:wght@400;700;900",
        "category": "Friendly",
        "tagline": "Warm humanist",
    },
    "Rubik": {
        "stack": '"Rubik", system-ui, sans-serif',
        "google": "Rubik:wght@400;500;600;700",
        "category": "Friendly",
        "tagline": "Rounded & casual",
    },
    "Mulish": {
        "stack": '"Mulish", system-ui, sans-serif',
        "google": "Mulish:wght@400;500;600;700",
        "category": "Friendly",
        "tagline": "Minimalist sans",
    },
    "Source Sans 3": {
        "stack": '"Source Sans 3", system-ui, sans-serif',
        "google": "Source+Sans+3:wght@400;500;600;700",
        "category": "Professional",
        "tagline": "Adobe workhorse",
    },
    "Open Sans": {
        "stack": '"Open Sans", system-ui, sans-serif',
        "google": "Open+Sans:wght@400;500;600;700",
        "category": "Professional",
        "tagline": "Neutral & trusted",
    },
    "Work Sans": {
        "stack": '"Work Sans", system-ui, sans-serif',
        "google": "Work+Sans:wght@400;500;600;700",
        "category": "Professional",
        "tagline": "Forms & tables",
    },
    "IBM Plex Sans": {
        "stack": '"IBM Plex Sans", system-ui, sans-serif',
        "google": "IBM+Plex+Sans:wght@400;500;600;700",
        "category": "Professional",
        "tagline": "Corporate clarity",
    },
    "Public Sans": {
        "stack": '"Public Sans", system-ui, sans-serif',
        "google": "Public+Sans:wght@400;500;600;700",
        "category": "Professional",
        "tagline": "Government-grade legible",
    },
    "Roboto": {
        "stack": '"Roboto", system-ui, sans-serif',
        "google": "Roboto:wght@400;500;700",
        "category": "Professional",
        "tagline": "Android-familiar",
    },
    "Montserrat": {
        "stack": '"Montserrat", system-ui, sans-serif',
        "google": "Montserrat:wght@400;500;600;700",
        "category": "Professional",
        "tagline": "Urban signage style",
    },
    "Space Grotesk": {
        "stack": '"Space Grotesk", system-ui, sans-serif',
        "google": "Space+Grotesk:wght@400;500;600;700",
        "category": "Technical",
        "tagline": "Startup / dev tools",
    },
    "Lexend": {
        "stack": '"Lexend", system-ui, sans-serif',
        "google": "Lexend:wght@400;500;600;700",
        "category": "Accessible",
        "tagline": "Reading ease focus",
    },
    "Atkinson Hyperlegible": {
        "stack": '"Atkinson Hyperlegible", system-ui, sans-serif',
        "google": "Atkinson+Hyperlegible:wght@400;700",
        "category": "Accessible",
        "tagline": "Low-vision friendly",
    },
    "Raleway": {
        "stack": '"Raleway", system-ui, sans-serif',
        "google": "Raleway:wght@400;500;600;700",
        "category": "Modern",
        "tagline": "Elegant thin strokes",
    },
    "Urbanist": {
        "stack": '"Urbanist", system-ui, sans-serif',
        "google": "Urbanist:wght@400;500;600;700",
        "category": "Modern",
        "tagline": "Low-contrast chic",
    },
    "Albert Sans": {
        "stack": '"Albert Sans", system-ui, sans-serif',
        "google": "Albert+Sans:wght@400;500;600;700",
        "category": "Modern",
        "tagline": "Geometric neutral",
    },
    "Karla": {
        "stack": '"Karla", system-ui, sans-serif',
        "google": "Karla:wght@400;500;600;700",
        "category": "Friendly",
        "tagline": "Grotesque warmth",
    },
    "System UI": {
        "stack": "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        "google": "",
        "category": "System",
        "tagline": "Native OS font",
    },
}

_FONT_ORDER = (
    "Plus Jakarta Sans",
    "Inter",
    "DM Sans",
    "Figtree",
    "Outfit",
    "Poppins",
    "Nunito Sans",
    "Source Sans 3",
    "Open Sans",
    "Work Sans",
    "IBM Plex Sans",
    "Lexend",
    "Atkinson Hyperlegible",
    "Space Grotesk",
    "Manrope",
    "Sora",
    "Lato",
    "Rubik",
    "Roboto",
    "Montserrat",
    "Public Sans",
    "Mulish",
    "Raleway",
    "Urbanist",
    "Albert Sans",
    "Karla",
    "System UI",
)

ALLOWED_FONTS = frozenset(FONT_CATALOG.keys())
ALLOWED_DEFAULT_THEMES = frozenset({"dark", "light", "system"})

_PRESET_ORDER = (
    "eye-comfort",
    "richcom-default",
    "sunset-coral",
    "sapphire-sky",
    "emerald-forest",
    "royal-indigo",
    "lavender-dusk",
    "warm-amber",
    "ocean-teal",
    "rose-blush",
    "plum-velvet",
    "mint-fresh",
    "golden-hour",
    "copper-rust",
    "slate-storm",
    "cherry-blossom",
    "arctic-frost",
    "graphite-minimal",
)

_THEME: dict[str, dict[str, Any]] = {
    "eye-comfort": {
        "label": "Eye Comfort",
        "tagline": "Neutral gray — easy default for any shop",
        "swatch": ["#2a2d35", "#5b8fd9", "#f2f4f8"],
        "brand_primary": "#4a7ec4",
        "brand_accent": "#7aa8e8",
        "dark": {
            "page_bg": "24 26 32",
            "page_fg": "224 226 232",
            "muted": "148 154 168",
            "border": "56 60 72",
            "surface": "34 36 44",
            "surface_2": "44 48 58",
        },
        "light": {
            "page_bg": "240 242 248",
            "page_fg": "28 32 42",
            "muted": "92 100 118",
            "border": "204 210 224",
            "surface": "255 255 255",
            "surface_2": "230 234 242",
        },
    },
    "richcom-default": {
        "label": "Richcom Orange",
        "tagline": "Warm amber brand — classic POS look",
        "swatch": ["#2c2620", "#ea580c", "#fff7ed"],
        "brand_primary": "#ea580c",
        "brand_accent": "#fb923c",
        "dark": {
            "page_bg": "28 22 18",
            "page_fg": "252 236 220",
            "muted": "168 148 128",
            "border": "72 56 44",
            "surface": "42 32 26",
            "surface_2": "54 42 34",
        },
        "light": {
            "page_bg": "255 247 237",
            "page_fg": "67 32 12",
            "muted": "146 98 68",
            "border": "254 215 170",
            "surface": "255 255 255",
            "surface_2": "254 237 220",
        },
    },
    "sunset-coral": {
        "label": "Sunset Coral",
        "tagline": "Peach & coral wash — lively retail",
        "swatch": ["#3a2228", "#f43f5e", "#fff1f2"],
        "brand_primary": "#e11d48",
        "brand_accent": "#fb7185",
        "dark": {
            "page_bg": "36 20 26",
            "page_fg": "255 228 232",
            "muted": "188 132 148",
            "border": "78 44 56",
            "surface": "48 28 36",
            "surface_2": "60 36 46",
        },
        "light": {
            "page_bg": "255 241 242",
            "page_fg": "136 28 48",
            "muted": "190 90 108",
            "border": "254 205 211",
            "surface": "255 255 255",
            "surface_2": "255 228 230",
        },
    },
    "sapphire-sky": {
        "label": "Sapphire Sky",
        "tagline": "Clear sky blues — open & trustworthy",
        "swatch": ["#142a4a", "#2563eb", "#eff6ff"],
        "brand_primary": "#2563eb",
        "brand_accent": "#60a5fa",
        "dark": {
            "page_bg": "14 26 48",
            "page_fg": "220 234 255",
            "muted": "130 158 210",
            "border": "36 58 98",
            "surface": "22 38 68",
            "surface_2": "30 48 82",
        },
        "light": {
            "page_bg": "239 246 255",
            "page_fg": "30 58 138",
            "muted": "96 130 188",
            "border": "191 219 254",
            "surface": "255 255 255",
            "surface_2": "219 234 254",
        },
    },
    "emerald-forest": {
        "label": "Emerald Forest",
        "tagline": "Rich greens — stock & nature retail",
        "swatch": ["#142820", "#059669", "#ecfdf5"],
        "brand_primary": "#059669",
        "brand_accent": "#34d399",
        "dark": {
            "page_bg": "12 28 22",
            "page_fg": "210 250 230",
            "muted": "120 168 148",
            "border": "32 68 54",
            "surface": "18 40 32",
            "surface_2": "26 52 42",
        },
        "light": {
            "page_bg": "236 253 245",
            "page_fg": "6 78 59",
            "muted": "72 140 115",
            "border": "167 243 208",
            "surface": "255 255 255",
            "surface_2": "209 250 229",
        },
    },
    "royal-indigo": {
        "label": "Royal Indigo",
        "tagline": "Deep violet — modern SaaS feel",
        "swatch": ["#1e1a36", "#6366f1", "#eef2ff"],
        "brand_primary": "#6366f1",
        "brand_accent": "#a5b4fc",
        "dark": {
            "page_bg": "22 18 42",
            "page_fg": "228 224 255",
            "muted": "148 140 200",
            "border": "52 46 88",
            "surface": "32 28 58",
            "surface_2": "42 38 72",
        },
        "light": {
            "page_bg": "238 242 255",
            "page_fg": "49 46 129",
            "muted": "108 102 180",
            "border": "199 210 254",
            "surface": "255 255 255",
            "surface_2": "224 231 255",
        },
    },
    "lavender-dusk": {
        "label": "Lavender Dusk",
        "tagline": "Soft purple haze — calm evenings",
        "swatch": ["#2a2438", "#9333ea", "#faf5ff"],
        "brand_primary": "#9333ea",
        "brand_accent": "#c084fc",
        "dark": {
            "page_bg": "30 24 44",
            "page_fg": "243 232 255",
            "muted": "168 148 200",
            "border": "68 54 92",
            "surface": "40 32 58",
            "surface_2": "52 42 72",
        },
        "light": {
            "page_bg": "250 245 255",
            "page_fg": "88 28 135",
            "muted": "152 108 190",
            "border": "233 213 255",
            "surface": "255 255 255",
            "surface_2": "243 232 255",
        },
    },
    "warm-amber": {
        "label": "Warm Amber",
        "tagline": "Honey gold — hospitality & cafes",
        "swatch": ["#2e2414", "#d97706", "#fffbeb"],
        "brand_primary": "#d97706",
        "brand_accent": "#fbbf24",
        "dark": {
            "page_bg": "32 24 12",
            "page_fg": "254 243 199",
            "muted": "186 158 100",
            "border": "72 56 32",
            "surface": "44 34 18",
            "surface_2": "56 44 24",
        },
        "light": {
            "page_bg": "255 251 235",
            "page_fg": "120 72 8",
            "muted": "180 130 60",
            "border": "253 230 138",
            "surface": "255 255 255",
            "surface_2": "254 243 199",
        },
    },
    "ocean-teal": {
        "label": "Ocean Teal",
        "tagline": "Aquamarine depth — fresh & cool",
        "swatch": ["#0f2e32", "#0d9488", "#f0fdfa"],
        "brand_primary": "#0d9488",
        "brand_accent": "#2dd4bf",
        "dark": {
            "page_bg": "10 32 36",
            "page_fg": "204 251 241",
            "muted": "112 180 172",
            "border": "28 72 68",
            "surface": "14 42 46",
            "surface_2": "20 54 58",
        },
        "light": {
            "page_bg": "240 253 250",
            "page_fg": "15 118 110",
            "muted": "72 150 142",
            "border": "153 246 228",
            "surface": "255 255 255",
            "surface_2": "204 251 241",
        },
    },
    "rose-blush": {
        "label": "Rose Blush",
        "tagline": "Dusty rose — boutique & beauty",
        "swatch": ["#361c28", "#db2777", "#fdf2f8"],
        "brand_primary": "#db2777",
        "brand_accent": "#f472b6",
        "dark": {
            "page_bg": "36 18 28",
            "page_fg": "252 231 243",
            "muted": "188 120 158",
            "border": "76 40 58",
            "surface": "48 26 38",
            "surface_2": "60 34 48",
        },
        "light": {
            "page_bg": "253 242 248",
            "page_fg": "157 23 77",
            "muted": "190 100 150",
            "border": "251 207 232",
            "surface": "255 255 255",
            "surface_2": "252 231 243",
        },
    },
    "plum-velvet": {
        "label": "Plum Velvet",
        "tagline": "Wine plum — premium executive",
        "swatch": ["#2a1420", "#9d174d", "#fce7f3"],
        "brand_primary": "#9d174d",
        "brand_accent": "#ec4899",
        "dark": {
            "page_bg": "34 14 24",
            "page_fg": "252 226 238",
            "muted": "180 110 140",
            "border": "74 36 54",
            "surface": "46 22 34",
            "surface_2": "58 28 42",
        },
        "light": {
            "page_bg": "253 242 248",
            "page_fg": "131 24 67",
            "muted": "168 88 120",
            "border": "244 187 212",
            "surface": "255 255 255",
            "surface_2": "252 231 240",
        },
    },
    "mint-fresh": {
        "label": "Mint Fresh",
        "tagline": "Bright mint — health & wellness",
        "swatch": ["#143228", "#10b981", "#d1fae5"],
        "brand_primary": "#10b981",
        "brand_accent": "#6ee7b7",
        "dark": {
            "page_bg": "14 32 26",
            "page_fg": "209 250 229",
            "muted": "118 180 158",
            "border": "32 72 58",
            "surface": "20 42 34",
            "surface_2": "28 54 44",
        },
        "light": {
            "page_bg": "236 253 245",
            "page_fg": "4 120 87",
            "muted": "80 160 130",
            "border": "167 243 208",
            "surface": "255 255 255",
            "surface_2": "209 250 229",
        },
    },
    "golden-hour": {
        "label": "Golden Hour",
        "tagline": "Sunlit yellow cream — upbeat daytime",
        "swatch": ["#30280c", "#ca8a04", "#fefce8"],
        "brand_primary": "#ca8a04",
        "brand_accent": "#facc15",
        "dark": {
            "page_bg": "34 28 8",
            "page_fg": "254 249 195",
            "muted": "190 170 90",
            "border": "76 64 28",
            "surface": "46 38 14",
            "surface_2": "58 48 20",
        },
        "light": {
            "page_bg": "254 252 232",
            "page_fg": "113 88 12",
            "muted": "162 138 48",
            "border": "254 240 138",
            "surface": "255 255 255",
            "surface_2": "254 249 195",
        },
    },
    "copper-rust": {
        "label": "Copper Rust",
        "tagline": "Terracotta rust — artisan & hardware",
        "swatch": ["#321c14", "#c2410c", "#ffedd5"],
        "brand_primary": "#c2410c",
        "brand_accent": "#f97316",
        "dark": {
            "page_bg": "34 20 14",
            "page_fg": "255 237 213",
            "muted": "190 140 110",
            "border": "78 48 36",
            "surface": "48 30 22",
            "surface_2": "60 38 28",
        },
        "light": {
            "page_bg": "255 247 237",
            "page_fg": "124 45 18",
            "muted": "180 110 80",
            "border": "254 215 170",
            "surface": "255 255 255",
            "surface_2": "255 237 213",
        },
    },
    "slate-storm": {
        "label": "Slate Storm",
        "tagline": "Blue-slate storm — tech & logistics",
        "swatch": ["#1a2432", "#475569", "#f1f5f9"],
        "brand_primary": "#475569",
        "brand_accent": "#64748b",
        "dark": {
            "page_bg": "20 28 38",
            "page_fg": "226 232 240",
            "muted": "140 158 178",
            "border": "48 62 80",
            "surface": "30 40 52",
            "surface_2": "40 52 66",
        },
        "light": {
            "page_bg": "241 245 249",
            "page_fg": "30 41 59",
            "muted": "100 116 139",
            "border": "203 213 225",
            "surface": "255 255 255",
            "surface_2": "226 232 240",
        },
    },
    "cherry-blossom": {
        "label": "Cherry Blossom",
        "tagline": "Pink blossom spring — fashion & gifts",
        "swatch": ["#3a1828", "#ec4899", "#fdf2f8"],
        "brand_primary": "#ec4899",
        "brand_accent": "#f9a8d4",
        "dark": {
            "page_bg": "40 16 30",
            "page_fg": "253 242 248",
            "muted": "200 120 168",
            "border": "82 38 62",
            "surface": "52 24 40",
            "surface_2": "64 32 50",
        },
        "light": {
            "page_bg": "253 242 248",
            "page_fg": "190 24 93",
            "muted": "219 100 160",
            "border": "251 207 232",
            "surface": "255 255 255",
            "surface_2": "252 231 243",
        },
    },
    "arctic-frost": {
        "label": "Arctic Frost",
        "tagline": "Icy cyan frost — clinical & clean",
        "swatch": ["#102030", "#0284c7", "#f0f9ff"],
        "brand_primary": "#0284c7",
        "brand_accent": "#38bdf8",
        "dark": {
            "page_bg": "12 24 40",
            "page_fg": "224 242 254",
            "muted": "130 170 210",
            "border": "32 56 88",
            "surface": "18 34 56",
            "surface_2": "26 44 70",
        },
        "light": {
            "page_bg": "240 249 255",
            "page_fg": "12 74 110",
            "muted": "90 140 180",
            "border": "186 230 253",
            "surface": "255 255 255",
            "surface_2": "224 242 254",
        },
    },
    "graphite-minimal": {
        "label": "Graphite Minimal",
        "tagline": "Pure neutral zinc — no color bias",
        "swatch": ["#222228", "#71717a", "#fafafa"],
        "brand_primary": "#52525b",
        "brand_accent": "#a1a1aa",
        "dark": {
            "page_bg": "24 24 28",
            "page_fg": "228 228 232",
            "muted": "150 150 160",
            "border": "58 58 66",
            "surface": "36 36 40",
            "surface_2": "46 46 52",
        },
        "light": {
            "page_bg": "250 250 250",
            "page_fg": "39 39 42",
            "muted": "113 113 122",
            "border": "228 228 231",
            "surface": "255 255 255",
            "surface_2": "244 244 245",
        },
    },
}

# Legacy preset ids → closest new theme (existing DB values keep working)
_LEGACY_PRESET_ALIASES: dict[str, str] = {
    "soft-twilight": "sapphire-sky",
    "midnight-pro": "sapphire-sky",
    "emerald-enterprise": "emerald-forest",
    "ocean-cyan": "ocean-teal",
    "rose-executive": "rose-blush",
}

THEME_PRESETS: dict[str, dict[str, Any]] = _THEME


def normalize_theme_preset(key: str | None) -> str:
    k = (key or "").strip().lower()
    if k in THEME_PRESETS:
        return k
    if k in _LEGACY_PRESET_ALIASES:
        return _LEGACY_PRESET_ALIASES[k]
    return DEFAULT_THEME_PRESET


def normalize_font_family(name: str | None) -> str:
    n = (name or "").strip()
    return n if n in ALLOWED_FONTS else DEFAULT_FONT_FAMILY


def normalize_default_theme(mode: str | None) -> str:
    m = (mode or "").strip().lower()
    return m if m in ALLOWED_DEFAULT_THEMES else "dark"


def font_css_stack(name: str | None) -> str:
    return FONT_CATALOG[normalize_font_family(name)]["stack"]


def google_fonts_url(*families: str) -> str:
    slugs: list[str] = []
    for fam in families:
        slug = FONT_CATALOG.get(normalize_font_family(fam), {}).get("google") or ""
        if slug and slug not in slugs:
            slugs.append(slug)
    if not slugs:
        return ""
    return "https://fonts.googleapis.com/css2?family=" + "&family=".join(slugs) + "&display=swap"


def theme_presets_for_template() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key in _PRESET_ORDER:
        if key not in THEME_PRESETS:
            continue
        p = THEME_PRESETS[key]
        out.append(
            {
                "id": key,
                "label": p["label"],
                "tagline": p["tagline"],
                "swatch": p.get("swatch") or [],
                "brand_primary": p.get("brand_primary") or "#ea580c",
                "brand_accent": p.get("brand_accent") or "#fb923c",
                "dark": p["dark"],
                "light": p["light"],
                "recommended": key == DEFAULT_THEME_PRESET,
            }
        )
    return out


def fonts_for_template() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key in _FONT_ORDER:
        if key not in FONT_CATALOG:
            continue
        f = FONT_CATALOG[key]
        out.append(
            {
                "id": key,
                "stack": f["stack"],
                "category": f.get("category") or "Modern",
                "tagline": f.get("tagline") or "",
                "recommended": key == DEFAULT_FONT_FAMILY,
            }
        )
    return out


def font_categories_for_template() -> list[str]:
    seen: list[str] = []
    for key in _FONT_ORDER:
        if key not in FONT_CATALOG:
            continue
        cat = FONT_CATALOG[key].get("category") or "Modern"
        if cat not in seen:
            seen.append(cat)
    return seen
