# Headrace - brand

The visual identity for Headrace. This directory is the source of truth; if a rendered asset and
this spec disagree, the spec wins.

## 1. Concept

A *headrace* is the engineered channel that carries a stream to where work is extracted - the mill,
the turbine. That is the project: carry telemetry to where it is aggregated.

The mark is the letter **H**: two banks, and between them the **race** - drawn as a shallow wave, so
it reads as water moving through the channel. It is a letterform, so it works at any size and as an
avatar or favicon without redrawing. The mark is **monochrome**: the shape carries the meaning, so
the single color is free to just be the brand.

## 2. The mark

A 96 x 96 grid; everything derives from it. Pure geometry, no font dependency, one color.

| Element      | Spec                                                        |
| ------------ | ----------------------------------------------------------- |
| Grid/viewBox | `0 0 96 96`                                                 |
| Banks        | `M30 20 V76`, `M66 20 V76`; stroke **11**, round cap        |
| Race         | `M30 45 Q39 39 48 45 T66 45` (shallow wave); stroke **11**  |

The crossbar sits at `y=45`, optically just above center. All three strokes share one color; never
fill the mark, and keep the strokes round-capped.

## 3. Variants

| File            | Use it for                                                                  |
| --------------- | --------------------------------------------------------------------------- |
| `logo.svg`      | default on light grounds (teal)                                             |
| `logo-dark.svg` | dark grounds (bright teal)                                                  |
| `logo-mono.svg` | one-color / neutral contexts; inherits `currentColor`                       |
| `favicon.svg`   | browser tab; stroke bumped, ground-agnostic teal for 16 px                  |
| `avatar.svg`    | GitHub org / social avatar; white mark on a teal tile, padded for round crop |

`logo.png`, `logo-dark.png`, and `avatar.png` (512 px) plus `favicon-16.png` / `favicon-32.png` are
rendered from the SVGs (see [§7](#7-regenerating-assets)); upload `avatar.png` as the GitHub org
logo. Wordmark lockups and `favicon.ico` are the next step.

## 4. Color

The mark is monochrome, so the palette is small. Teal is the brand; the neutral is only for
`logo-mono.svg` where the mark must match surrounding text.

| Role         | Name  | Light     | Dark      |
| ------------ | ----- | --------- | --------- |
| Mark (brand) | Teal  | `#0E9AA0` | `#2DD4BF` |
| Mark, mono   | Ink   | `#0F1B24` | `#E6EDF3` |
| Surface      | Paper | `#F5F7F9` | `#0B1014` |

Ground-agnostic teal (favicon): `#14B8A6` - one value that holds up on both a light and a dark tab.
The avatar tile is the brand teal `#0E9AA0` with a paper `#F5F7F9` mark.

## 5. Clear space and minimum size

- **Clear space:** at least the stroke width on all sides.
- **Minimum size:** **16 px** (favicon-verified); the wave flattens to a bar at that size, which is
  fine. Below 16 px, prefer `avatar.svg` (the filled tile).

## 6. Usage - do and don't

**Do** use the provided SVGs; recolor `logo-mono.svg` via `currentColor`; keep the mark one color.

**Don't** fill the mark; use more than one color in it; add a second accent; skew, rotate, or
re-space the strokes; flatten the wave to a straight bar except by rendering small.

Embedding in a themed README/docs: use a `<picture>` that swaps `logo.svg` / `logo-dark.svg` by
`prefers-color-scheme` (an `<img>` cannot inherit `currentColor`).

## 7. Regenerating assets

The SVGs are the source of truth. The PNGs are rendered from them with the
[resvg](https://github.com/linebender/resvg) CLI:

```sh
cargo install resvg   # once
./render.sh           # run from brand/; re-renders the PNGs from the SVGs
```

Edit the geometry in the SVGs (or the spec above), never a rendered PNG. The wordmark lockup and
`favicon.ico` come next.
