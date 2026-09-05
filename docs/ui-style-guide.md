# UI style guide

## Approved direction

**A — cool slate + teal**, selected on 2026-09-05, is the visual direction for
the `airy-v0` light theme. This is a UI style guide, not a separate brand identity.
The editor, sidebar, tables, dialogs, and compatibility catalog should look like
one product: **the same semantic role receives the same visual treatment**.

The executable theme lives in [`web/src/style.css`](../web/src/style.css), in
`:root[data-design="airy-v0"][data-theme="light"]`. Prefer its semantic CSS
variables over literal colors in new component rules. Some existing island and
table rules still use literals; their presence is not permission to introduce
another local palette. When touching them, reuse an appropriate existing token,
or define a shared semantic token if the role genuinely differs.

## Palette

| Role | Color | Existing token |
| --- | --- | --- |
| Page, inputs, raised white surfaces | `#FFFFFF` | `--canvas`, `--control`, `--panel`, `--popup` |
| Sidebar and gray islands | `#EDF1F4` | `--sidebar`, `--panel2` |
| Subtle surfaces, alternate table rows | `#F1F5F8` | `--surface-soft`, `--control-hover` |
| Neutral hover and subtle separators | `#E8EDF1` | `--surface-hover`, `--line-inner` |
| Borders | `#CFD8DE` | `--field-border`, `--line-strong`, `--route-line` |
| Row separators | `#DCE3E8` | `--line` |
| Primary text | `#0B1220` | `--text-primary` |
| Secondary text | `#202938` | `--text-secondary` |
| Muted text | `#64717D` | `--muted` |
| Muted icons and placeholders | `#89939D` | `--icon-muted`, `--placeholder` |
| Primary action and focus accent | `#0D9488` | `--blue` (historical token name; the color is teal) |
| Accent hover | `#0F7F76` | `--blue-hover` |
| Selected/active tinted surface | `#E5F2F0` | `--surface-selected`, `--surface-active` |
| Disabled text / surface / border | `#89939D` / `#E8EDF1` / `#CFD8DE` | `--disabled-text`, `--disabled-surface`, `--disabled-border` |

Focus rings use `--focus-ring` (teal at 42% opacity). Shadows use `--shadow`
(near-black slate at 12% opacity), not green-tinted shadows.

## Component rules

- Keep the page white and the three delivery islands slate gray, with white
  fields. Parser details continue the source island rather than introducing a
  fourth palette. Use consistent borders across these connected surfaces.
- Selected tabs use a white surface, dark text, and a teal bottom indicator in
  both the editor and catalog. Available unselected tabs remain readable;
  disabled tabs use the common disabled treatment and lock indicator.
- Primary actions and focus use teal. Do not use teal indiscriminately for
  ordinary text or give disabled actions an enabled accent appearance.
- Editable tables use a white base, subtle cool-gray alternating rows, a tinted
  header, horizontal separators, and a rounded outer frame. Selection, errors,
  and drag feedback take precedence over zebra striping.
- Keep existing typography: the `airy-v0` UI font stack is DM Sans / Avenir Next /
  Avenir / Helvetica Neue / sans-serif. Do not add a separate font per screen.
- Use shared radii (`--radius-control`: 7px; `--radius-panel`: 9px) rather than
  unrelated corner treatments. Appearance changes must not resize hit targets
  or introduce interaction-dependent layout shifts.

## Semantic colors are exceptions, not competing palettes

Errors remain red (`--red`, `--danger-*`), success remains green (`--success-*`),
and warnings remain amber (`--warning-*`). B and S badges retain distinct green
and cyan families; B+S retains its combined-mode treatment. Preserve the distinct
matrix search, click-selection, and hover states. Do not flatten these meanings
into the neutral palette or rely solely on color to communicate them.

## Change discipline

- No greenish or lavender neutral backgrounds mixed into the cool slate scale.
- Change a shared role at the theme level, not through isolated screen overrides.
- Keep dark-theme tokens separate: this palette does not authorize recoloring
  the dark theme or changing classic-theme layout.
- Generated mockups guide appearance only, not connector capabilities, labels,
  or application behavior. The production catalog remains authoritative.
- Update this document when an approved palette changes. Add regression coverage
  for shared tokens and state styling; follow the repository's verification policy.
