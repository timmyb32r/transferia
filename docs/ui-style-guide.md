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

### Segmented choices

Use the shared `SegmentedControl` for two or three short, mutually exclusive
choices. It is a radio group, not a slider or an on/off switch. All choices stay
visible; the active segment uses the shared soft teal surface and accent border.
Keep dimensions identical across idle, selected, pressed, focus and disabled
states. Support arrow keys, Home/End, Space and one tab stop. Use a dropdown for
long labels or larger choice sets. The first approved use is Selected tables /
All tables; do not replace unrelated dropdowns automatically.

Table matching lists are an explicit layout-stability exception: user-triggered
expansion may push later rows down. Async preview updates must not change the
height of an already open list or the result/status control regions.
Each open matching list uses **A — a text action above the list**: `Show all`
fits the current results without internal scrolling; `Restore height` returns
to the compact viewport. Center the visible CSS arrow icon and label as one
pair, reserving the wider pair's width so the button stays in place. Use a
compact outlined secondary button with a transparent surface and neutral hover;
never reserve scrollbar gutters inside the action. Use one native delayed title
tooltip. Do not expose a drag-to-resize
corner or a double-click-only action. Fit height is measured on activation and
stays fixed across preview updates; close/reopen returns to compact height.

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

- Database-source connection checks use **A — required form item**: a neutral
  `Required` badge, a stable status beside the button, and a bordered group for
  dependent table fields. Before verification show `Not checked` and a small
  lock, not a validation error. Unlock only after authenticated verification
  returns a table catalog (including an empty catalog). Keep status slots and
  controls mounted with identical dimensions while checking, succeeding,
  failing, or invalidating credentials. Connection/advanced options stay outside
  the locked group so users can repair a failed connection. Ordinary diagnostic
  checks in destinations and non-table sources do not gain a required badge.
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

- A scalar control width cap must never constrain a nested form. Object, array,
  optional-object and editable union settings use the full available row width;
  determine this from the schema, not the selected branch, to keep the selector
  stable. Installation selectors retain their full-row nested section.
- Use the `form-space` container query for label placement: at 520px or less,
  put labels above controls instead of consuming their width. Multi-column parser
  settings wrap intrinsically; do not use viewport size as a proxy for field width.
- The catalog readiness regression traverses selectable endpoint branches,
  including parsers and serializers, and rejects scalar width caps on nested
  forms. The CSS layout contract covers responsive stacking. These structural
  checks do not replace browser geometry checks for custom table/cell editors.

- No greenish or lavender neutral backgrounds mixed into the cool slate scale.
- Change a shared role at the theme level, not through isolated screen overrides.
- Keep dark-theme tokens separate: this palette does not authorize recoloring
  the dark theme or changing classic-theme layout.
- Generated mockups guide appearance only, not connector capabilities, labels,
  or application behavior. The production catalog remains authoritative.
- Update this document when an approved palette changes. Add regression coverage
  for shared tokens and state styling; follow the repository's verification policy.
