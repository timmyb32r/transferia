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
Short lists open at their content height, up to the 140px compact viewport.
Pin that measured height before paint; later async results must not resize it.
Hide `Show all` when the results fit, retaining its header slot without a focus
or click target. A taller matching list uses **A — a text action above the list**: `Show all`
fits the current results without internal scrolling; `Restore height` returns
to the compact viewport. Center the visible CSS arrow icon and label as one
pair, reserving the wider pair's width so the button stays in place. Use a
compact outlined secondary button using the shared action colors below;
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

- Secondary actions in every form use a **white surface, teal text/icons and a
  visible teal outline** in airy-light. Hover adds the existing soft teal tint;
  press and focus keep their immediate shared feedback without changing geometry.
  `Button` defaults to `variant="secondary"`; standard, icon, add-row and compact
  actions (including Add transform, Preview, Available tables and Show all)
  share `--secondary-action-*` tokens. Small labels use a slightly darkened
  `--blue-hover` for readable contrast on both white and hover surfaces.
  Do not add feature-local action palettes.
- **All clipboard actions** use `CopyButton`, a quiet neutral icon utility:
  secondary-text icons and a soft neutral hover/press surface in both themes.
  Transfer ID and matched-table rows have no visible border; catalog popup rows
  use a thin neutral outline.
  Clone keeps its label and clone semantics, with the same `CopyIcon` glyph and
  plain `copy-action copy-action-framed` styling. Rounded overlapping pages have
  an occluded rear outline, not intersecting borders.
  Copy alone uses a shared fixed-overlay tooltip (350 ms hover/focus delay),
  not a native title: `Copy` changes to `Copied` after the clipboard write succeeds,
  with a check inside the front page. Click/pending/error feedback is immediate;
  pending writes are deduplicated. The tooltip has fixed dimensions and never
  enters document flow, captures clicks or creates another tab stop. Do not
  layer native titles over it. Focus rings and control geometry stay unchanged.
- Source table selectors use **A — a calm Tables section**. Its header combines
  `Available tables (N)` and `Schemas loaded X/N · Y failed` in one fixed-size,
  two-line action opening the shared catalog popup. Both counters refer to the
  catalog after Hide system tables. Schema failures are amber; the popup exposes
  Loaded / Not loaded / Failed per table. The amber failure count is a separate
  one-click action opening the Failed filter; the catalog has All / Failed /
  Not loaded filters. Clicking Failed on a row selects its full cached error in
  a fixed-height, keyboard-scrollable details area with Copy. Keep this area
  reserved throughout browsing rather than expanding rows. Controls keep fixed geometry during polling.
  Place the inline Hide system tables checkbox beside Selected / All tables.
  Use `Add tables` and retain the overall All matched tables disclosure.
- Keep table-group padding compact (8px). Empty Exclude starts as a quiet
  `+ Exclude` action beside Include, opening its field in the same row and
  focusing it. This explicit opt-in makes an additional `(optional)` label redundant.
  Saved nonempty exclusions are always visible, including readonly
  forms. Clearing a value does not collapse its field; `Hide` explicitly closes
  an empty Exclude and restores focus to the action without changing configuration.
  Reserve equal label heights and a fixed Delete column so opening Exclude does
  not move Delete or later controls in normal-width forms. Only table sections
  narrower than 380px put Exclude below Include; Delete stays on the Include row.
- Source and transform scopes share `TableRuleFields`: magnifier, exact Use,
  independent modes, optional Exclude and exact-match check. Source Include is
  labelled once; subsequent rules omit visible repeated labels while retaining
  unique accessible names. Keep the compact matching rail reserved, remove the
  large per-row separator/padding, and never collapse it on a late preview result.
- A truncated table-pattern value gets an immediate full-value tooltip on hover.
  Measure the rendered text against the actual input space (excluding inline
  icons), and show no tooltip if it fits. The shared `TablePatternInput` owns
  this explicit exception to native-title defaults: a pointer-transparent fixed
  overlay, without a competing native title or any change to field geometry.
- Exact Include names get a green check inside a permanently reserved input slot,
  not a duplicate Table found line or a matched-table disclosure. Pattern rules
  retain their disclosure; an already-open list stays mounted while typing until
  explicitly closed. Keep a compact result rail reserved so pattern edits and
  asynchronous checks cannot move later controls. Each matched name has frameless Copy.
- The neutral magnifier immediately before Include's `.*` opens the same popup
  with Copy and Use. Use inserts an exact-name pattern in the existing Include
  mode, preserves Exclude and other rules, closes the popup and restores focus to
  the magnifier. It does not change clipboard contents. Header browsing is read-only.
  Transforms retain their compact Available tables action and the shared popup.
  Unknown catalogs disable browsing; known empty catalogs open normally.
  Invalidating metadata closes either popup without reopening on reconnect.
- Transform available table rows place a compact `Use` action beside Copy. Use replaces
  the current transform's Include with an exact-name pattern in its existing
  glob/regex mode, preserves Exclude and the transform, and closes the dialog.
  The popup search mode does not change Include's mode. Read-only browsing keeps
  search and Copy available but disables Use. Keep both row actions fixed-size.
- A new transform starts with `Transformation: Not selected`, never an implicit
  SQL or filter action. Its table scope stays editable; action-specific fields
  and Preview require an explicit selection. Clone retains the original action.
- Use `variant="plain"` for tabs, selectors, navigation, drag handles and
  disclosures such as Matched tables. These are not secondary form actions and
  keep their existing neutral/selected treatments. Primary, danger and transport
  actions retain their semantic styling. Disabled controls remain gray, without
  enabled hover/press feedback; pending actions retain their label, dimensions and
  spinner. This action palette does not recolor classic or dark themes.
- Database-source metadata uses **A — required form item**: a neutral
  `Required` badge, a stable Connected/pending/error status beside the button, and a bordered group for
  dependent table fields. Tables occupy a full-width island below both endpoints,
  in DOM and keyboard order after Destination, with the existing form contexts
  preserved. The island is mounted and locked before connection, not inserted
  by an asynchronous response. The primary action is `Connect & load metadata`, then
  `Refresh metadata` after success; reserve the longer label's width. Before
  verification show `Required to unlock tables and transforms` and a small
  lock, not a validation error. Unlock only after authenticated verification
  returns a table catalog (including an empty catalog). Do not duplicate readiness
  in a Table settings are ready banner. Keep status slots and
  controls mounted with identical dimensions while checking, succeeding,
  failing, or invalidating credentials. Connection/advanced options stay outside
  the locked group so users can repair a failed connection. Ordinary diagnostic
  checks in destinations and non-table sources do not gain a required badge.
- Only an explicitly clicked successful metadata connection scrolls to Tables,
  once, without stealing keyboard focus. Polling, Validate's automatic connection
  and remounts never trigger this. Cancel the pending reveal on a new gesture,
  focus change, failed/stale response or unmount; honor reduced-motion preferences.
- Source and Transforms share one authenticated table catalog and a server-side
  metadata session. Fewer than 1000 catalog tables triggers asynchronous schema
  preloading; 1000 or more uses explicit `Load schemas` beside each transform's
  Preview disclosure. This action loads only that transform's matches, not rows.
  Keep its status and control slots fixed across pending, partial success and
  errors. Add transform stays disabled until the catalog is known, with a tooltip
  directing the user to the source metadata action; known-empty is not unknown.
- Editor discovery is cache-only. Validate loads missing schemas for the selected
  source tables and reports `Schemas checked X/Y` in the existing fixed progress
  overlay, then checks transforms and destination constraints. Validate first
  connects if needed and joins an already-pending connection request. Cached
  successes and errors are reused until explicit Refresh, connection/decoding
  options change, the editor closes, or the server restarts. Run preview checks
  the selected table's current schema against the cache before reading rows;
  drift is an explicit error, never an automatic cache replacement. Actual
  delivery startup always discovers fresh schemas independently of this cache.
- Metadata catalog reads use batches of up to 100 uncached tables, shared by
  preload, Load schemas and Validate. Progress advances as each batch completes;
  overlapping requests reuse its results. MySQL uses one joined catalog query;
  PostgreSQL uses one catalog query and one combined projection preflight.
  ClickHouse batches columns and keys but retains per-table projection checks.
  Decoder errors are table-local; a failed batch SQL request is reported for all
  tables in that request, never hidden by dropping a table or switching readers.
- Keep the page white and the three delivery islands slate gray, with white
  fields. Parser details continue the source island rather than introducing a
  fourth palette. Use consistent borders across these connected surfaces.
- Selected tabs use a white surface, dark text, and a teal bottom indicator in
  both the editor and catalog. Available unselected tabs remain readable;
  disabled tabs use the common disabled treatment and lock indicator.
  Configuration tabs and the transform's Before step / After step tabs share
  a continuous slate backing, with the same rounded corners and inner inset.
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
