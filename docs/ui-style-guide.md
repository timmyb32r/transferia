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
Every focusable element receives the shared focus fallback, including textareas,
links, native selects and tabindex regions. Native controls use the theme accent;
never leave their focus color to the browser. Component-specific treatments may
override the fallback, but must preserve visible keyboard focus without resizing
the control. Error and warning semantics remain distinct.

## Component rules

- Sidebar `Data viewer` sits next to `Schema widget`. Route by source preview
  capabilities: queues and S3 show output from the configured production parser
  (or native Parquet reader), including DLQ; Scan remains a separate detection tool.
  Both parsed and table sources use a fixed-size sample dialog. Parsed samples
  read one complete message/object and display up to the requested rows per table;
  an oversized object fails explicitly instead of parsing a truncated prefix.
  Table viewing uses only source-selected tables and loads only the requested
  schema/sample, without executing transforms or preparing/writing a destination.
  Load automatically on opening and choosing a different table. Row-limit edits
  wait for the manual `Load sample` action. Keep the existing 16 MiB / 30-second
  request limits without displaying their fields. Pending feedback is immediate
  and duplicate requests are suppressed. Table selection stays usable while
  loading; changing it cancels the previous read. Closing or changing source aborts reads;
  results/errors stay inside reserved regions and never move sidebar controls.
  Unsupported sampling and missing discovery have explicit disabled explanations.

- Parser columns (JSON/TSKV) and transforms use shared pointer-driven row
  reordering from the dot handle, not native browser drag images. The full row
  follows even a one-pixel movement. Neighbours slide aside as the pointer crosses
  their original midpoints, leaving a row-sized gap at the prospective position.
  There is no insertion line. The list footprint stays fixed; reduced-motion
  preferences disable neighbour animation. Only release commits;
  Escape, pointer cancellation, lost capture and window blur discard the gesture.
  Scroll near container/viewport edges without moving other controls unexpectedly.
  Arrow keys on the handle reorder without a pointer. Readonly handles stay disabled.
  `npm run test:row-reorder` checks real pointer motion and geometry in all three
  editors; it accepts the same Playwright/browser overrides as transform-scroll.

- Transform SQL uses a native autofill-resistant textarea over an aria-hidden
  SQL highlight layer. Preserve authored text, undo, selection and IME behavior;
  this is lexical coloring, not DataFusion validation or formatting. Both layers
  share typography, padding, tab stops and scroll positions; highlighting cannot
  resize the editor or move following controls. Focus remains teal. Reuse the
  syntax palette for keywords, function calls, literals and comments in both themes.

- About replaces the sidebar Matrix launcher and retains Matrix, Entities and
  Properties. Source types / Destination types use variant A: a searchable
  connector rail on the left and a fixed-size mapping viewport on the right.
  Type mappings are accessible only from About, not endpoint headers.
  The matching-mode footer appears only in Matrix. Type tabs have no summary
  banner or footer; their table fills the remaining height below a compact
  heading and search. General caveats and connector-specific context live in
  the heading's native-title help tooltip, with an accessible hidden description.
  Kafka, Logbroker and S3 show parser/serializer explanations instead of tables
  and type search. Data generator explains preset-defined synthetic Arrow data;
  Discard explains that it drops benchmark data without creating a schema.
  Mapping rows are offline examples evaluated by production Rust resolvers,
  never a separately authored output table. Keep concrete parameters, rejection
  reasons and configuration/extension caveats visible; examples are not an
  exhaustive support guarantee. Parser-defined sources explicitly say so.
  Filtering and connector selection keep search/action geometry stable;
  `npm run test:about-layout` covers this with the real catalog in a browser.

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
  Not loaded filters. Failed rows have a clickable name/status area; Copy and
  Use remain separate actions. A click opens the full cached error with Copy and
  Close in an overlay inside the fixed-size catalog window, with keyboard scrolling
  and focus contained in the details. There is no permanent Schema errors panel
  or empty space reserved for it, and selecting the Failed filter does not open
  an error automatically. Keep the underlying list mounted and inert; preserve
  its scroll position, filter and control geometry. Escape closes details first
  and restores focus to the row (or Search if the row no longer exists).
  Controls keep fixed geometry during polling.
  Place the inline Hide system tables checkbox beside Selected / All tables.
  Use `Add tables` and retain the overall All matched tables disclosure.
  Keep the disclosure left-aligned, beside Add tables when that action is present.
- Keep table-group padding compact (8px). In sources, empty Exclude starts as a quiet
  `+ Exclude` action beside Include, opening its field in the same row and
  focusing it. This explicit opt-in makes an additional `(optional)` label redundant.
  Saved nonempty exclusions are always visible, including readonly
  forms. Clearing a value does not collapse its field; `Hide` explicitly closes
  an empty Exclude and restores focus to the action without changing configuration.
  Reserve equal label heights and a fixed Delete column so opening Exclude does
  not move Delete or later controls in normal-width forms. Only table sections
  narrower than 380px put Exclude below Include; Delete stays on the Include row.
  In transforms, Exclude starts expanded even when empty, for both new and saved
  steps (including readonly forms). Manual Hide remains available for an empty
  editable Exclude; this visibility preference never changes the configured scope.
- Source and transform scopes share `TableRuleFields`: magnifier, exact Use,
  independent modes, optional Exclude and matched-table disclosure. Source Include is
  labelled once; subsequent rules omit visible repeated labels while retaining
  unique accessible names. Keep the compact matching rail reserved, remove the
  large per-row separator/padding, and never collapse it on a late preview result.
- Include in both source tables and transforms opens a floating suggestion list
  immediately on focus, including an empty field. Search the cached catalog with
  the same prefix / substring / subsequence ranking and character highlighting
  as Logbroker topic paths, including case-insensitive and keyboard-layout matching.
  For an authored glob/regex, search its literal prefix; `*` and `.*` show the
  catalog. This is suggestion search only: actual rule matching and validation
  retain their existing glob/regex semantics. No request is needed per keystroke.
  Clicking a result, or selecting it with arrows and Enter, inserts its exact
  escaped name in the current mode. Plain Enter finishes the authored pattern;
  Tab and Escape close the menu without choosing a table. Exclude and the
  Available tables popup retain their pattern search. Keep the list out of flow.
- A truncated table-pattern value gets an immediate full-value tooltip on hover.
  Measure the rendered text against the actual input space (excluding inline
  icons), and show no tooltip if it fits. The shared `TablePatternInput` owns
  this explicit exception to native-title defaults: a pointer-transparent fixed
  overlay, without a competing native title or any change to field geometry.
- Exact Include names and patterns share the same Matched tables disclosure and
  count in sources and transforms, including zero matches. There is no in-field
  confirmation check. An already-open list stays mounted while typing until
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
- **Rename table** is a data transform, separate from the decorative strip name.
  Its compact 480px settings column offers Exact name / Regex replacement, with
  authored name or pattern/replacement fields. Changing modes is an explicit
  form change below the stationary mode selector; never move the strip header.
  A stationary checkbox below the mode selector opts into renaming only the last
  table component; unchecked means the full qualified name, as in Include.
  Explain namespace preservation, escaped identifier dots and capture rules inline.
  Run preview validates the entire matched scope before reading samples; a pattern
  nonmatch shows an error in the reserved status region, never stale/partial results.
  Following
  transforms show projected names while source reads and cached schemas retain
  their original identities. Keep the available catalog stable while editing
  only Include/Exclude; invalidate it when a preceding transform changes.
- Transform naming uses **C — the overflow menu** after Delete: `Set name` for
  unnamed steps, `Rename` otherwise. The menu opens a small floating name editor,
  never a field in the expanded settings. Save (or Enter) commits; Cancel, Escape
  and outside click discard the draft. Shift+Enter inserts a newline. Empty Save
  explicitly removes the custom name; other text is retained exactly. The name
  is the strip title, with the action type in a quiet badge and the existing
  summary underneath. Long titles ellipsize visually and expose the full name
  through a native title, without resizing the header or its controls. Menus and
  editors stay out of flow; closing with Save, Cancel or Escape restores focus to
  the overflow button without scrolling. Readonly names remain visible but cannot
  be changed. Optional `middlewares[].name` is display-only configuration metadata,
  preserved in YAML and clones; it never renames tables or changes the action.
  The transform-scroll browser regression also checks rename geometry.
- Expanding a transform keeps its header at the same viewport coordinates,
  including at the page bottom: the body grows downwards, never underneath the
  pointer. Suspend native document scroll anchoring only for the toggle's layout
  commit, then restore it before paint; keep focus on the header without scrolling.
  Do not use delayed scroll corrections or a persistent scroll lock. The browser
  regression is `npm run test:transform-scroll` (Playwright; optional
  `TRANSFERIA_PLAYWRIGHT_MODULE` and `TRANSFERIA_BROWSER_EXECUTABLE` overrides).
- Use `variant="plain"` for tabs, selectors, navigation, drag handles and
  disclosures such as Matched tables. These are not secondary form actions and
  keep their existing neutral/selected treatments. Primary, danger and transport
  actions retain their semantic styling. Disabled controls remain gray, without
  enabled hover/press feedback; pending actions retain their label, dimensions and
  spinner. This action palette does not recolor classic or dark themes.
- Source, Destination, Tables and parser settings are independent rounded islands.
  Tables and parser settings span the full route width below both endpoints,
  separated by the shared editor gap. There are no bridges, concave joins or
  stretched endpoint cards; absent sections leave no empty grid rows.
  Iceberg table names, OpenSearch indices and YDB/YTsaurus table paths also live
  in Tables, using their original list editors and configuration paths. Keep
  connection/catalog settings, including Iceberg namespace, in Source. These
  explicit lists stay editable before connection checking: sources without a
  table-catalog capability have no Discover tables action or discovery gate.
  S3 keeps Path prefix, Table name and the Parser selector (with Scan) in Source,
  with Check connection immediately before Parser. S3 has no separate Tables
  island. Parser settings remain in their own subsequent island, with JSON
  Output columns retaining their full width. This is only a presentation split:
  source configuration paths and entered values stay exact.
  Destination table/index settings are not moved.
- Island forms use **B — an invisible, left-aligned inner column**. All content
  in Source, Destination, Tables and ordinary parser settings occupies 60% of the
  island's content width, with a 320px readability floor capped by the available
  width. Leave the right side empty; add no inner frame, background or centering.
  Headers and actions share the same column. JSON and TSKV scalar sections use
  the same compact width: table naming, framing and parsing/error policies.
  Parser scalar columns additionally cap at 480px: 60% alone remains too wide
  inside a full-route island. This cap is shared across parser types and sources,
  including Table name, its nested Name and JSON framing; apply it only once.
  JSON's Data schema / Output columns, including nested column settings, remain
  full-width and unchanged; TSKV's output schema also retains its full width. Identify these by
  parser capability metadata, not display labels. Apply the width once at the
  island boundary (or JSON/TSKV scalar-section boundary), never again to nested
  settings. Delivery identity uses the same compact column; Transforms are unchanged. The column
  is a `form-space` container and never changes width on network/status updates.
- All parser scalar fields share top-aligned labels, a 6px label/control gap,
  and controls filling their compact column. Nested settings use the remaining
  column width, not another 60% reduction. Parsing-policy pairs align at their
  top edges even when one has nested options; they stack at narrow widths.
  JSON uses the same authored editor for S3, Kafka and Logbroker. Referenced
  `x-ui` hints are retained when a branch adds capabilities; explicit sibling
  hints override individual keys. Keep source-specific fields in their actual
  owning section (S3 table naming remains in Source), without placeholder fields
  or changing defaults to make forms match. Do not restyle Output columns cells.
  `npm run test:parser-layout` checks actual-catalog browser geometry for JSON,
  TSKV, Schema Registry, Debezium and Raw to table across supported sources and
  responsive widths, including dropdown and nested-setting interactions.
- Description is a multiline field starting at one control-height row. It grows
  and shrinks synchronously with user-entered wrapped lines, without an internal
  scrollbar or manual resize handle. A hidden, accessibility-excluded sizing
  mirror shares the textarea's typography and grid cell; no delayed measurement
  or height animation is involved. Keep saved whitespace/newlines exact and
  size saved descriptions before paint. This user-requested growth is an explicit
  layout-stability exception: it may move later fields while the user edits the
  description, but unrelated network/status updates must not resize the field.
- Catalog-enabled database sources have an ordinary `Check connection` action. It authenticates
  without enumerating tables, loading schemas or invalidating an existing catalog.
  Tables owns `Discover tables`, becoming `Refresh tables` after success. Reserve
  the longer label's width. The island and locked table controls are mounted
  before discovery; the discovery button stays outside the locked fieldset.
  Unlock selection and transforms only after authenticated discovery returns a
  catalog (including an empty catalog). Check and discovery have independent
  pending/success/error regions and deduplicate their requests. Discovery retains
  a fixed-size slot; connection diagnostics follow the full-text rule below. No
  automatic scrolling follows either action. Connection edits invalidate both
  states and release the metadata session; plain re-checks do not.
- Check connection feedback shows the entire diagnostic inline, with preserved
  line breaks and wrapping even for long unbroken words. No fixed height, line
  clamp, ellipsis, clipping or internal scrollbar is allowed. Reserve at least
  one button-height row; longer messages grow downwards as an explicit full-text
  display exception. The row is top-aligned so its fixed-size button and spinner
  do not move down as the message grows. Following content may move down to make
  room for the complete diagnostic; never cover it with overflowing text. Keep
  the full accessible status/alert text and native hover title as well.
- Source and Transforms share one authenticated table catalog and a server-side
  metadata session. Fewer than 1000 catalog tables triggers asynchronous schema
  preloading; 1000 or more uses explicit `Load schemas` beside each transform's
  Preview disclosure. This action loads only that transform's matches, not rows.
  Keep its status and control slots fixed across pending, partial success and
  errors. Add transform stays disabled until the catalog is known, with a tooltip
  directing the user to `Discover tables` in Tables; known-empty is not unknown.
- Editor discovery is cache-only. Validate loads missing schemas for the selected
  source tables and reports `Schemas checked X/Y` in the existing fixed progress
  overlay, then checks transforms and destination constraints. Validate first
  discovers tables if needed and joins an already-pending discovery request. Cached
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
- Keep the page white and delivery islands slate gray, with white fields. Use
  the same palette and borders for the separate Tables and parser islands.
- Source and Destination keep their content height. Empty destinations must not
  inherit Source's height.
- Final schema uses a fixed-size, resizable inspector with a stationary toolbar
  and a separately scrolling schema table. Long column names and full type
  descriptions wrap inside their own cells, never over PK / Not null. Its type
  tabs share the editor tab style, including locally anchored disabled locks.
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
