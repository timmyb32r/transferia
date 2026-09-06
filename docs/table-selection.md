# Source table selection

The shared Rust rule evaluator lives in
`transferia-registry::table_selection`. Rule-card UI, authenticated catalogs,
fixed startup selection and namespace-aware MySQL destination routing are
implemented. MySQL CREATE admission uses an ordered destination-preparation
barrier followed by an atomic membership/binlog checkpoint. Startup verifies
the table-rule membership again under the read lock. Restart reconstructs
the destination actor from committed membership without preparing old tables
again. A completely empty combined selection always rejects startup before
destination preparation. Every individual rule must also select at least one table.
See `table-selection-worklog.md` for acceptance and verification evidence.

## Scope

PostgreSQL, MySQL and ClickHouse source table selection uses rule cards rather
than a manually maintained list of unqualified table names. MySQL destinations
also support an optional database override.

## Connection and catalog

ClickHouse sources default `hide_system_tables` to true. Discovery excludes
the exact databases `system`, `_system`, `INFORMATION_SCHEMA`, and every database
whose name starts with lowercase `information_schema`. The filter applies before
access checks and rule matching, for both Check connection and startup discovery.
Disable the checkbox to include those databases. Changing it invalidates the
checked catalog; run Check connection again before editing or starting.

- A successful authenticated Check connection loads all accessible tables.
- Only then can the user add rule cards. Network reachability alone is not enough.
- PostgreSQL lists schemas and tables within the configured database. MySQL and
  ClickHouse list accessible databases and tables on the connected server.
- Changing connection parameters preserves rules but invalidates the catalog and
  blocks additions and launch until another successful check. Editing a rule
  does not invalidate the connection check.
- Preview is explicitly labeled as the result of the last connection check.
- Backend startup resolves rules against a fresh catalog and validates the
  resulting source and destination contracts before destination preparation.

## Rules

The segmented control defaults to Selected tables (`type: selected`, `rules`).
Each compact row has required Include, optional Exclude and a delete button;
the plus button adds a row. Each field has its own `.*` toggle: `include_mode`
and `exclude_mode`, both defaulting to glob. An exact name is a glob with no
unescaped wildcard characters. A missing exact table retains its entered name,
shows an invalid field and prevents startup.

All tables (`type: all`) has no additional settings.
It explicitly selects the entire accessible catalog. Use Selected tables to
apply Include/Exclude rules. The editor
remembers inactive drafts while mounted; only the active variant is serialized
and validated. Backend schemas reject fields belonging to an inactive variant.

The bottom Matched tables disclosure shows the combined final selection across
all rules, or the entire accessible catalog in All tables mode. Clicking expands
the full list inside a bounded scroll viewport. This explicit expansion may move
later rows; asynchronous changes keep that viewport and the result/status regions
stable. Per-pattern previews remain available beside the corresponding rule.

```yaml
tables:
  type: selected
  rules:
    - include: public.reports_*
      include_mode: glob
      exclude: 'public\.reports_(test|temp)'
      exclude_mode: regex
```

- PostgreSQL names are schema.table; MySQL and ClickHouse names are database.table.
- Glob: `*` matches any number of characters, `?` one character, `_` is literal.
- Regex matches the full qualified name, not a substring.
- Backslash escapes special characters. Selecting a suggestion produces an
  exact-match expression with the appropriate escaping; it never renames the
  actual table or changes its identifier.
- Keep namespace and table identifiers separate in catalog data.
- The qualified text representation escapes literal dots and backslashes inside
  each identifier so a namespace containing a dot cannot alias a table containing
  a dot. Expressions match this representation; exact suggestions escape it for
  the selected pattern mode. The source/destination identifiers remain unchanged.
- Question-mark help explains syntax, escaping, examples and exclusion scope.

For a catalog C and card i, let I_i be its Include matches and X_i its Exclude
matches. Its selected set is S_i = I_i minus X_i; its excluded set is
E_i = I_i intersect X_i. Exclude is strictly per-card.

For distinct cards i and j, reject S_i intersect S_j and S_i intersect E_j.
Show the conflicting tables and both cards. Within-card subtraction is normal,
not a conflict. Never silently choose a winning card.

Every S_i must contain at least one table after its own Exclude is applied.
An empty card always fails validation before destination preparation, even if
another card matches tables. This policy is not configurable in UI or YAML.
An Exclude expression matching nothing is not itself an error. Invalid
expressions always fail. Match previews show a count and a bounded list with access
to the complete result without unbounded growth of the form.

## Runtime membership

PostgreSQL resolves a fixed set at startup. Explain this in the table-rule help:
"Table patterns are resolved at delivery startup. Tables created later are not
added automatically." Polling plus real watermark-based DBLog is future work.

MySQL stream and batch+stream have a New tables dropdown:

- Include automatically (default).
- Ignore new tables (fixed startup membership).

Automatic inclusion processes creation DDL from binlog. Validate the new table's
rule ownership, schema and destination constraints, and prepare its destination
before releasing its first data records. Do not acknowledge records before
durable destination commit. Startup and restart must preserve this guarantee.

Periodic catalog polling alone is not lossless admission from table creation.
PostgreSQL pgoutput uses a fixed FOR TABLE publication. MySQL processes proven
empty, permanent CREATE TABLE operations, including CREATE TABLE LIKE.
Ambiguous/no-op/populating forms, executable comments and other unsupported
DDL fail closed rather than being treated as an empty-table creation. A
selected CREATE is replayed after a restart until destination preparation and
the source checkpoint have both completed.

### Tables entering selection through rename

A pre-existing, populated table can enter a rule through RENAME TABLE, including
a move between databases. It is not equivalent to creating an empty table: its
historical rows need not appear as new row events. If a previously unselected
table enters selection through RENAME TABLE or ALTER TABLE ... RENAME, stop the
delivery with an explicit error before writing its records or acknowledging the
unsupported change. Report the old and new qualified names and matching rule.
Never silently admit it as an empty table or skip the rename and continue.

Explain this limitation in the New tables help: automatic admission currently
covers newly created tables, not existing tables renamed into the selection.

Coordinated loading of existing rows using the watermark-based DBLog protocol
is future work for MySQL, as is the planned PostgreSQL polling plus DBLog path.
Neither future path is implemented by this specification; a plain snapshot or
catalog poll must not be labeled DBLog.

## MySQL destination database

- An explicit Database directs all selected tables into that database.
- Without an override, preserve source database/schema. Missing namespace is an
  error; never substitute one silently.
- Known name collisions are rejected during configuration/discovery and again
  before execution, before destination writes.
- A collision introduced by a future table fails before writing that table.

## Acceptance coverage

Cover rule syntax and escaping, per-card exclusion, pairwise conflicts, empty
results, stale connection results, asynchronous cancellation and repeated clicks,
and bounded previews without unstable hit targets. Backend and UI previews must
agree. Cover multi-database discovery and destination-name collisions at startup
and at runtime. MySQL integration coverage must create a table and immediately
write records, then verify all records survive admission and restart/replay.
Also cover a populated, previously unselected table renamed into the selection
(including a cross-database rename): it must fail before destination writes and
without acknowledging past the unsupported change.

The shared evaluator has authored unit tests covering glob/regex,
Unicode, exact suggestions, identifier boundaries, conflicts, empty-match policy,
malformed patterns, repeated catalog identities and refusal to consume an invalid
preview. These are not a substitute for connector and UI integration coverage.

Follow AGENTS.md verification policy: author regression coverage, run the normal
compile-only gate during implementation, and do not claim runtime verification
without explicitly authorized execution of the relevant tests.
