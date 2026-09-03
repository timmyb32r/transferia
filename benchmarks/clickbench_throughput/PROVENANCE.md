# ClickBench `hits` fixture provenance

This document records verified local facts and the validation contract for the
throughput fixture. It contains no credentials, endpoint addresses, account
names, or private infrastructure identifiers.

## Input identity

| Property | Value | Status |
|---|---:|---|
| File name | `hits.csv` | Verified |
| File size | 81,136,059,858 bytes | Verified |
| Header row | None | Verified |
| Ordered column count | 105 | Verified in the schema and sampled complete rows |
| Exact row count | TBD | Must be counted by the fail-closed importer |
| Murmur3 x64 128 | `bba51f01e068246aae3d3a716992238e` | Verified over all 81,136,059,858 bytes |
| Schema fingerprint | `38ce8f59e196b2eca53457cfff1d1b11` | Murmur3 x64 128 over the canonical schema rows defined below |

The analyzer currently carries `99,997,497` as a configured reference row
count. That value has not been established by a complete fail-closed scan and
must not be reported as the verified fixture row count.

Murmur3 is intentionally non-cryptographic. This benchmark has no adversarial
collision threat model; the digest is reproducibility metadata and is never the
sole proof of row equality or destination integrity.

## Exact complete-record prefix

The bounded local tournament used the first 2,000,000 **complete** RFC4180
records, extracted without decoding or rewriting their bytes. Embedded newlines
inside quoted fields do not count as record boundaries.

| Property | Value |
|---|---:|
| Rows | 2,000,000 |
| Columns per row | 105 |
| Bytes | 1,675,324,145 |
| Murmur3 x64 128 | `6f44be423e550234ceaccd888532032a` |
| Exact full-PK distinct rows | 2,000,000 |

The schema fingerprint input is the UTF-8 sequence
`ordinal<TAB>name<TAB>Arrow type<TAB>primary-key member<LF>` for the 105 rows
below, in order. Nullability is omitted from each row only because this contract
declares every field non-nullable; the fingerprint is valid only with that
document-level invariant. Official primary-key order is recorded separately and
must be compared exactly, not inferred from the membership booleans.

## Logical Arrow schema

Every field is non-nullable. `TimestampSecond` below means
`Timestamp(Second, None)`: the CSV contains no timezone, and attaching one
would change its semantics. `Binary` is intentional and preserves arbitrary
bytes without Unicode replacement or normalization.

| # | Column | Arrow type | Primary-key member |
|---:|---|---|---|
| 1 | `WatchID` | `Int64` | Yes |
| 2 | `JavaEnable` | `Int16` | No |
| 3 | `Title` | `Binary` | No |
| 4 | `GoodEvent` | `Int16` | No |
| 5 | `EventTime` | `TimestampSecond` | Yes |
| 6 | `EventDate` | `Date32` | Yes |
| 7 | `CounterID` | `Int32` | Yes |
| 8 | `ClientIP` | `Int32` | No |
| 9 | `RegionID` | `Int32` | No |
| 10 | `UserID` | `Int64` | Yes |
| 11 | `CounterClass` | `Int16` | No |
| 12 | `OS` | `Int16` | No |
| 13 | `UserAgent` | `Int16` | No |
| 14 | `URL` | `Binary` | No |
| 15 | `Referer` | `Binary` | No |
| 16 | `IsRefresh` | `Int16` | No |
| 17 | `RefererCategoryID` | `Int16` | No |
| 18 | `RefererRegionID` | `Int32` | No |
| 19 | `URLCategoryID` | `Int16` | No |
| 20 | `URLRegionID` | `Int32` | No |
| 21 | `ResolutionWidth` | `Int16` | No |
| 22 | `ResolutionHeight` | `Int16` | No |
| 23 | `ResolutionDepth` | `Int16` | No |
| 24 | `FlashMajor` | `Int16` | No |
| 25 | `FlashMinor` | `Int16` | No |
| 26 | `FlashMinor2` | `Binary` | No |
| 27 | `NetMajor` | `Int16` | No |
| 28 | `NetMinor` | `Int16` | No |
| 29 | `UserAgentMajor` | `Int16` | No |
| 30 | `UserAgentMinor` | `Binary` | No |
| 31 | `CookieEnable` | `Int16` | No |
| 32 | `JavascriptEnable` | `Int16` | No |
| 33 | `IsMobile` | `Int16` | No |
| 34 | `MobilePhone` | `Int16` | No |
| 35 | `MobilePhoneModel` | `Binary` | No |
| 36 | `Params` | `Binary` | No |
| 37 | `IPNetworkID` | `Int32` | No |
| 38 | `TraficSourceID` | `Int16` | No |
| 39 | `SearchEngineID` | `Int16` | No |
| 40 | `SearchPhrase` | `Binary` | No |
| 41 | `AdvEngineID` | `Int16` | No |
| 42 | `IsArtifical` | `Int16` | No |
| 43 | `WindowClientWidth` | `Int16` | No |
| 44 | `WindowClientHeight` | `Int16` | No |
| 45 | `ClientTimeZone` | `Int16` | No |
| 46 | `ClientEventTime` | `TimestampSecond` | No |
| 47 | `SilverlightVersion1` | `Int16` | No |
| 48 | `SilverlightVersion2` | `Int16` | No |
| 49 | `SilverlightVersion3` | `Int32` | No |
| 50 | `SilverlightVersion4` | `Int16` | No |
| 51 | `PageCharset` | `Binary` | No |
| 52 | `CodeVersion` | `Int32` | No |
| 53 | `IsLink` | `Int16` | No |
| 54 | `IsDownload` | `Int16` | No |
| 55 | `IsNotBounce` | `Int16` | No |
| 56 | `FUniqID` | `Int64` | No |
| 57 | `OriginalURL` | `Binary` | No |
| 58 | `HID` | `Int32` | No |
| 59 | `IsOldCounter` | `Int16` | No |
| 60 | `IsEvent` | `Int16` | No |
| 61 | `IsParameter` | `Int16` | No |
| 62 | `DontCountHits` | `Int16` | No |
| 63 | `WithHash` | `Int16` | No |
| 64 | `HitColor` | `Binary` | No |
| 65 | `LocalEventTime` | `TimestampSecond` | No |
| 66 | `Age` | `Int16` | No |
| 67 | `Sex` | `Int16` | No |
| 68 | `Income` | `Int16` | No |
| 69 | `Interests` | `Int16` | No |
| 70 | `Robotness` | `Int16` | No |
| 71 | `RemoteIP` | `Int32` | No |
| 72 | `WindowName` | `Int32` | No |
| 73 | `OpenerName` | `Int32` | No |
| 74 | `HistoryLength` | `Int16` | No |
| 75 | `BrowserLanguage` | `Binary` | No |
| 76 | `BrowserCountry` | `Binary` | No |
| 77 | `SocialNetwork` | `Binary` | No |
| 78 | `SocialAction` | `Binary` | No |
| 79 | `HTTPError` | `Int16` | No |
| 80 | `SendTiming` | `Int32` | No |
| 81 | `DNSTiming` | `Int32` | No |
| 82 | `ConnectTiming` | `Int32` | No |
| 83 | `ResponseStartTiming` | `Int32` | No |
| 84 | `ResponseEndTiming` | `Int32` | No |
| 85 | `FetchTiming` | `Int32` | No |
| 86 | `SocialSourceNetworkID` | `Int16` | No |
| 87 | `SocialSourcePage` | `Binary` | No |
| 88 | `ParamPrice` | `Int64` | No |
| 89 | `ParamOrderID` | `Binary` | No |
| 90 | `ParamCurrency` | `Binary` | No |
| 91 | `ParamCurrencyID` | `Int16` | No |
| 92 | `OpenstatServiceName` | `Binary` | No |
| 93 | `OpenstatCampaignID` | `Binary` | No |
| 94 | `OpenstatAdID` | `Binary` | No |
| 95 | `OpenstatSourceID` | `Binary` | No |
| 96 | `UTMSource` | `Binary` | No |
| 97 | `UTMMedium` | `Binary` | No |
| 98 | `UTMCampaign` | `Binary` | No |
| 99 | `UTMContent` | `Binary` | No |
| 100 | `UTMTerm` | `Binary` | No |
| 101 | `FromTag` | `Binary` | No |
| 102 | `HasGCLID` | `Int16` | No |
| 103 | `RefererHash` | `Int64` | No |
| 104 | `URLHash` | `Int64` | No |
| 105 | `CLID` | `Int32` | No |

The names `TraficSourceID` and `IsArtifical` reproduce the upstream schema
exactly. They must not be silently corrected during import.

## Primary-key contract

The official key ordinal from the ClickBench table definition is:

1. `CounterID`
2. `EventDate`
3. `UserID`
4. `EventTime`
5. `WatchID`

The current logical schema preserves the complete member set but not this
ordinal; see the limitation in [REPORT.md](REPORT.md). Fixture creation must
record both the intended and actual physical key order.

## Temporal contract

The CSV syntax is strict:

- Timestamp fields: `%Y-%m-%d %H:%M:%S`
- Date field: `%Y-%m-%d`
- Fractional seconds: absent
- Timezone: absent

A destination may widen seconds to microseconds only with checked arithmetic.
For every widened value `physical`, validation must prove
`physical % 1_000_000 == 0` and `physical / 1_000_000 == logical`. Attaching a
timezone, parsing through a host-local timezone, rounding, truncating, or
formatting the values as strings invalidates the fixture.

## Import manifest to capture

The fail-closed importer must append the following evidence before any result
is published:

| Evidence | Value |
|---|---|
| Complete CSV scan succeeded | TBD |
| Exact row count | TBD |
| Duplicate complete primary keys | TBD |
| Full-file null count for every field | TBD |
| Full-file per-column min/max or byte-length bounds | TBD |
| Full-file exact temporal round-trip probes | TBD |
| Full-file exact binary round-trip probes | TBD |
| Canonical full-file fixture digests | Input Murmur3 recorded; typed artifact TBD |
| Exact-prefix native fixture row counts | Verified for all six connectors in `exact-prefix-summary.json`; OpenSearch uses the qualified 500,000-row subset |
| Native fixture physical schemas and key orders | Recorded per connector in `exact-prefix-summary.json`; YTsaurus/Iceberg full read-back is explicitly qualified where PK distinctness was not recomputed |
| Measured executable identity | Two exact-prefix binary Murmur3 values and their measurement scopes are recorded; source revision unavailable because the trees were dirty |
| Accepted-window timing and process resources | Initial 44 local windows are in `exact-prefix-runs.json`; later temporal-v2, Iceberg, and YTsaurus per-run arrays are in `exact-prefix-summary.json` |

The canonical fixture should be a deterministic, typed Parquet dataset with a
fixed file order, shard count, row-group size, and compression setting. Every
native fixture must derive from that artifact, not from an independent native
CSV parser. This prevents differences in quoting, control-byte handling, or
temporal parsing from changing the workload between connectors.

## Synthetic-profile tournament evidence

The 2026-09-03 tournament did **not** satisfy the exact import manifest above.
It used the bundled deterministic ClickBench generator profile, then persisted
that generated stream into connector-native source fixtures. This is useful for
comparing implementation settings while holding the logical schema and sampled
distribution constant, but it does not prove preservation of every original
CSV row.

| Native source | Persisted rows measured | Representation qualification |
|---|---:|---|
| YTsaurus | 1,409,820 | Typed 105-column schema; timestamp seconds physically widened with checked microsecond arithmetic |
| ClickHouse | 2,000,000 | Typed 105-column schema; Date32 and TimestampSecond native paths covered by lossless regressions |
| Apache Iceberg | 7,754,010 | Typed 105-column schema; timestamp seconds physically widened with checked microsecond arithmetic |
| PostgreSQL | 1,386,323 | Historical run predating typed temporal support; current exact-prefix evidence uses Date32 and timezone-explicit microsecond timestamps |
| MySQL | 540,431 | Historical run predating typed temporal support; current exact-prefix evidence uses Date32 and timezone-explicit microsecond timestamps |
| OpenSearch | 234,970 | Source returns an opaque JSON envelope, not the typed 105-column Arrow schema |

Synthetic candidates are recorded in
[`results/2026-09-03/summary.json`](results/2026-09-03/summary.json). That file
does not prove persisted row counts for every synthetic destination run and is
therefore retained only as tuning history. Exact-prefix persisted-count and
value probes are recorded separately in
[`results/2026-09-03/exact-prefix-summary.json`](results/2026-09-03/exact-prefix-summary.json).
No row-count or uniqueness claim for the complete original CSV is inferred from
either bounded fixture.
