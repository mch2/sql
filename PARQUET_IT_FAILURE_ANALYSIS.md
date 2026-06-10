# Parquet-Backed IT Failure Analysis

Scope: failures observed running the SQL repo's `Calcite*` remote ITs against a **parquet-backed**
(composite data format) cluster at `localhost:9200`, with a per-method index wipe active
(`@After wipeIndicesAfterEachMethod` added to `SQLIntegTestCase`).

Evidence sources:
- **Authoritative test output:** `/tmp/rerun_xml/*.xml` (the per-method-wipe run).
- **Live reproductions:** PPL queries + `_explain` + node DEBUG logs against `localhost:9200`.

Confidence legend:
- **VERIFIED** — root cause reproduced live this session (query + explain/log evidence).
- **CAPTURED** — failure + assertion message taken from the rerun XML; bucket inferred from the
  message/query shape but NOT individually reproduced live. Flagged as such; not a guess about the
  number, only about the bucket attribution.

---

## Root-cause buckets

| ID | Root cause | Status |
|----|------------|--------|
| **RC1** | `geo_point` field rejected by composite/parquet mapping → index creation 500s, whole class fails in setup | VERIFIED |
| **RC2** | PPL **search shorthand** (`source=idx <expr>` with no `\| where`) lowers to `query_string`, which returns 0 on numeric fields | VERIFIED |
| **RC3** | `PPL_SUBSEARCH_MAXOUT` cluster setting not forwarded on the unified/analytics path → subsearch FETCH limit never inserted → subsearch not capped | VERIFIED |
| **RC4** | `text`-field equality (`=`) delegated to Lucene as a `term` query → misses analyzed (lowercased) tokens → returns 0 | VERIFIED |
| **RC5** | Distinct planner/backend capability gaps (each a separate missing feature; 500s) | VERIFIED (messages) |
| **RC6** | Residual correctness diffs after wipe (off-by-one / null-grouping etc.) | CAPTURED |
| **RC7** | `text` `=` literal vs unsupported operators surfaced as 500 | VERIFIED (subset of RC4/RC5) |
| **TEST** | Test over-specifies row order on a sort **tie** (engine result is correct) | VERIFIED |

---

## Per-test table

### CalciteDataTypeIT — 7/7 (RC1)
All fail at index creation: `mapper_parsing_exception: searchCapability is not supported for field:
geo_point_value of type: geo_point`. The `datatypes` dataset has a `geo_point` field; neither parquet
nor lucene registers `geo_point` in `CompositeDataFormatPlugin.assignCapabilities()`, so creation 500s
and every method fails in setup. **VERIFIED** (live: creating a geo_point mapping under composite 500s).

- test_numeric_data_types
- test_long_integer_data_type
- test_alias_data_type
- test_nonnumeric_data_types
- testNumericFieldFromString
- testBooleanFieldFromString
- testBooleanFieldFromNumberAcrossWildcardIndices

### CalciteMultiValueStatsIT — 31/31 (RC1)
Same `geo_point` mapping rejection; the dataset includes `geo_point_value`. Entire class dies in setup.
**VERIFIED** (same mechanism as DataTypeIT). All 31 `testListFunction*` / `testValuesFunction*` methods.

### CalciteOperatorIT — 7/21 (RC2)
All 7 use the **search shorthand** form (`source=%s age > 36`, no `| where`). Shorthand lowers to
`query_string(MAP('query','age:>36'))` over the **numeric** `age` field; query_string returns 0 on a
numeric field on parquet. The `| where age > 36` form works (returns correct rows). **VERIFIED** (live:
shorthand→0, where→1; explain shows the query_string lowering).

| Test | shorthand query | expected | got |
|------|-----------------|----------|-----|
| testEqualOperator | `age = 32` | 1 | 0 |
| testNotEqualOperator | `age != 32` | 6 | 0 |
| testGreaterOperator | `age > 36` | 1 | 0 |
| testGteOperator | `age >= 36` | 3 | 0 |
| testLessOperator | `age < 32` | 1 | 0 |
| testLteOperator | `age <= 32` | 2 | 0 |
| testNotOperator | `not age > 32` | 2 | 7 |

### CalcitePPLInSubqueryIT — 4/18
- **testSubsearchMaxOut** — RC3. `id in [...]`, maxout=1 → expected 1, got 5. The SUBSEARCH_MAXOUT
  fetch limit is never inserted on the analytics path (`RestUnifiedQueryAction.applyClusterOverrides`
  does not forward `PPL_SUBSEARCH_MAXOUT`; `SysLimit.subsearchLimit()` stays 0/unlimited). **VERIFIED**
  (live: legacy plan has `LogicalSystemLimit(SUBSEARCH_MAXOUT,fetch=1)`, parquet plan does not).
- **testInCorrelatedSubquery** — RC4. `name in [ ... where id=uid and department='DATA' ]` → expected
  3, got 1. Isolated: same subsearch WITHOUT the `department='DATA'` text-equality returns 4; the text
  `=` predicate returns 0, dropping matches. **VERIFIED**.
- **testInSubqueryWithTableAlias** — RC4. `where i.department='DATA'` → expected 2, got 0. Same text
  `=` bug (isolated: without the text predicate the subquery returns 4). **VERIFIED**.
- **testTwoExpressionsInSubquery** — **TEST** (not an engine bug). `(id,name) in [...] | sort -salary`.
  The sort IS applied correctly; failure is the **tie at salary=120000** (John 1002 vs David 1003) —
  `verifyDataRowsInOrder` over-specifies the order of tied rows, and the analytics engine's tie order
  differs (and varies run-to-run). **VERIFIED** (live: rows correctly desc-salary; only the tie order
  differs).

### CalcitePPLExistsSubqueryIT — 4/19
- **testSubsearchMaxOut1** — RC3. `exists[id=uid]`, maxout=1 → expected 1, got 5. Maxout not forwarded
  (same as InSubquery testSubsearchMaxOut). **VERIFIED**.
  - NOTE: even once maxout is forwarded, the analytics engine deliberately STRIPS the limit for EXISTS
    (`PlannerImpl.stripExistsSubqueryLimits`, required for decorrelation) and returns the true EXISTS
    count. So this test may still need a test-side adjustment — EXISTS maxout is semantically a no-op.
- **testSubsearchMaxOut2** — RC4. `exists[id=uid and department='DATA']` → expected 2, got 0. The
  `department='DATA'` text-`=` returns 0. NOT maxout. **VERIFIED**.
- **testSubsearchMaxOut3** — RC4. `exists[id=uid | eval dept=department | where dept='DATA']` →
  expected 1, got 0. Text `=` on `dept` returns 0. **VERIFIED**.
- **testSubsearchMaxOut4** — RC4. `exists[eval dept=department | where dept='DATA' | id=uid]` →
  expected 2, got 0. Same text `=` bug. **VERIFIED**.

### CalcitePPLScalarSubqueryIT — 1/14
- **testTwoUncorrelatedScalarSubqueriesInOr** — RC4. Second scalar subquery has
  `where department='DATA' | stats min(uid)`; the text `=` returns 0 so that subquery yields nothing →
  expected 2, got 1. **VERIFIED** (live: the `department='DATA'` filter returns 0).

### CalcitePPLConditionBuiltinFunctionIT — 5/24 (RC5 — distinct capability gaps; 500s)
- **testIsNullWithStruct** — 500 `Field [aws] not found.` (struct/object field access). CAPTURED.
- **testIsNotNullWithStruct** — 500 `Field [aws] not found.` CAPTURED.
- **testIsNullWithNested** — 500 `Field [address] not found.` (nested field access). CAPTURED.
- **testIsNotNullWithNested** — 500 `Field [address] not found.` CAPTURED.
- **testEarliestWithEval** — 500 `type mismatch: BOOLEAN vs BOOLEAN NOT NULL`. CAPTURED.

### CalciteMultisearchCommandIT — 6/21 (RC5 + RC6)
- **testMultisearchBinTimestamp** — 500 `No backend supports scalar function [ADDDATE] among
  [datafusion]` (RC5, missing ADDDATE). CAPTURED.
- **testMultisearchBinAndStats** — 500 `type mismatch: EXPR_TIMESTAMP VARCHAR vs TIMESTAMP(9)` (RC5,
  timestamp typing). CAPTURED.
- **testMultisearchWithoutFurtherProcessing** — expected 51, got 52 (off-by-one, RC6). CAPTURED.
- **testMultisearchWithComplexAggregation** — bare AssertionError (row-set mismatch, RC6). CAPTURED.
- **testMultisearchWithThreeSubsearches** — bare AssertionError (RC6). CAPTURED.
- **testMultisearchWithTimestampInterleaving** — bare AssertionError (RC6). CAPTURED.

### CalcitePPLCaseFunctionIT — 4/9
- **testCaseWhenInFilter** — 500 `Unrecognized filter operator [IS NOT TRUE / IS_NOT_TRUE]` (RC5,
  missing IS_NOT_TRUE). CAPTURED.
- **testCaseAggWithNullValues** — expected 3, got 4 (off-by-one over null grouping, RC6). CAPTURED.
- **testNestedCaseAggWithAutoDateHistogram** — bare AssertionError (RC6). CAPTURED.
- **testCaseCanBePushedDownAsRangeQuery** — bare AssertionError (RC6). CAPTURED.

### CalciteObjectFieldOperateIT — 1/5 (RC6)
- **verify_schema_without_fields** — expected 4, got 5 (schema/column-count mismatch). CAPTURED;
  NOT individually reproduced.

### CalciteSettingsIT — 2/2
- **testQuerySizeLimit** — expected 3, got 0. CAPTURED; NOT individually reproduced. Likely the
  query-size-limit setting also isn't forwarded on the unified path (sibling of RC3), but UNVERIFIED.
- **testQuerySizeLimit_NoPushdown** — expected 2, got 3. CAPTURED; UNVERIFIED.

### CalciteLikeQueryIT — 2/11 (RC6 / LIKE semantics)
- **test_the_default_3rd_option** — expected 0, got 7. CAPTURED; NOT reproduced. LIKE/wildcard
  semantics divergence.
- **test_convert_field_text_to_keyword** — bare AssertionError. CAPTURED; NOT reproduced.

### CalcitePPLStringBuiltinFunctionIT — 0/27
No failures under the per-method wipe (the prior testTrim/testRTrim failures were the `_id`-append
accumulation artifact, now fixed by the wipe). Listed for completeness.

---

## Summary counts (this rerun)

| Bucket | # tests | Notes |
|--------|---------|-------|
| RC1 geo_point mapping | 38 | DataTypeIT (7) + MultiValueStatsIT (31); whole classes fail in setup |
| RC2 search shorthand → query_string | 7 | CalciteOperatorIT |
| RC3 subsearch maxout not forwarded | 2 | InSubquery.testSubsearchMaxOut, Exists.testSubsearchMaxOut1 |
| RC4 text `=` → Lucene term on analyzed field | 6 | InCorrelated, TableAlias, ExistsMaxOut2/3/4, ScalarTwoUncorrelatedInOr |
| RC5 distinct capability-gap 500s | ~7 | ADDDATE, IS_NOT_TRUE, struct/nested field access, timestamp typing |
| RC6 residual correctness / row-set diffs | ~8 | off-by-ones, multisearch row-sets, case-agg null, like |
| TEST tie-order over-specified | 1 | InSubquery.testTwoExpressionsInSubquery |

---

## Root-cause detail for the VERIFIED buckets

### RC1 — geo_point mapping rejection
`CompositeDataFormatPlugin.assignCapabilities()` requires every field's requested capabilities to be
covered by a configured data format. `geo_point` is registered by neither parquet nor lucene → unclaimed
capabilities → `MapperParsingException` at index creation. Fix candidate: allow unsupported field types
to pass with empty capabilities (inert column).

### RC2 — search shorthand → query_string
`source=idx <expr>` (no `| where`) is lowered by the PPL frontend to a `query_string` filter
(`query_string(MAP('query','age:>36'))`). On numeric fields query_string matches nothing on parquet.
The explicit `| where <expr>` form lowers to a real comparison and works. Affects every shorthand-filter
query on non-text fields.

### RC3 — subsearch maxout FETCH not forwarded
maxout is a pure FETCH limit: the plugin materializes it as `LogicalSystemLimit(fetch=N,
type=SUBSEARCH_MAXOUT)` (a `Sort` subclass) which the engine consumes as a backend `LIMIT->N`.
`RestUnifiedQueryAction.applyClusterOverrides()` forwards only `PPL_REX_MAX_MATCH_LIMIT` and
`PPL_SYNTAX_LEGACY_PREFERRED`, NOT `PPL_SUBSEARCH_MAXOUT`. So `SysLimit.subsearchLimit()` defaults to
0 (unlimited), `CalciteRexNodeVisitor` skips inserting the limit node, and no cap reaches the engine.
Verified by comparing legacy plan (has the node) vs parquet plan (does not). NOTE: for EXISTS the engine
intentionally strips the limit (decorrelation), so forwarding alone won't change EXISTS row counts.

### RC4 — text `=` delegated to Lucene `term` on analyzed field
For `where department='DATA'` (department = analyzed `text`, no keyword subfield): datafusion has no
EQUALS serializer (`Performance-delegation skipped: no serializer for [EQUALS] on delegated backend
[datafusion]; falling back to native on operator [lucene]`), so `=` is rendered as a Lucene
`{"term":{"department":{"value":"DATA"}}}`. A `term` query is NOT analyzed; the inverted index holds the
lowercased token `data`, so the exact-bytes lookup for `DATA` misses → 0 rows. Other operators on the
same column succeed because they take different paths: `IN`, `>`, `like` run on datafusion against the
raw parquet value; `match`/`query_string` are analyzed (match token `data`); keyword/integer `=` are
exact-by-nature. Legacy forwarded text `=` as a runtime SCRIPT doing exact `_source.equals()`.
