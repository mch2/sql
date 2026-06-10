# SQL IT tests — bucketed by query shape (one representative each)

The ~250 tests grouped into distinct query shapes. One representative test + its query per bucket.

---

### 1. Scalar comparison filter (`= != > < >= <=`, `not`, numeric)
`CalciteOperatorIT.testGreaterOperator`
`source=opensearch-sql_test_index_bank age > 36 | fields age`

### 2. search-command space syntax (field=value + boolean/parens)
`CalciteSearchCommandIT.testSearchWithComplexBooleanExpression`
`search source=opensearch-sql_test_index_otel_logs (severityText="ERROR" OR severityText="WARN") AND severityNumber>10 | sort time | fields severityText, severityNumber | head 5`

### 3. search-command time-range (earliest/latest, relative/absolute/numeric)
`CalciteSearchCommandIT.testSearchWithChainedRelativeTimeRange`
`search source=opensearch-sql_test_index_time_data earliest='2025-08-01 03:47:41' latest=+1months@year | fields @timestamp`

### 4. search-command wildcard pattern match
`CalciteSearchCommandIT.testWildcardPatternMatching`
`search source=opensearch-sql_test_index_otel_logs severityText=ERR* | sort time | fields severityText, body | head 5`

### 5. `where` with `==` / string equality
`CalciteWhereCommandIT.testDoubleEqualWithStringComparison`
`source=opensearch-sql_test_index_account | where firstname == 'Amber' | fields firstname, lastname`

### 6. `IN`-list filter
`CalciteWhereCommandIT.testWhereWithIn`
`source=opensearch-sql_test_index_account | where firstname in ('Amber', 'Dale') | fields firstname`

### 7. text/keyword field filter
`CalcitePPLBasicIT.testFilterOnTextField`
`source=opensearch-sql_test_index_bank | where gender = 'F' | fields firstname, lastname`

### 8. fields projection (include / minus)
`CalcitePPLBasicIT.testQueryMinusFieldsWithFilter`
`source=opensearch-sql_test_index_bank | where (account_number = 20 or city = 'Brogan') and balance > 10000 | fields - firstname, lastname`

### 9. multisearch (union of subsearches → stats → sort)
`CalciteMultisearchCommandIT.testMultisearchWithThreeSubsearches`
`| multisearch [search source=..._account | where state = "IL" | eval region = "Illinois"] [search source=..._account | where state = "TN" | eval region = "Tennessee"] [search source=..._account | where state = "CA" | eval region = "California"] | stats count by region | sort region`

### 10. union command (+ maxout)
`CalciteUnionCommandIT.testUnionThreeSubsearches`
`| union [search source=..._account | where state = "IL" | eval region = "Illinois"] [search source=..._account | where state = "TN" | eval region = "Tennessee"] [search source=..._account | where state = "CA" | eval region = "California"] | stats count by region | sort region`

### 11. relational join with `ON` condition (inner/left/right/semi/anti/cross)
`CalcitePPLJoinIT.testComplexLeftJoin`
`source = ..._state_country | where country = 'Canada' OR country = 'England' | left join left=a, right=b ON a.name = b.name ..._occupation | sort a.age | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary`

### 12. join with field-list / self-join / max / overwrite
`CalcitePPLJoinIT.testJoinWithFieldList`
`source=..._state_country | join type=inner name,year,month ..._occupation`

### 13. join alias / subquery-alias reference access
`CalcitePPLJoinIT.testCheckAccessTheReferenceByAliases`
`source = ..._state_country | JOIN left = t1 ON t1.name = t2.name ..._occupation as t2 | fields t1.name, t2.name`

### 14. IN subquery
`CalcitePPLInSubqueryIT.testWhereInSubquery`
`source = ..._worker | where id in [ source = ..._work_information | fields uid ] | sort - salary | fields id, name, salary`

### 15. EXISTS subquery (+ correlated / maxout)
`CalcitePPLExistsSubqueryIT.testSimpleExistsSubquery`
`source = ..._worker | where exists [ source = ..._work_information | where id = uid ] | sort - salary | fields id, name, salary`

### 16. scalar subquery (correlated / in eval / in filter)
`CalcitePPLScalarSubqueryIT.testCorrelatedScalarSubqueryInWhere`
`source = ..._worker | where id = [ source = ..._work_information | where id = uid | stats max(uid) ] | fields id, name`

### 17. streamstats (window / by / variance / null buckets)
`CalciteStreamstatsCommandIT.testStreamstatsByWithNull`
`source=..._state_country_with_null | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by country | fields name, country, state, month, year, age, cnt, avg, min, max`

### 18. reverse command
`CalciteReverseCommandIT.testStreamstatsWithSortThenReverse`
`source=..._state_country | streamstats count() as cnt | sort age | reverse | head 3`

### 19. stats aggregation (percentile / span / by / limit / null)
`CalciteStatsCommandIT.testStatsPercentileBySpan`
`source=opensearch-sql_test_index_bank | stats percentile(balance, 50) as p50 by span(age, 10) as age_bucket`

### 20. sort / head ordering (+ auto-cast, cast expr, head-then-sort)
`CalcitePPLSortIT.testSortAgeAndFieldsNameAge`
`source=opensearch-sql_test_index_bank | sort - age | fields firstname, age`

### 21. datetime cross-type comparison (DATE/TIME/TIMESTAMP)
`CalciteDateTimeComparisonIT.testCompare`
`source=..._datatypes_nonnumeric | eval `r` = TIME('10:20:30') <= TIMESTAMP('2026-06-03 10:20:30') | fields `r``

### 22. datetime functions (week / make* / date_format / strftime / unix_timestamp)
`CalciteDateTimeFunctionIT.testDateFormat`
`source=..._date | eval f = date_format(timestamp('1998-01-31 13:14:15.012345'), '%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M %m %p %r %S %s %T %% %P') | fields f`

### 23. cast functions (DATE/TIME/TIMESTAMP/BOOLEAN)
`CalcitePPLCastFunctionIT.testCastTimestamp`
`source=..._date_formats | eval a = cast('1984-04-12 09:07:42' as TIMESTAMP) | fields a`

### 24. case / conditional eval (+ pushdown as range)
`CalcitePPLCaseFunctionIT.testCaseWhenWithCast`
`source=..._weblogs | eval status = case(cast(response as int) >= 200 AND cast(response as int) < 300, 'Success', ... else concat('Incorrect HTTP status code for', url)) | where status != 'Success' | fields host, method, message, bytes, response, url, status`

### 25. condition builtins (if / isnull / ifnull / nullif / isempty / ispresent)
`CalcitePPLConditionBuiltinFunctionIT.testIfNull`
`source=..._state_country_with_null | eval new_name = ifnull(name, 'Unknown') | fields new_name, age`

### 26. arithmetic builtins (divide / mod, type widening)
`CalcitePPLBuiltinFunctionIT.testModShouldReturnWiderTypes`
`source=..._datatypes_numeric | eval b = byte_number % 2, i = mod(integer_number, 3), l = mod(long_number, 2), f = float_number % 2, d = mod(double_number, 2), s = short_number % byte_number | fields b, i, l, f, d, s`

### 27. string builtins (trim / ltrim / reverse)
`CalcitePPLStringBuiltinFunctionIT.testTrim`
`source=..._state_country | where Trim(name) = 'Jim' | fields name, age`

### 28. json builtins (json_set / json_delete)
`CalcitePPLJsonBuiltinFunctionIT.testJsonSetWithDollarPrefixedPath`
`source=..._people2 | eval a = json_set('{"name":"alice","scores":[90,85,92]}', '$.name', 'modified_alice') | fields a | head 1`

### 29. dedup (count / multi-field / expr)
`CalcitePPLDedupIT.testDedupExpr`
`source=..._duplication_nullable | eval new_name = lower(name) | dedup 1 new_name`

### 30. append (merged columns)
`CalcitePPLAppendCommandIT.testAppendWithMergedColumn`
`source=..._account | stats sum(age) as sum by gender | append [ source=..._account | stats sum(age) as sum by state | sort sum ] | head 5`

### 31. chart (over/by + time span)
`CalciteChartCommandIT.testChartMaxValueByTimestampSpanDayAndWeek`
`source=..._time_data | chart max(value) by timestamp span=1day, @timestamp span=2weeks`

### 32. full-text relevance + wildcard (like / multi_match / query_string / simple_query_string)
`CalciteQueryStringIT.wildcard_test`
`source=..._beer | where query_string(['T*'], 'taste')`

### 33. comments (block / line / multi-line)
`CommentIT.testLineComment`
`source=..._account | fields firstname // line comment | where firstname='Amber' // line comment | fields firstname // line comment`

### 34. regex extraction (parse / rex named groups)
`CalciteRexCommandIT.testRexWithWhere`
`source=..._account | where state="CA" | rex field=email "(?<user>[^@]+)@(?<domain>.+)" | fields email, user, domain`

### 35. error reporting (field/index not found, stage messages)
`CalciteErrorReportStageIT.testFieldNotFoundErrorIncludesStage`
`source=..._account | fields nonexistent_field`

### 36. explain command
`CalcitePPLExplainIT.testExplainCommand`
`explain source=test | where age = 20 | fields name, age`

### 37. patterns (BRAIN log clustering)
`CalcitePPLPatternsIT.testBrainParseWithUUID_ShowNumberedToken`
`source=..._weblogs | eval body = '[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d user_currency=USD' | head 1 | patterns body method=BRAIN mode=label show_numbered_token=true | fields patterns_field, tokens`

### 38. rename (incl. wildcard rename)
`CalcitePPLRenameIT.testRenameFullWildcardExcludesMetadataFields`
`source = ..._state_country | rename * as old_*`

### 39. typeof / system function introspection
`CalciteSystemFunctionIT.typeof_opensearch_types`
`source=..._datatypes_numeric | eval `double` = typeof(double_number), `long` = typeof(long_number), ... | fields ...`

### 40. datatype scan / numeric-from-string
`CalciteDataTypeIT.testNumericFieldFromString`
`source=..._datatypes_numeric | where long_number=12345678 | fields long_number, integer_number, double_number, float_number`

### 41. fetch size / pagination
`CalciteFetchSizeIT.testFetchSizeWithSort`
`source=..._account | sort age | fields firstname, age`

### 42. query size limit / settings (pushdown on/off)
`CalciteSettingsIT.testQuerySizeLimit_NoPushdown`
`search source=..._bank | eval a = 1 | where age>35 | fields firstname`

### 43. complex dashboard pipeline (where + stats + span + eval/concat + sort + head)
`NfwPplDashboardIT.testTopLongLivedTCPFlows`
`source=nfw_logs | WHERE `event.proto` = 'TCP' and `event.netflow.age` > 350 | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.src_port`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP:Port - Dst IP:Port` = CONCAT(...) | SORT - Count | HEAD 10`
