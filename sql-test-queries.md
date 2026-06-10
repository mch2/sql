# SQL-repo IT test → query reference

Each test below with the PPL query it sends. Index constants resolved from
`integ-test/.../legacy/TestsConstants.java` (base prefix `opensearch-sql_test_index`).
Where a test runs several queries, all are listed; `(setting …)` notes a cluster setting the test sets first.

---

## CalciteOperatorIT
*(methods inherited from `org.opensearch.sql.ppl.OperatorIT`, index = `..._bank`)*

### testEqualOperator
```
source=opensearch-sql_test_index_bank age = 32 | fields age
source=opensearch-sql_test_index_bank | where age = 32 | fields age
source=opensearch-sql_test_index_bank | where 32 = age | fields age
```
### testGreaterOperator
`source=opensearch-sql_test_index_bank age > 36 | fields age`
### testLessOperator
`source=opensearch-sql_test_index_bank age < 32 | fields age`
### testLteOperator
`source=opensearch-sql_test_index_bank age <= 32 | fields age`
### testGteOperator
`source=opensearch-sql_test_index_bank age >= 36 | fields age`
### testNotEqualOperator
```
source=opensearch-sql_test_index_bank age != 32 | fields age
source=opensearch-sql_test_index_bank | where age != 32 | fields age
source=opensearch-sql_test_index_bank | where 32 != age | fields age
```
### testNotOperator
`source=opensearch-sql_test_index_bank not age > 32 | fields age`

---

## CalciteSearchCommandIT

### testSearchWithAbsoluteEarliestAndNow
```
search source=opensearch-sql_test_index_time_data earliest='2025-08-01 03:47:41' latest=now | fields @timestamp
search source=opensearch-sql_test_index_time_data earliest='2025-08-01 03:47:42' latest=now() | fields @timestamp
search source=opensearch-sql_test_index_time_data earliest='2025-08-01 02:00:55' | fields @timestamp
```
### testSearchWithAttributeFields
``search source=opensearch-sql_test_index_otel_logs `attributes.http.status_code`=200 | fields body``
### testSearchWithChainedRelativeTimeRange
`search source=opensearch-sql_test_index_time_data earliest='2025-08-01 03:47:41' latest=+1months@year | fields @timestamp`
### testSearchWithDateFormats
```
search source=opensearch-sql_test_index_otel_logs @timestamp="2024-01-15T10:30:00.123456789Z" | sort @timestamp | fields @timestamp, body | head 1
search source=opensearch-sql_test_index_otel_logs @timestamp="2024-01-15" | sort @timestamp | fields @timestamp | head 3
search source=opensearch-sql_test_index_otel_logs @timestamp="2024-01-15T10:30:01" | sort @timestamp | fields @timestamp | head 2
```
### testSearchWithDoubleFieldComparisons
```
search source=..._otel_logs `attributes.payment.amount`=1500.0  | fields `attributes.payment.amount`
search source=..._otel_logs `attributes.payment.amount`=1500.0d | fields `attributes.payment.amount`
search source=..._otel_logs `attributes.payment.amount`=1500.0f | fields `attributes.payment.amount`
search source=..._otel_logs `attributes.payment.amount`=1500   | fields `attributes.payment.amount`
```
### testSearchWithDoubleINOperator
``search source=opensearch-sql_test_index_otel_logs `attributes.payment.amount` IN (1000.0, 1500.0, 2000.0) | fields `attributes.payment.amount`, body``
### testSearchWithDoubleRangeOperators
```
search source=..._otel_logs `attributes.payment.amount`>1000.0  | fields `attributes.payment.amount`
search source=..._otel_logs `attributes.payment.amount`>=1500.0 | fields `attributes.payment.amount`
search source=..._otel_logs `attributes.payment.amount`>=1000.0 AND `attributes.payment.amount`<=2000.0 | fields `attributes.payment.amount`
search source=..._otel_logs `attributes.payment.amount`!=1500.0 | fields `attributes.payment.amount`
```
### testSearchWithIPAddress
``search source=opensearch-sql_test_index_otel_logs `attributes.client.ip`="192.168.1.1" | fields body``
### testSearchWithNumericTimeRange
`search source=opensearch-sql_test_index_time_data earliest=1754020060.123 latest=1754020061 | fields @timestamp`
### testSearchWithDateINOperator
`search source=opensearch-sql_test_index_otel_logs @timestamp IN ("2024-01-15T10:30:00.123456789Z", "2024-01-15T10:30:01.234567890Z") | sort @timestamp | fields @timestamp, severityText`
### testSearchWithMultipleFieldTypes
`search source=opensearch-sql_test_index_otel_logs severityText="ERROR" AND severityNumber=17 | fields severityText, severityNumber | head 2`
### testSearchWithDateRangeComparisons
```
search source=..._otel_logs @timestamp>"2024-01-15T10:30:00Z"  | sort @timestamp | fields @timestamp | head 3
search source=..._otel_logs @timestamp<="2024-01-15T10:30:01Z" | sort @timestamp | fields @timestamp | head 2
search source=..._otel_logs @timestamp>="2024-01-15T10:30:00Z" AND @timestamp<"2024-01-15T10:30:05Z" | sort @timestamp | fields @timestamp | head 5
search source=..._otel_logs @timestamp!="2024-01-15T10:30:00.123456789Z" | sort @timestamp | fields @timestamp | head 3
```
### testWildcardPatternMatching
```
search source=..._otel_logs severityText=ERR*    | sort time | fields severityText, body | head 5
search source=..._otel_logs severityText="INFO?" | sort time | fields severityText, body | head 3
search source=..._otel_logs body="user*"         | sort time | fields body | head 3
search source=..._otel_logs fail*                | sort time | fields body | head 3
search source=..._otel_logs `resource.attributes.service.name`=*-service | sort time | fields `resource.attributes.service.name`, body | head 4
search source=..._otel_logs severityText=ERR* AND severityNumber>16 | sort time | fields severityText, severityNumber | head 3
```
### testSearchMixedWithPipeCommands
`search source=opensearch-sql_test_index_otel_logs severityNumber>10 | where severityText != "INFO" | sort time | fields severityText, body | head 5`
### testSearchWithNestedParentheses
```
search source=..._otel_logs ((severityNumber<15 AND severityNumber>5) OR (severityNumber>20)) | sort time | fields severityNumber | head 5
search source=..._otel_logs severityNumber<15 AND severityNumber>5 OR severityNumber>20     | sort time | fields severityNumber | head 5
search source=..._otel_logs severityNumber<15 AND (severityNumber>5 OR severityNumber>20)   | sort time | fields severityNumber | head 5
```
### testSearchWithInclusiveRanges
`search source=opensearch-sql_test_index_otel_logs severityNumber>=9 AND severityNumber<=10 | sort time | fields severityNumber, body | head 6`
### testSearchWithNumericComparison
`search source=opensearch-sql_test_index_otel_logs severityNumber>15 AND severityNumber<=20 | sort time | fields severityNumber, severityText`
### testSearchWithComplexBooleanExpression
`search source=opensearch-sql_test_index_otel_logs (severityText="ERROR" OR severityText="WARN") AND severityNumber>10 | sort time | fields severityText, severityNumber | head 5`
### testDifferenceBetweenNOTAndNotEquals
```
search source=..._otel_logs `attributes.http.status_code`!=200    | sort time | fields body | head 5
search source=..._otel_logs NOT `attributes.http.status_code`=200 | sort time | fields body | head 5
search source=..._otel_logs `attributes.user.email`!="user@example.com"    | sort time | fields body | head 2
search source=..._otel_logs NOT `attributes.user.email`="user@example.com" | sort time | fields body | head 5
```

---

## CalciteWhereCommandIT

### testDoubleEqualChainedWhereCommands
`source=opensearch-sql_test_index_account | where age == 32 | where gender == 'M' | where state == 'TN' | fields firstname, age, gender, state`
### testDoubleEqualWithSpecialCharacters
`source=opensearch-sql_test_index_account | where email == 'amberduke@pyrami.com' | fields firstname, email`
### testDoubleEqualWithStringComparison
`source=opensearch-sql_test_index_account | where firstname == 'Amber' | fields firstname, lastname`
### testFilterScriptPushDown
`source=opensearch-sql_test_index_account | where firstname ='Amber' and age - 2.0 = 30 | fields firstname, age`
### testMixedEqualOperators
```
source=opensearch-sql_test_index_account | where age = 32 AND state == 'TN' | fields firstname, age, state
source=opensearch-sql_test_index_account | where age == 32 AND state = 'TN' | fields firstname, age, state
```
### testMultipleWhereCommands
`source=opensearch-sql_test_index_account | where firstname='Amber' | fields lastname, age | where lastname='Duke' | fields age | where age=32 | fields age`
### testWhereWithIn
```
source=opensearch-sql_test_index_account | where firstname in ('Amber') | fields firstname
source=opensearch-sql_test_index_account | where firstname in ('Amber', 'Dale') | fields firstname
source=opensearch-sql_test_index_account | where balance in (4180, 5686.0) | fields balance
```
### testWhereWithLogicalExpr
`source=opensearch-sql_test_index_account | fields firstname | where firstname='Amber' | fields firstname`
### testWhereWithMultiLogicalExpr
`source=opensearch-sql_test_index_account | where firstname='Amber' and lastname='Duke' and age=32 | fields firstname, lastname, age`
### testWhereEquivalentSortCommand
```
source=opensearch-sql_test_index_account | where firstname='Amber'
source=opensearch-sql_test_index_account firstname='Amber'
```
### testDoubleEqualCaseSensitiveStringComparison
```
source=opensearch-sql_test_index_account | where firstname == 'amber' | fields firstname
source=opensearch-sql_test_index_account | where firstname == 'Amber' | fields firstname
```

---

## CalcitePPLBasicIT

### testFilterOnTextFieldWithKeywordSubField
`source=opensearch-sql_test_index_bank | where state = 'VA' | fields firstname, lastname`
### testFilterQueryWithOr2
`source=opensearch-sql_test_index_bank (account_number = 20 or city = 'Brogan') and balance > 10000 | fields firstname, lastname`
### testFilterOnTextField
`source=opensearch-sql_test_index_bank | where gender = 'F' | fields firstname, lastname`
### testQueryMinusFields
`source=opensearch-sql_test_index_bank | fields - firstname, lastname, birthdate`
### testQueryMinusFieldsWithFilter
`source=opensearch-sql_test_index_bank | where (account_number = 20 or city = 'Brogan') and balance > 10000 | fields - firstname, lastname`

---

## CalciteMultisearchCommandIT

### testMultisearchWithComplexAggregation
`| multisearch [search source=..._account | where gender = "M" | eval segment = "male"] [search source=..._account | where gender = "F" | eval segment = "female"] | stats count as customer_count, avg(balance) as avg_balance by segment | sort segment`
### testMultisearchWithThreeSubsearches
`| multisearch [search source=..._account | where state = "IL" | eval region = "Illinois"] [search source=..._account | where state = "TN" | eval region = "Tennessee"] [search source=..._account | where state = "CA" | eval region = "California"] | stats count by region | sort region`
### testMultisearchWithFieldsProjection
`| multisearch [search source=..._account | where gender = "M" | fields firstname, lastname, balance] [search source=..._account | where gender = "F" | fields firstname, lastname, balance] | head 5`
### testMultisearchWithTimestampInterleaving
`| multisearch [search source=..._time_data | where category IN ("A", "B")] [search source=..._time_data2 | where category IN ("E", "F")] | head 10`
### testMultisearchWithoutFurtherProcessing
`| multisearch [search source=..._time_data | where category = "A"] [search source=..._time_data | where category = "B"]`

---

## CalciteUnionCommandIT

### testUnionThreeSubsearches
`| union [search source=..._account | where state = "IL" | eval region = "Illinois"] [search source=..._account | where state = "TN" | eval region = "Tennessee"] [search source=..._account | where state = "CA" | eval region = "California"] | stats count by region | sort region`
### testUnionWithMaxout
`| union maxout=5 [search source=..._account | where gender = "M"] [search source=..._account | where gender = "F"]`
### testUnionMidPipeline_SingleExplicitDataset
`search source=..._account | where gender = "M" | union [search source=..._account | where gender = "F"] | stats count() as total`

---

## CalciteSettingsIT

### testQuerySizeLimit
`search source=opensearch-sql_test_index_bank age>35 | fields firstname`  *(run twice, before/after setting query size limit)*
### testQuerySizeLimit_NoPushdown
`search source=opensearch-sql_test_index_bank | eval a = 1 | where age>35 | fields firstname`  *(CALCITE_PUSHDOWN_ENABLED=false; run 3×)*

---

## CalcitePPLJoinIT
*(indices: `..._state_country`, `..._occupation`, `..._hobbies`)*

### testJoinSubsearchMaxOut
`source=..._state_country | where country = 'Canada' | join type=inner max=0 country ..._occupation`  *(run with join subsearch maxout=5, then reset)*
### testJoinTwoColumnsAndDisjointFilters
`source = ..._state_country | inner join left=a, right=b ON a.name = b.name AND a.country = b.country AND a.year = 2023 AND a.month = 4 AND b.salary > 100000 ..._occupation | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary`
### testJoinWithFieldListMaxEqualsOne
```
source=..._state_country | join type=inner max=1 name,year,month ..._occupation
source=..._state_country | join type=inner max=1 name,year,month ..._occupation | fields name, country
```
### testCheckAccessTheReferenceByOverrideSubqueryAliases
```
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation as tt ] | fields tt.name
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation as tt ] as t2 | fields tt.name
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation ] as tt | fields tt.name
```
### testMultipleJoinsWithoutTableAliases
`source = ..._state_country | JOIN ON ..._state_country.name = ..._occupation.name ..._occupation | JOIN ON ..._occupation.name = ..._hobbies.name ..._hobbies | fields ..._state_country.name, ..._occupation.name, ..._hobbies.name`
### testJoinWithFieldListSelfJoin
`source=..._state_country | join name,year,month ..._state_country`
### testCheckAccessTheReferenceByAliases
```
source = ..._state_country | JOIN left = t1 ON t1.name = t2.name ..._occupation as t2 | fields t1.name, t2.name
source = ..._state_country as t1 | JOIN ON t1.name = t2.name ..._occupation as t2 | fields t1.name, t2.name
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name ..._occupation as tt | fields tt.name
source = ..._state_country as tt | JOIN left = t1 right = t2 ON t1.name = t2.name ..._occupation | fields t1.name
source = ..._state_country as tt | JOIN left = t1 ON t1.name = t2.name ..._occupation as t2 | fields t1.name
```
### testComplexSortPushDownForSMJ
`source=..._state_country | eval name2=substring(name, 2, 1) | join left=a right=b on a.name2 = b.state2 [ source=..._state_country | eval state2=substring(state, 2, 1) ] | fields name, name2, b.name, b.state, state2`
### testComplexLeftJoin
`source = ..._state_country | where country = 'Canada' OR country = 'England' | left join left=a, right=b ON a.name = b.name ..._occupation | sort a.age | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary`
### testJoinComparing
```
source=..._state_country | where country = 'Canada' | join type=inner max=0 country ..._occupation
source=..._state_country | where country = 'Canada' | join type=inner max=1 country ..._occupation
source=..._state_country | where country = 'Canada' | join type=inner max=2 country ..._occupation
source=..._state_country | where country = 'Canada' | join max=0 left=l right=r on l.country = r.country ..._occupation
source=..._state_country | where country = 'Canada' | join max=1 left=l right=r on l.country = r.country ..._occupation
source=..._state_country | where country = 'Canada' | join max=2 left=l right=r on l.country = r.country ..._occupation
```
### testJoinWithFieldList2
```
source=..._state_country | join type=inner overwrite=false name,year,month ..._occupation
source=..._state_country | join type=inner overwrite=false name,year,month ..._occupation | fields name, country
```
### testCheckAccessTheReferenceByOverrideSubqueryAliases2
```
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation as tt ] | fields t2.name
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation as tt ] as t2 | fields t2.name
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation ] as tt | fields t2.name
```
### testJoinWithoutFieldList
`source=..._state_country | join type=inner overwrite=false ..._state_country`
### testInnerJoinWithRelationSubquery
`source = ..._state_country | where country = 'USA' OR country = 'England' | inner join left=a, right=b ON a.name = b.name [ source = ..._occupation | where salary > 0 | fields name, country, salary | sort salary | head 3 ] | stats avg(salary) by span(age, 10) as age_span, b.country`
### testComplexCrossJoin
`source = ..._state_country | where country = 'Canada' OR country = 'England' | join left=a, right=b on 1=1 ..._occupation | sort a.age | stats count()`
### testJoinWithFieldListSelfJoin2
`source=..._state_country | join type=inner overwrite=true name,year,month ..._state_country | join type=left overwrite=false name,year,month ..._state_country`
### testCheckAccessTheReferenceByOverrideAliases
```
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name ..._occupation as tt | fields tt.name
source = ..._state_country as tt | JOIN left = t1 right = t2 ON t1.name = t2.name ..._occupation | fields t1.name
source = ..._state_country as tt | JOIN left = t1 ON t1.name = t2.name ..._occupation as t2 | fields t1.name
```
### testJoinWithFieldList
```
source=..._state_country | join type=inner name,year,month ..._occupation
source=..._state_country | join type=inner name,year,month ..._occupation | fields name, country
```
### testComplexSortPushDownForSMJWithMaxOptionAndFieldList
`source=..._state_country | eval name2=substring(name, 2, 1) | join max=1 name2,age [ source=..._state_country | eval name2=substring(state, 2, 1) ]`
### testJoinWithoutFieldListMaxEqualsOne
`source=..._state_country | join type=inner overwrite=false max=1 ..._state_country`
### testMultipleJoinsWithSubquerySelfJoin
`source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name ..._occupation | JOIN right = t3 ON t1.name = t3.name ..._hobbies | JOIN ON t1.name = t4.name [ source = ..._state_country ] as t4 | fields t1.name, t2.name, t3.name, t4.name`
### testMultipleJoinsWithSelfJoin
`source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name ..._occupation | JOIN right = t3 ON t1.name = t3.name ..._hobbies | JOIN right = t4 ON t1.name = t4.name ..._state_country | fields t1.name, t2.name, t3.name, t4.name`
### testComplexSemiJoin
`source = ..._state_country | where country = 'Canada' OR country = 'England' | left semi join left=a, right=b ON a.name = b.name ..._occupation | sort a.age`
### testJoinWithTwoJoinConditions
`source = ..._state_country | inner join left=a, right=b ON a.name = b.name AND a.country = b.country AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4 ..._occupation | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary`
### testJoinWithCondition
`source=..._state_country | inner join left=a, right=b ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4 ..._occupation | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary`
### testJoinWhenLegacyNotPreferred
*(PPL_SYNTAX_LEGACY_PREFERRED=false)*
```
source=..._state_country | join type=inner name,year,month ..._occupation
source=..._state_country | join type=inner max=1 name,year,month ..._occupation | fields name, country
```
### testCheckAccessTheReferenceBySubqueryAliases
```
source = ..._state_country | JOIN left = t1 ON t1.name = t2.name [ source = ..._occupation ] as t2 | fields t1.name, t2.name
source = ..._state_country | JOIN left = t1 ON t1.name = t2.name [ source = ..._occupation as t2 ] | fields t1.name, t2.name
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation as tt ] | fields tt.name
source = ..._state_country | JOIN left = t1 ON t1.name = t2.name [ source = ..._occupation as tt ] as t2 | fields tt.name
source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = ..._occupation ] as tt | fields tt.name
```
### testComplexAntiJoin
`source = ..._state_country | where country = 'Canada' OR country = 'England' | left anti join left=a, right=b ON a.name = b.name ..._occupation | sort a.age`
### testComplexRightJoin
`source = ..._state_country | where country = 'Canada' OR country = 'England' | right join left=a, right=b ON a.name = b.name ..._occupation | sort a.age | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary`
### testMultipleJoinsWithPartTableAliases
`source = ..._state_country | JOIN left = t1 right = t2 ON t1.name = t2.name ..._occupation | JOIN right = t3 ON t1.name = t3.name ..._hobbies | fields t1.name, t2.name, t3.name`

---

## CalcitePPLInSubqueryIT
*(indices: `..._worker`, `..._work_information`, `..._occupation`)*

### testInSubqueryWithTableAlias
`source = ..._worker as o | where id in [ source = ..._work_information as i | where i.department = 'DATA' | fields uid ] | sort - o.salary | fields o.id, o.name, o.salary`
### testTwoExpressionsNotInSubquery
`source = ..._worker | where (id, name) not in [ source = ..._work_information | fields uid, name ] | sort - salary | fields id, name, salary`
### testSubsearchMaxOutZeroMeansUnlimited
`source = ..._worker | where id in [ source = ..._work_information | fields uid ] | sort - salary | fields id, name, salary`  *(subsearch maxout=0)*
### testWhereInSubquery
`source = ..._worker | where id in [ source = ..._work_information | fields uid ] | sort - salary | fields id, name, salary`
### testInSubqueryWithParentheses
`source = ..._worker | where (id) in [ source = ..._work_information | fields uid ] | sort - salary | fields id, name, salary`
### testEmptyInSubquery
`source = ..._worker | where id not in [ source = ..._work_information | where uid = 0000 | fields uid ] | sort - salary | fields id, name, salary`
### testSubsearchMaxOut
`source = ..._worker | where id in [ source = ..._work_information | fields uid ] | sort - salary | fields id, name, salary`  *(subsearch maxout=1)*
### testSelfInSubquery
`source = ..._worker | where id in [ source=..._worker | where country = 'USA' | fields id ] | fields name, country, occupation, id, salary`
### testNestedInSubquery2
`source = ..._worker | where id in [ source = ..._work_information | where occupation in [ source = ..._occupation | where occupation != 'Engineer' | fields occupation ] | fields uid ] | sort - salary | fields name, country, occupation, id, salary`
### testTwoExpressionsInSubquery
`source = ..._worker | where (id, name) in [ source = ..._work_information | fields uid, name ] | sort - salary | fields id, name, salary`
### testInCorrelatedSubquery
`source = ..._worker | where name in [ source = ..._work_information | where id = uid and (like(occupation, '%ist') or occupation = 'Engineer') | fields name ] | sort - salary | fields id, name, salary`
### testNestedInSubquery
`source = ..._worker | where id in [ source = ..._work_information | where occupation in [ source = ..._occupation | where occupation != 'Engineer' | fields occupation ] | fields uid ] | sort - salary | fields id, name, salary`
### testFilterInSubquery
`source = ..._worker | where id in [ source = ..._work_information | fields uid ] | sort - salary | fields id, name, salary`

---

## CalcitePPLExistsSubqueryIT
*(indices: `..._worker`, `..._work_information`, `..._occupation`)*

### testSimpleExistsSubquery / testSimpleExistsSubqueryInFilter
`source = ..._worker | where exists [ source = ..._work_information | where id = uid ] | sort - salary | fields id, name, salary`
### testSubsearchMaxOut1
same as above *(subsearch maxout=1)*
### testSubsearchMaxOutNegativeMeansUnlimited
same as above *(subsearch maxout=-1)*
### testSubsearchMaxOut2
`source = ..._worker | where exists [ source = ..._work_information | where id = uid and department = 'DATA' ] | sort - salary | fields id, name, salary`  *(maxout=2)*
### testSubsearchMaxOut3
`source = ..._worker | where exists [ source = ..._work_information | where id = uid | eval dept = department | where dept = 'DATA' | sort - dept ] | sort - salary | fields id, name, salary`  *(maxout=2)*
### testSubsearchMaxOut4
`source = ..._worker | where exists [ source = ..._work_information | eval dept = department | where dept = 'DATA' | where id = uid ] | sort - salary | fields id, name, salary`  *(maxout=2)*
### testNestedExistsSubquery
`source = ..._worker | where exists [ source = ..._work_information | where exists [ source = ..._occupation | where ..._occupation.occupation = ..._work_information.occupation ] | where id = uid ] | sort - salary | fields id, name, salary`
### testIssue3566
`source = ..._worker | fields id, country | where exists [ source = ..._work_information | where id = uid ] | stats count() by country`
### testUncorrelatedSubsearchMaxOutZeroMeansUnlimited
`source = ..._worker | where exists [ source = ..._work_information | where name = 'Tom' ] | sort - salary | fields id, name, salary`  *(maxout=0)*
### testCorrelatedSubsearchMaxOutZeroMeansUnlimited
```
source = ..._worker | where exists [ source = ..._work_information | where id = uid ] | sort - salary | fields id, name, salary
source = ..._worker | where not exists [ source = ..._work_information | where id = uid ] | sort - salary | fields id, name, salary
```
*(maxout=0)*
### testExistsSubqueryAndAggregation
`source = ..._worker | where exists [ source = ..._work_information | where id = uid ] | stats count() by country`
### testSubsearchMaxOutUncorrelated
`source = ..._worker | where exists [ source = ..._work_information | join type=left uid ..._work_information | eval dept = department | where dept = 'DATA' ] | sort - salary | fields id, name, salary`  *(maxout=1)*
### testUncorrelatedExistsSubquery
```
source = ..._worker | where exists [ source = ..._work_information | where name = 'Tom' ] | sort - salary | fields id, name, salary
source = ..._worker | where not exists [ source = ..._work_information | where name = 'Tom' ] | sort - salary | fields id, name, salary
```
### testUncorrelatedExistsSubqueryCheckTheReturnContentOfInnerTableIsEmptyOrNot
```
source = ..._worker | where exists [ source = ..._work_information ] | eval constant = "Bala" | fields constant
source = ..._worker | where exists [ source = ..._work_information | where uid = 999 ] | eval constant = 'Bala' | fields constant
```

---

## CalcitePPLScalarSubqueryIT
*(indices: `..._worker`, `..._work_information`)*

### testUncorrelatedScalarSubqueryInSelectAndInFilter / testUncorrelatedScalarSubqueryInSelectAndWhere
`source = ..._worker | where id > [ source = ..._work_information | stats count(department) ] + 999 | eval count_dept = [ source = ..._work_information | stats count(department) ] | fields name, count_dept`
### testCorrelatedScalarSubqueryInWhere / testCorrelatedScalarSubqueryInFilter
`source = ..._worker | where id = [ source = ..._work_information | where id = uid | stats max(uid) ] | fields id, name`
### testTwoCorrelatedScalarSubqueriesInOr
`source = ..._worker | where id = [ source = ..._work_information | where id = uid | stats max(uid) ] OR id = [ source = ..._work_information | sort uid | where department = 'DATA' | stats min(uid) ] | fields id, name`
### testTwoUncorrelatedScalarSubqueriesInOr
`source = ..._worker | where id = [ source = ..._work_information | sort uid | stats max(uid) ] OR id = [ source = ..._work_information | sort uid | where department = 'DATA' | stats min(uid) ] | fields id, name`
### testUncorrelatedScalarSubqueryInExpressionInSelect
`source = ..._worker | eval count_dept = [ source = ..._work_information | stats count(department) ] + 10 | fields name, count_dept`
### testCorrelatedScalarSubqueryInSelectWithNonEqual
`source = ..._worker | eval count_dept = [ source = ..._work_information | where id > uid | stats count(department) ] | fields id, name, count_dept`
### testUncorrelatedScalarSubqueryInSelect
`source = ..._worker | eval count_dept = [ source = ..._work_information | stats count(department) ] | fields name, count_dept`
### testCorrelatedScalarSubqueryInSelect
`source = ..._worker | eval count_dept = [ source = ..._work_information | where id = uid | stats count(department) ] | fields id, name, count_dept`
### testDisjunctiveCorrelatedScalarSubquery
`source = ..._worker | where [ source = ..._work_information | where id = uid OR uid = 1010 | stats count() ] > 0 | fields id, name`
### testSubsearchMaxOutZeroMeansUnlimited
`source = ..._worker | where id = [ source = ..._work_information | where id = uid | stats max(uid) ] | fields id, name`  *(maxout=0)*

---

## CalciteStreamstatsCommandIT
*(index: `..._state_country_with_null` unless noted)*

### testStreamstatsWithNull
`source=..._state_country_with_null | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age) as max | fields name, country, state, month, year, age, cnt, avg, min, max`
### testStreamstatsByWithNull
```
source=..._state_country_with_null | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by country | fields ...
source=..._state_country_with_null | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by state   | fields ...
```
### testStreamstatsByWithNullBucket
```
source=..._state_country_with_null | streamstats bucket_nullable=false count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by country | fields ...
source=..._state_country_with_null | streamstats bucket_nullable=false count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by state   | fields ...
```
### testStreamstatsBySpanWithNull
`source=..._state_country_with_null | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by span(age, 10) as age_span | fields ...`
### testStreamstatsByMultiplePartitionsWithNull1
```
source=..._state_country_with_null | streamstats bucket_nullable=false count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by span(age, 10) as age_span, country | fields ...
source=..._state_country_with_null | streamstats bucket_nullable=true  count() as cnt, avg(age) as avg, min(age) as min, max(age) as max by span(age, 10) as age_span, country | fields ...
```
### testStreamstatsWindowWithNull
`source=..._state_country_with_null | streamstats window = 3 avg(age) as avg | fields name, country, state, month, year, age, avg`
### testStreamstatsCurrentAndWindowWithNull
`source=..._state_country_with_null | streamstats current = false window = 2 avg(age) as avg | fields name, country, state, month, year, age, avg`
### testMultipleStreamstatsWithWindow
`source=..._state_country_with_null | streamstats window=2 avg(age) as avg_age by state, country | streamstats window=2 avg(avg_age) as avg_state_age by country | fields name, country, state, month, year, age, avg_age, avg_state_age`
### testMultipleStreamstatsWithEval2
```
source=..._state_country_with_null | eval new_state=lower(state), new_country=lower(country) | streamstats bucket_nullable=false avg(age) as avg_age by new_state, new_country | fields ...
source=..._state_country_with_null | eval new_state=lower(state), new_country=lower(country) | streamstats bucket_nullable=true  avg(age) as avg_age by new_state, new_country | fields ...
```
### testStreamstatsDistinctCountWithNull
`source=..._state_country_with_null | streamstats dc(state) as dc_state | fields name, country, state, month, year, age, dc_state`
### testStreamstatsVarianceWithNull
`source=..._state_country_with_null | streamstats stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age) | fields name, country, state, month, year, age, \`stddev_pop(age)\`, \`stddev_samp(age)\`, \`var_pop(age)\`, \`var_samp(age)\``
### testStreamstatsVarianceWithNullBy
`source=..._state_country_with_null | streamstats stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age) by country | fields ... (same agg cols)`
### testStreamstatsAndSort
`source=..._state_country | sort age | streamstats window = 2 avg(age) as avg_age | fields name, country, state, month, year, age, avg_age`
### testLeftJoinWithStreamstats
`source=..._state_country as l | left join left=l right=r on l.country = r.country [ source=..._state_country_with_null | streamstats window=2 avg(age) as avg_age] | fields l.name, l.country, l.state, l.month, l.year, l.age, r.name, r.country, r.state, r.month, r.year, r.age, avg_age`

---

## CalciteReverseCommandIT
*(index: `..._state_country`)*

### testStreamstatsWindowWithReverse
`source=..._state_country | streamstats window=2 avg(age) as avg | reverse`
### testStreamstatsWithReverse
`source=..._state_country | streamstats count() as cnt, avg(age) as avg | reverse`
### testStreamstatsWithSortThenReverse
`source=..._state_country | streamstats count() as cnt | sort age | reverse | head 3`
### testStreamstatsByWithReverse
`source=..._state_country | streamstats count() as cnt, avg(age) as avg by country | reverse`

---

## CalciteStatsCommandIT

### testStatsWithLimit
`source=opensearch-sql_test_index_bank_with_null_values | stats avg(balance) as a by age | head 5`
### testStatsBySpanTimeWithNullBucket
`source=opensearch-sql_test_index_time_date_null | stats percentile(value, 50) as p50 by span(@timestamp, 12h) as half_day`
### testStatsPercentileByNullValueNonNullBucket
`source=opensearch-sql_test_index_bank_with_null_values | stats bucket_nullable=false percentile(balance, 50) as p50 by age`
### testStatsPercentileWithNull
`source=opensearch-sql_test_index_bank_with_null_values | stats percentile(balance, 50)`
### testStatsPercentileBySpan
`source=opensearch-sql_test_index_bank | stats percentile(balance, 50) as p50 by span(age, 10) as age_bucket`
### testStatsPercentileByNullValue
`source=opensearch-sql_test_index_bank_with_null_values | stats percentile(balance, 50) as p50 by age`

---

## CalcitePPLAggregationIT / CalcitePPLAggregationPaginatingIT
*(Paginating inherits both methods unchanged)*

### testSumGroupByNullValue
`source=opensearch-sql_test_index_bank_with_null_values | stats sum(balance) as a by age`
### testPercentile
`source=opensearch-sql_test_index_bank | stats percentile(balance, 50) as p50, percentile(balance, 90) as p90`

---

## CalcitePPLSortIT / CalciteSortCommandIT

### testSortWithAutoCast
`source=opensearch-sql_test_index_bank | sort AUTO(age) | fields firstname, age`
### testSortWithNullValue
`source=opensearch-sql_test_index_bank_with_null_values | sort balance | fields firstname, balance`
### testSortAgeAndFieldsNameAge
`source=opensearch-sql_test_index_bank | sort - age | fields firstname, age`
### testPushdownSortCastToDoubleExpression
`source=opensearch-sql_test_index_bank | eval age2 = cast(age as double) | sort age2 | fields age, age2 | head 2`
### testHeadThenSort
`source=opensearch-sql_test_index_bank | head 2 | sort age | fields age`

---

## CalciteDateTimeComparisonIT.testCompare
Data-driven; one method, query template:
```
source=opensearch-sql_test_index_datatypes_nonnumeric | eval `<name>` = <LEFT op RIGHT> | fields `<name>`
```
`2026-06-03` in the listed cases = `LocalDate.now()` at run time. Each listed row substitutes its comparison expression for `<LEFT op RIGHT>` and asserts the boolean. Examples:
```
source=...datatypes_nonnumeric | eval `d_t` = DATE('2026-06-03') = TIME('00:00:00') | fields `d_t`          (=> true)
source=...datatypes_nonnumeric | eval `t_d` = TIME('00:00:00') = DATE('2026-06-03') | fields `t_d`          (=> true)
source=...datatypes_nonnumeric | eval `t_ts` = TIME('10:20:30') <= TIMESTAMP('2026-06-03 10:20:30') | fields `t_ts`  (=> true)
source=...datatypes_nonnumeric | eval `ts_t` = TIMESTAMP('2026-06-03 10:20:30') = TIME('10:20:30') | fields `ts_t`   (=> true)
source=...datatypes_nonnumeric | eval `ts_t` = TIMESTAMP('2026-06-03 20:50:42') > TIME('10:20:30') | fields `ts_t`   (=> true)
```
The `!=` / `<` / `>` rows that expect `false` use the same template with the corresponding operator.

---

## CalciteDateTimeFunctionIT
*(index: `..._date` unless noted)*

### testWeek
`source=..._date | eval f = week(date('2008-02-20')) | fields f`
### testWeek_of_year
`source=..._date | eval f = week_of_year(date('2008-02-20')) | fields f`
### testMicrosecond
```
source=..._date | eval f = microsecond(timestamp('2020-09-16 17:30:00.123456')) | fields f
source=..._date | eval f = microsecond(timestamp('2020-09-16 17:30:00.1234')) | fields f
source=..._date | eval f = microsecond(time('17:30:00.000010')) | fields f
```
### testDateFormat
```
source=..._date | eval f = date_format(timestamp('1998-01-31 13:14:15.012345'), '%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M %m %p %r %S %s %T %% %P') | fields f
source=..._date | eval f = date_format('1998-01-31 13:14:15.012345', '...same format...') | fields f
source=..._date | eval f = date_format(date('1998-01-31'), '%U %u %V %v %W %w %X %x %Y %y') | fields f
source=..._date | eval f = date_format('1998-01-31', '%U %u %V %v %W %w %X %x %Y %y') | fields f
```
### testStrftimeWithDateFields
```
source=..._date_formats | eval formatted = strftime(epoch_millis, '%Y-%m-%d %H:%M:%S') | fields epoch_millis, formatted | head 1
source=..._date_formats | eval unix_ts = unix_timestamp(date_time) | eval formatted = strftime(unix_ts, '%F') | fields formatted | head 1
```
### testMakeTime
`source=..._date | eval f1 = MAKETIME(20, 30, 40), f2 = MAKETIME(20.2, 49.5, 42.100502) | fields f1, f2`
### testMakeDate
`source=..._date | eval f1 = MAKEDATE(1945, 5.9), f2 = MAKEDATE(1984, 1984) | fields f1, f2`
### testStrToDate
```
source=..._date | eval f = str_to_date('01,5,2013', '%d,%m,%Y') | fields f
source=..._date | eval f = str_to_date('1-May-13', '%d-%b-%y') | fields f
```
### testUnixTimeStamp
`source=..._date | eval f1 = UNIX_TIMESTAMP(MAKEDATE(1984, 1984)), f2 = UNIX_TIMESTAMP(TIMESTAMP('2003-12-31 12:00:00')), f3 = UNIX_TIMESTAMP(20771122143845) | fields f1, f2, f3`

### CalciteDateTimeImplementationIT.inRangeNoToTZ
`source=..._date | eval f = DATETIME('2008-01-01 02:00:00+10:00') | fields f`

---

## CalcitePPLCastFunctionIT
*(index: `..._date_formats`; testCastBOOLEAN uses `..._datatypes_nonnumeric`)*

### testCastDate
```
source=..._date_formats | eval a = cast('1984-04-12' as DATE) | fields a
source=..._date_formats | head 1 | eval a = cast('2023-10-01 12:00:00' as date) | fields a
source=..._date_formats | eval a = cast('09:07:42' as DATE) | fields a   (expected to throw)
```
### testCastTimestamp
```
source=..._date_formats | eval a = cast('1984-04-12 09:07:42' as TIMESTAMP) | fields a
source=..._date_formats | head 1 | eval a = cast('2023-10-01 12:00:00.123456' as timestamp) | fields a
source=..._date_formats | eval a = cast('1984-04-12' as TIMESTAMP) | fields a
source=..._date_formats | eval a = cast('09:07:42' as TIMESTAMP) | fields a   (expected to throw)
```
### testCastTime
```
source=..._date_formats | eval a = cast('09:07:42' as TIME) | fields a
source=..._date_formats | head 1 | eval a = cast('09:07:42.12345' as TIME) | fields a
source=..._date_formats | head 1 | eval a = cast('1985-10-09 12:00:00' as time) | fields a
source=..._date_formats | eval a = cast('1984-04-12' as TIME) | fields a   (expected to throw)
```
### testCastBOOLEAN
```
source=..._datatypes_nonnumeric | eval a = cast(boolean_value as INT) | fields a
source=..._datatypes_nonnumeric | eval a = cast(boolean_value as LONG) | fields a
source=..._datatypes_nonnumeric | eval a = cast(boolean_value as STRING) | fields a
```

---

## CalcitePPLBuiltinFunctionsNullIT
*(index: `..._date_formats_with_null`)*

### testStrTDateInvalid1
`source=..._date_formats_with_null | eval a = str_to_date('01,13,2013', '%d,%m,%Y')`   (expected to throw)
### testHourInvalid
`source=..._date_formats_with_null | eval h1 = HOUR('2020-08-26') | fields h1`   (expected to throw)

---

## CalcitePPLCaseFunctionIT

### testCaseAggWithNullValues
`source=..._state_country_with_null | eval age_category = case(age < 20, 'teenager', age < 70, 'adult', age >= 70, 'senior' else 'unknown') | stats avg(age) by age_category`
### testCaseWhenWithIn
`source=..._weblogs | eval status = case(response in ('200'), 'Success', response in ('300', '301'), 'Redirection', response in ('400', '403'), 'Client Error', response in ('500', '505'), 'Server Error' else concat('Incorrect HTTP status code for', url)) | where status != 'Success' | fields host, method, message, bytes, response, url, status`
### testCaseWhenInSubquery
`source=..._weblogs | where response in [ source = ..._weblogs | eval new_response = case(response in ('200'), '201', response in ('300', '301'), '301', response in ('400', '403'), '403', response in ('500', '505'), '500' else concat('Incorrect HTTP status code for', url)) | fields new_response ] | fields host, method, message, bytes, response, url`
### testCaseWhenNoElse
`source=..._weblogs | eval status = case(cast(response as int) >= 200 AND cast(response as int) < 300, 'Success', cast(response as int) >= 300 AND cast(response as int) < 400, 'Redirection', cast(response as int) >= 400 AND cast(response as int) < 500, 'Client Error', cast(response as int) >= 500 AND cast(response as int) < 600, 'Server Error') | where isnull(status) OR status != 'Success' | fields host, method, message, bytes, response, url, status`
### testCaseWhenWithCast
`source=..._weblogs | eval status = case(... same 4 cast ranges ... else concat('Incorrect HTTP status code for', url)) | where status != 'Success' | fields host, method, message, bytes, response, url, status`
### testCaseCanBePushedDownAsRangeQuery
```
source=..._bank | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100') | stats avg(age) as avg_age by age_range
source=..._bank | eval age_range = case(age < 30, 'u30', age >= 30 and age < 40, 'u40' else 'u100') | stats avg(age) by age_range
source=..._bank | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100'), balance_range = case(balance < 20000, 'medium' else 'high') | stats avg(balance) as avg_balance by age_range, balance_range
source=..._bank | eval age_range = case(age < 30, 'u30', (age >= 35 and age < 40) or age >= 80, '30-40 or >=80') | stats avg(balance) by age_range
source=..._bank | eval age_range = case(age < 30, 'u30', age >= 30 and age <= 40, 'u40' else 'u100') | stats avg(age) as avg_age by age_range
```

---

## CalcitePPLConditionBuiltinFunctionIT
*(index: `..._state_country_with_null`)*

### testEvalIsNullWithIf
`source=..._state_country_with_null | eval n=if(isnull(name), 'yes', 'no') | fields name, n`
### testIsNotNullWithSingleNotEquals
`source=..._state_country_with_null | where name != 'Jake' and isnotnull(name) | fields name`
### testIfNull
`source=..._state_country_with_null | eval new_name = ifnull(name, 'Unknown') | fields new_name, age`
### testNullIfWithExpression
`source=..._state_country_with_null | eval new_name = Nullif(concat('H', name), 'HHello') | fields name, new_name`
### testIsNotNull
`source=..._state_country_with_null | where isnotnull(name) | fields name`
### testEvalIsNullInComplexExpression
`source=..._state_country_with_null | eval safe_name=if(isnull(name), 'Unknown', name) | fields safe_name, age`
### testIf
`source=..._state_country_with_null | where isnotnull(age) | eval judge = if(age>50, 'old', 'young') | fields judge, age`
### testIsBlank
`source=..._state_country_with_null | where isblank(name) | fields name, age`
### testIsEmpty
`source=..._state_country_with_null | where isempty(name) | fields name, age`
### testNullIf
`source=..._state_country_with_null | eval new_age = nullif(age, 20) | fields name, new_age`
### testEvalIsNotNullDirect
`source=..._state_country_with_null | eval is_not_null_name=isnotnull(name) | fields name, is_not_null_name`
### testIsPresent
`source=..._state_country_with_null | where ispresent(name) | fields name, age`

---

## CalciteDataTypeIT

### test_nonnumeric_data_types
`source=opensearch-sql_test_index_datatypes_nonnumeric`
### testNumericFieldFromString
`source=opensearch-sql_test_index_datatypes_numeric | where long_number=12345678 | fields long_number, integer_number, double_number, float_number`  *(after inserting a doc via REST)*

---

## CalcitePPLBuiltinFunctionIT
*(index: `..._datatypes_numeric`)*

### testDivide
`source=..._datatypes_numeric | eval r1 = 22 / 7, r2 = integer_number / 1, r3 = 21 / 7, r4 = byte_number / short_number, r5 = half_float_number / float_number, r6 = float_number / short_number, r7 = 22 / 7.0, r8 = 22.0 / 7, r9 = 21.0 / 7.0, r10 = half_float_number / short_number, r11 = double_number / float_number | fields r1..r11`
### testModFloatAndNegative
`source=..._datatypes_numeric | eval f = mod(float_number, 2), n = -1 * short_number % 2, nd = -1 * double_number % 2 | fields f, n, nd`
### testModShouldReturnWiderTypes
`source=..._datatypes_numeric | eval b = byte_number % 2, i = mod(integer_number, 3), l = mod(long_number, 2), f = float_number % 2, d = mod(double_number, 2), s = short_number % byte_number | fields b, i, l, f, d, s`

---

## CalcitePPLStringBuiltinFunctionIT
*(index: `..._state_country`)*

### testTrim
`source=..._state_country | where Trim(name) = 'Jim' | fields name, age`
### testLTrim
`source=..._state_country | where LTrim(name) = 'Jim' | fields name, age`
### testReverse
`source=..._state_country | where Reverse(name) = name | fields name, age`

---

## CalcitePPLJsonBuiltinFunctionIT
*(index: `..._people2`)*

### testJsonSetWithDollarPrefixedPath
`source=..._people2 | eval a = json_set('{"name":"alice","scores":[90,85,92]}', '$.name', 'modified_alice') | fields a | head 1`
### testJsonDeleteWithDollarPrefixedPath
`source=..._people2 | eval a = json_delete('{"name":"alice","scores":[90,85,92]}', '$.name') | fields a | head 1`

---

## CalcitePPLDedupIT
*(index: `..._duplication_nullable`)*

### testDedupComplex
```
source=..._duplication_nullable | dedup 1 name
source=..._duplication_nullable | fields category, name | dedup 1 name
source=..._duplication_nullable | dedup 1 name, category
source=..._duplication_nullable | fields category, id, name | dedup 2 name, category
```
### testDedupExpr
```
source=..._duplication_nullable | eval new_name = lower(name) | dedup 1 new_name
source=..._duplication_nullable | fields category, name, id | eval new_name = lower(name), new_category = lower(category) | dedup 1 new_name, new_category
source=..._duplication_nullable | eval new_name = lower(name), new_category = lower(category) | dedup 2 name, category
source=..._duplication_nullable | fields category, id, name | eval new_name = lower(name) | eval new_category = lower(category) | sort name, -category | dedup 2 new_name, new_category
```

---

## CalcitePPLAppendCommandIT

### testAppendWithMergedColumn
`source=..._account | stats sum(age) as sum by gender | append [ source=..._account | stats sum(age) as sum by state | sort sum ] | head 5`

---

## CalciteChartCommandIT
*(index: `..._time_data`)*

### testChartMaxValueOverCategoryByTimestampSpanWeek
`source=..._time_data | chart max(value) over category by timestamp span=1week`
### testChartMaxValueByTimestampSpanDayAndWeek
`source=..._time_data | chart max(value) by timestamp span=1day, @timestamp span=2weeks`

---

## CalciteLikeQueryIT / CalciteMultiMatchIT / CalciteQueryStringIT / CalciteSimpleQueryStringIT
*(indices: `..._wildcard`, `..._beer`)*

### CalciteLikeQueryIT.test_convert_field_text_to_keyword
`source=..._wildcard | WHERE Like(TextKeywordBody, '*') | fields TextKeywordBody`
### CalciteLikeQueryIT.test_the_default_3rd_option
`source=..._wildcard | WHERE Like(KeywordBody, 'test Wildcard%') | fields KeywordBody`
### CalciteMultiMatchIT.test_wildcard_multi_match
```
SOURCE=..._beer | WHERE multi_match(['Tags'], 'taste') | fields Id
SOURCE=..._beer | WHERE multi_match(['T*'], 'taste') | fields Id
source=..._beer | where simple_query_string(['*Date'], '2014-01-22')
```
### CalciteQueryStringIT.wildcard_test
```
source=..._beer | where query_string(['Tags'], 'taste')
source=..._beer | where query_string(['T*'], 'taste')
source=..._beer | where query_string(['*Date'], '2014-01-22')
```
### CalciteSimpleQueryStringIT.test_wildcard_simple_query_string
```
SOURCE=..._beer | WHERE simple_query_string(['Tags'], 'taste') | fields Id
SOURCE=..._beer | WHERE simple_query_string(['T*'], 'taste') | fields Id
source=..._beer | where simple_query_string(['*Date'], '2014-01-22')
```

---

## CommentIT
*(index: `..._account`)*

### testBlockComment
```
search /* block comment */ source=..._account /* block comment */ | fields /* … */ firstname | /* … */ where firstname='Amber' | fields /* … */ firstname
```
*(with a leading multi-line block comment; equivalent to `search source=..._account | fields firstname | where firstname='Amber' | fields firstname`)*
### testLineComment
```
source=..._account | fields firstname // line comment
| where firstname='Amber' // line comment
| fields firstname // line comment
```
### testMultipleLinesCommand
`source=..._account | fields firstname | where firstname='Amber' | fields firstname`  *(split across multiple lines)*

---

## CalciteParseCommandIT
*(index: `..._bank`; all expected to error on invalid capture-group name)*

### testParseErrorInvalidGroupNameHyphen
`source=..._bank | parse email '.+@(?<host-name>.+)' | fields email`
### testParseErrorInvalidGroupNameSpecialCharacter
`source=..._bank | parse email '.+@(?<host@name>.+)' | fields email`
### testParseErrorInvalidGroupNameStartingWithDigit
`source=..._bank | parse email '.+@(?<1host>.+)' | fields email`
### testParseErrorInvalidGroupNameUnderscore
`source=..._bank | parse email '.+@(?<host_name>.+)' | fields email`

---

## CalciteRexCommandIT
*(index: `..._account`; the invalid-group-name ones expected to error)*

### testRexErrorInvalidGroupNameHyphen
`source=..._account | rex field=email "(?<user-name>[^@]+)@(?<domain>.+)" | fields email`
### testRexErrorInvalidGroupNameSpecialCharacter
`source=..._account | rex field=email "(?<user@name>[^@]+)@(?<domain>.+)" | fields email`
### testRexErrorInvalidGroupNameStartingWithDigit
`source=..._account | rex field=email "(?<1user>[^@]+)@(?<domain>.+)" | fields email`
### testRexErrorInvalidGroupNameUnderscore
`source=..._account | rex field=email "(?<user_name>[^@]+)@(?<domain>.+)" | fields email`
### testRexWithWhere
`source=..._account | where state="CA" | rex field=email "(?<user>[^@]+)@(?<domain>.+)" | fields email, user, domain`

---

## CalciteErrorReportStageIT
*(index: `..._account`; all error-path tests)*

### testFieldNotFoundErrorIncludesStage
`source=..._account | fields nonexistent_field`
### testIndexNotFoundErrorIncludesStage
`source=nonexistent_index | fields age`
### testMultipleFieldErrorsIncludeStage
`source=..._account | fields nonexistent1, nonexistent2, nonexistent3`
### testStageDescriptionIsUserFriendly
`source=..._account | fields undefined_field`
### testLocationMessagesAreUserFriendly
`source=..._account | fields xyz123`

---

## CalcitePPLExplainIT

### testExplainCommand
`explain source=test | where age = 20 | fields name, age`

---

## CalcitePPLPatternsIT

### testBrainLabelMode_ShowNumberedToken
`source=..._hdfs_logs | patterns content method=BRAIN mode=label max_sample_count=5 show_numbered_token=true variable_count_threshold=5 frequency_threshold_percentage=0.2 | head 2 | fields content, patterns_field, tokens`
### testBrainParseWithUUID_ShowNumberedToken
`source=..._weblogs | eval body = '[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d user_currency=USD' | head 1 | patterns body method=BRAIN mode=label show_numbered_token=true | fields patterns_field, tokens`

---

## CalcitePPLRenameIT

### testRenameFullWildcardExcludesMetadataFields
`source = ..._state_country | rename * as old_*`

---

## CalciteSystemFunctionIT

### typeof_opensearch_types
```
source=..._datatypes_numeric | eval `double` = typeof(double_number), `long` = typeof(long_number), `integer` = typeof(integer_number), `byte` = typeof(byte_number), `short` = typeof(short_number), `float` = typeof(float_number), `half_float` = typeof(half_float_number), `scaled_float` = typeof(scaled_float_number) | fields ...
source=..._datatypes_nonnumeric | eval `text` = typeof(text_value), `date` = typeof(date_value), `date_nanos` = typeof(date_nanos_value), `boolean` = typeof(boolean_value), `object` = typeof(object_value), `keyword` = typeof(keyword_value), `ip` = typeof(ip_value), `binary` = typeof(binary_value), `geo_point` = typeof(geo_point_value) | fields ...
```

---

## FieldsCommandIT

### testEnhancedFieldsWhenCalciteDisabled
```
source=..._account | fields *
source=..._account | fields account_*
source=..._account | fields account_number balance firstname
source=..._account | fields account_number balance, firstname
```

---

## FetchSizeIT
*(index: `..._account`; each sets a fetch_size and asserts pagination)*

### testFetchSizeAsUrlParameter
`source=..._account | fields firstname`
### testFetchSizeLimitsResults
`source=..._account`
### testFetchSizeWithSort
`source=..._account | sort age | fields firstname, age`
### testFetchSizeJsonBodyTakesPrecedenceOverUrlParam
`source=..._account | fields firstname`
### testFetchSizeWithFields
`source=..._account | fields firstname, age`
### testFetchSizeWithFilter
`source=..._account | where age > 30`
### testFetchSizeSmallerThanHead
`source=..._account | head 100 | fields firstname`
### testFetchSizeWithRename
`source=..._account | rename firstname as first_name | fields first_name, age`
### testFetchSizeOne
`source=..._account | fields firstname`
### testFetchSizeWithEval
`source=..._account | eval age_plus_10 = age + 10 | fields firstname, age, age_plus_10`

---

## Dashboard ITs

### NfwPplDashboardIT.testTopLongLivedTCPFlows
`source=nfw_logs | WHERE \`event.proto\` = 'TCP' and \`event.netflow.age\` > 350 | STATS count() as Count by SPAN(\`event.timestamp\`, 2d) as timestamp_span, \`event.src_ip\`, \`event.src_port\`, \`event.dest_ip\`, \`event.dest_port\` | EVAL \`Src IP:Port - Dst IP:Port\` = CONCAT(\`event.src_ip\`, ": ", CAST(\`event.src_port\` AS STRING), " - ", \`event.dest_ip\`, ": ", CAST(\`event.dest_port\` AS STRING)) | SORT - Count | HEAD 10`
### WafPplDashboardIT.testTotalBlockedRequests
`source=waf_logs | WHERE action = "BLOCK" | STATS count()`
### WafPplDashboardIT.testTopRequestURIs
`source=waf_logs | stats count() as Count by \`httpRequest.uri\` | sort - Count | head 10`
