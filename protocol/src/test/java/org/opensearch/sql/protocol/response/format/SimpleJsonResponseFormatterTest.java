/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.protocol.response.QueryResult;

class SimpleJsonResponseFormatterTest {

  private final ExecutionEngine.Schema schema =
      new ExecutionEngine.Schema(
          ImmutableList.of(
              new ExecutionEngine.Schema.Column("firstname", null, STRING),
              new ExecutionEngine.Schema.Column("age", null, INTEGER)));

  @Test
  void formatResponse() {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":"
            + "[[\"John\",20],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  @Test
  void formatResponsePretty() {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(PRETTY);
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Smith\",\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        formatter.format(response));
  }

  @Test
  void formatResponseSchemaWithAlias() {
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(new ExecutionEngine.Schema.Column("firstname", "name", STRING)));
    QueryResult response =
        new QueryResult(
            schema, ImmutableList.of(tupleValue(ImmutableMap.of("name", "John", "age", 20))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"name\",\"type\":\"string\"}],"
            + "\"datarows\":[[\"John\",20]],\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithMissingValue() {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                ExprTupleValue.fromExprValueMap(
                    ImmutableMap.of("firstname", stringValue("John"), "age", LITERAL_MISSING)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"John\",null],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithTupleValue() {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(
                    ImmutableMap.of(
                        "name",
                        "Smith",
                        "address",
                        ImmutableMap.of(
                            "state", "WA", "street", ImmutableMap.of("city", "seattle"))))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);

    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"Smith\",{\"state\":\"WA\",\"street\":{\"city\":\"seattle\"}}]],"
            + "\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithArrayValue() {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(
                    ImmutableMap.of(
                        "name",
                        "Smith",
                        "address",
                        Arrays.asList(
                            ImmutableMap.of("state", "WA"), ImmutableMap.of("state", "NYC"))))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"Smith\",[{\"state\":\"WA\"},{\"state\":\"NYC\"}]]],"
            + "\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  @Test
  void formatResponseWithHighlights() {
    ExecutionEngine.Schema schemaWithHighlight =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column("firstname", null, STRING),
                new ExecutionEngine.Schema.Column("age", null, INTEGER),
                new ExecutionEngine.Schema.Column("_highlight", null, STRING)));
    java.util.LinkedHashMap<String, ExprValue> map = new java.util.LinkedHashMap<>();
    map.put("firstname", ExprValueUtils.stringValue("John"));
    map.put("age", ExprValueUtils.integerValue(20));
    map.put(
        "_highlight",
        ExprTupleValue.fromExprValueMap(
            Map.of("firstname", collectionValue(List.of("<em>John</em>")))));
    ExprValue row = ExprTupleValue.fromExprValueMap(map);
    QueryResult response = new QueryResult(schemaWithHighlight, Collections.singletonList(row));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    String result = formatter.format(response);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"},"
            + "{\"name\":\"_highlight\",\"type\":\"string\"}],"
            + "\"datarows\":[[\"John\",20,{\"firstname\":[\"<em>John</em>\"]}]],"
            + "\"total\":1,\"size\":1}",
        result);
  }

  @Test
  void formatResponseWithoutHighlights() {
    QueryResult response =
        new QueryResult(
            schema,
            Collections.singletonList(tupleValue(ImmutableMap.of("firstname", "John", "age", 20))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    String result = formatter.format(response);
    // highlights field should not be present when no highlight data exists
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],"
            + "\"datarows\":[[\"John\",20]],\"total\":1,\"size\":1}",
        result);
  }

  @Test
  void formatError() {
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"type\":\"RuntimeException\",\"reason\":\"This is an exception\"}",
        formatter.format(new RuntimeException("This is an exception")));
  }

  @Test
  void formatErrorPretty() {
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(PRETTY);
    assertEquals(
        "{\n"
            + "  \"type\": \"RuntimeException\",\n"
            + "  \"reason\": \"This is an exception\"\n"
            + "}",
        formatter.format(new RuntimeException("This is an exception")));
  }

  // ─── streaming + serialization fidelity ─────────────────────────────────────────

  /** The streaming Writer overload produces byte-identical output to the String overload. */
  @Test
  void formatToWriterMatchesStringOverload() throws java.io.IOException {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    java.io.StringWriter sw = new java.io.StringWriter();
    formatter.format(response, sw);
    assertEquals(formatter.format(response), sw.toString());
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":"
            + "[[\"John\",20],[\"Smith\",30]],\"total\":2,\"size\":2}",
        sw.toString());
  }

  /** Double / float cells go through the floating-point branch of writeValue. */
  @Test
  void formatResponseWithDoubleAndFloatValues() {
    ExecutionEngine.Schema fpSchema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column(
                    "d", null, org.opensearch.sql.data.type.ExprCoreType.DOUBLE),
                new ExecutionEngine.Schema.Column(
                    "f", null, org.opensearch.sql.data.type.ExprCoreType.FLOAT)));
    java.util.LinkedHashMap<String, ExprValue> row = new java.util.LinkedHashMap<>();
    row.put("d", ExprValueUtils.doubleValue(1.5));
    row.put("f", ExprValueUtils.floatValue(2.5f));
    QueryResult response =
        new QueryResult(fpSchema, Collections.singletonList(ExprTupleValue.fromExprValueMap(row)));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"d\",\"type\":\"double\"},{\"name\":\"f\",\"type\":\"float\"}],"
            + "\"datarows\":[[1.5,2.5]],\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  /** Boolean cells go through the boolean branch of writeValue. */
  @Test
  void formatResponseWithBooleanValue() {
    ExecutionEngine.Schema boolSchema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column(
                    "flag", null, org.opensearch.sql.data.type.ExprCoreType.BOOLEAN)));
    QueryResult response =
        new QueryResult(
            boolSchema, Collections.singletonList(tupleValue(ImmutableMap.of("flag", true))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"flag\",\"type\":\"boolean\"}],"
            + "\"datarows\":[[true]],\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  /**
   * When a profiling context is active, {@code QueryProfiling.current().finish()} returns a
   * non-null profile and the formatter emits a "profile" object (the profile-present branch of
   * writeJson).
   */
  @Test
  void formatResponseEmitsProfileWhenProfilingActive() {
    QueryResult response =
        new QueryResult(
            schema,
            Collections.singletonList(tupleValue(ImmutableMap.of("firstname", "John", "age", 20))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    String result =
        org.opensearch.sql.monitor.profile.QueryProfiling.withCurrentContext(
            new org.opensearch.sql.monitor.profile.DefaultProfileContext(),
            () -> formatter.format(response));
    assertTrue(result.contains("\"profile\""), "response must contain the profile object");
    assertTrue(result.contains("\"datarows\":[[\"John\",20]]"), "datarows must still be present");
  }

  /**
   * PRETTY style: the embedded profile JSON is pretty-printed (the pretty branch of writeProfile).
   */
  @Test
  void formatEmitsPrettyProfileWhenPretty() {
    QueryResult response =
        new QueryResult(
            schema,
            Collections.singletonList(tupleValue(ImmutableMap.of("firstname", "John", "age", 20))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(PRETTY);
    String result =
        org.opensearch.sql.monitor.profile.QueryProfiling.withCurrentContext(
            new org.opensearch.sql.monitor.profile.DefaultProfileContext(),
            () -> formatter.format(response));
    assertTrue(result.contains("\"profile\""), "response must contain the profile object");
    // pretty printing → the profile object contains newline+indent, not a compact one-liner.
    assertTrue(
        result.contains("\"profile\": {\n"), "profile must be pretty-printed under PRETTY style");
  }

  /** contentType is JSON. */
  @Test
  void contentTypeIsJson() {
    assertEquals(
        "application/json; charset=UTF-8", new SimpleJsonResponseFormatter(COMPACT).contentType());
  }

  /**
   * A BigDecimal with a fractional part and a BigInteger beyond Long.MAX_VALUE serialize EXACTLY —
   * regression for the old Double/Float-vs-longValue() branching, which truncated both.
   */
  @Test
  void formatPreservesBigDecimalAndBigInteger() {
    ExecutionEngine.Schema numSchema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column(
                    "dec", null, org.opensearch.sql.data.type.ExprCoreType.DOUBLE),
                new ExecutionEngine.Schema.Column(
                    "big", null, org.opensearch.sql.data.type.ExprCoreType.LONG)));
    // QueryResult iterates Object[] rows directly; construct raw Number cells to exercise
    // writeValue.
    java.math.BigInteger big =
        java.math.BigInteger.valueOf(Long.MAX_VALUE).add(java.math.BigInteger.TEN);
    QueryResult response =
        new QueryResult(numSchema, Collections.emptyList()) {
          @Override
          public java.util.Iterator<Object[]> iterator() {
            return Collections.singletonList(new Object[] {new java.math.BigDecimal("2.75"), big})
                .iterator();
          }

          @Override
          public java.util.Map<String, String> columnNameTypes() {
            return ImmutableMap.of("dec", "double", "big", "long");
          }

          @Override
          public int size() {
            return 1;
          }
        };
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertEquals(
        "{\"schema\":[{\"name\":\"dec\",\"type\":\"double\"},{\"name\":\"big\",\"type\":\"long\"}],"
            + "\"datarows\":[[2.75,9223372036854775817]],\"total\":1,\"size\":1}",
        formatter.format(response));
  }

  /** IOException from the underlying Writer is wrapped as UncheckedIOException. */
  @Test
  void formatWrapsIOExceptionAsUnchecked() {
    QueryResult response =
        new QueryResult(
            schema,
            Collections.singletonList(tupleValue(ImmutableMap.of("firstname", "John", "age", 20))));
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    java.io.Writer throwing =
        new java.io.Writer() {
          @Override
          public void write(char[] cbuf, int off, int len) throws java.io.IOException {
            throw new java.io.IOException("boom");
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    java.io.UncheckedIOException e =
        assertThrows(
            java.io.UncheckedIOException.class, () -> formatter.format(response, throwing));
    assertTrue(e.getMessage().contains("Failed to serialize query response"));
  }
}
