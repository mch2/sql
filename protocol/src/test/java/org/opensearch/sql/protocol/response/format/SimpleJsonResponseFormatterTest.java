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
import org.opensearch.sql.monitor.AlwaysHealthyMonitor;
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

  // ─── streaming writer + heap guards ────────────────────────────────────────────

  /** Passing an explicit (healthy) monitor produces the exact same output as the default ctor. */
  @Test
  void formatResponseWithExplicitHealthyMonitor() {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter =
        new SimpleJsonResponseFormatter(COMPACT, new AlwaysHealthyMonitor());
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":"
            + "[[\"John\",20],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  /** Hard per-response byte cap: a result whose serialized size exceeds the cap is rejected. */
  @Test
  void formatRejectsWhenResultExceedsByteCap() {
    QueryResult response = manyRows(50);
    // Tiny cap (10 bytes) so the first row already trips it; health-check interval huge so only the
    // cap branch fires.
    SimpleJsonResponseFormatter formatter =
        new SimpleJsonResponseFormatter(COMPACT, new AlwaysHealthyMonitor(), 10L, Long.MAX_VALUE);
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> formatter.format(response));
    assertTrue(e.getMessage().contains("too large to serialize"));
  }

  /** Byte-interval heap poll: when the monitor reports unhealthy past the interval, reject. */
  @Test
  void formatRejectsWhenMonitorUnhealthyPastInterval() {
    QueryResult response = manyRows(50);
    // High cap (never the cap branch), tiny health-check interval (1 byte) so the monitor is polled
    // after the first row, and an unhealthy monitor → rejection.
    SimpleJsonResponseFormatter formatter =
        new SimpleJsonResponseFormatter(COMPACT, new UnhealthyMonitor(), Long.MAX_VALUE, 1L);
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> formatter.format(response));
    assertTrue(e.getMessage().contains("Insufficient memory"));
  }

  /** A healthy monitor at a tiny interval is polled repeatedly and the response still completes. */
  @Test
  void formatCompletesWhenMonitorHealthyAtTinyInterval() {
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))));
    SimpleJsonResponseFormatter formatter =
        new SimpleJsonResponseFormatter(COMPACT, new AlwaysHealthyMonitor(), Long.MAX_VALUE, 1L);
    assertEquals(
        "{\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":"
            + "[[\"John\",20],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  /** buildJsonObject is unreachable on the success path (format is overridden) — guards misuse. */
  @Test
  void buildJsonObjectThrows() {
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(COMPACT);
    assertThrows(UnsupportedOperationException.class, () -> formatter.buildJsonObject(manyRows(1)));
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
   * An IOException from the underlying writer is wrapped as an UncheckedIOException (the
   * IOException catch in format()). Injected via the newResponseBuffer seam with a StringWriter
   * that throws on close (JsonWriter.close() flushes/closes the delegate).
   */
  @Test
  void formatWrapsIOExceptionAsUnchecked() {
    QueryResult response =
        new QueryResult(
            schema,
            Collections.singletonList(tupleValue(ImmutableMap.of("firstname", "John", "age", 20))));
    SimpleJsonResponseFormatter formatter =
        new SimpleJsonResponseFormatter(COMPACT) {
          @Override
          java.io.StringWriter newResponseBuffer(QueryResult r) {
            return new java.io.StringWriter() {
              @Override
              public void close() throws java.io.IOException {
                throw new java.io.IOException("boom");
              }
            };
          }
        };
    java.io.UncheckedIOException e =
        assertThrows(java.io.UncheckedIOException.class, () -> formatter.format(response));
    assertTrue(e.getMessage().contains("Failed to serialize query response"));
  }

  private QueryResult manyRows(int n) {
    List<ExprValue> rows = new java.util.ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      rows.add(tupleValue(ImmutableMap.of("firstname", "name" + i, "age", i)));
    }
    return new QueryResult(schema, rows);
  }

  /** Test monitor that always reports unhealthy, to exercise the rejection branch. */
  private static final class UnhealthyMonitor extends org.opensearch.sql.monitor.ResourceMonitor {
    @Override
    protected boolean isHealthyImpl() {
      return false;
    }
  }
}
