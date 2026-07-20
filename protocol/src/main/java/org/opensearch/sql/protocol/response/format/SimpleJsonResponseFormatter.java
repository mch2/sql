/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.opensearch.sql.protocol.response.format.ErrorFormatter.compactFormat;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.compactJsonify;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.prettyFormat;
import static org.opensearch.sql.protocol.response.format.ErrorFormatter.prettyJsonify;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Map;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * JSON response format with schema header and data rows. Writes JSON incrementally via {@link
 * JsonWriter} so the response is never materialized as a Gson object tree — no intermediate {@code
 * Object[][]} copy, no reflection tree.
 *
 * <p>This formatter is <b>policy-free</b>: memory backpressure (the per-response cap and
 * heap-monitor polling) lives at the plugin layer via a guarded {@link Writer} passed to {@link
 * #format(QueryResult, Writer)}. The protocol module therefore has no dependency on resource
 * monitoring.
 *
 * <pre>
 *  {
 *      "schema": [{"name": "col", "type": "string"}, ...],
 *      "datarows": [["val1"], ["val2"], ...],
 *      "total": 2,
 *      "size": 2
 *  }
 * </pre>
 */
public class SimpleJsonResponseFormatter implements ResponseFormatter<QueryResult> {

  private final JsonResponseFormatter.Style style;

  public SimpleJsonResponseFormatter(JsonResponseFormatter.Style style) {
    this.style = style;
  }

  /**
   * Serializes to a {@code String} via an in-memory {@link StringWriter}. The transport contract
   * requires a single String; peak heap is that buffer. Callers that need a size/heap guard while
   * the buffer grows use {@link #format(QueryResult, Writer)} with a guarded Writer instead.
   */
  @Override
  public String format(QueryResult response) {
    StringWriter sw = new StringWriter(Math.min(response.size() * 64, 1024 * 1024));
    format(response, sw);
    return sw.toString();
  }

  /**
   * Streaming entry point: writes the response JSON incrementally to {@code out}. A guarding {@link
   * Writer} decorator (plugin layer) observes the growth per row and can abort mid-serialization.
   */
  public void format(QueryResult response, Writer out) {
    ProfileMetric formatMetric = QueryProfiling.current().getOrCreateMetric(MetricName.FORMAT);
    long formatTime = System.nanoTime();
    try {
      writeJson(response, out);
      formatMetric.set(System.nanoTime() - formatTime);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize query response", e);
    }
  }

  @Override
  public String format(Throwable t) {
    return (style == PRETTY) ? prettyFormat(t) : compactFormat(t);
  }

  @Override
  public String contentType() {
    return JsonResponseFormatter.CONTENT_TYPE;
  }

  // ─── Internal streaming writer ─────────────────────────────────────────────────

  private void writeJson(QueryResult response, Writer out) throws IOException {
    try (JsonWriter w = new JsonWriter(out)) {
      if (style == PRETTY) {
        w.setIndent("  ");
      }
      w.beginObject();

      writeSchema(w, response);

      w.name("datarows").beginArray();
      for (Object[] row : response) {
        writeRow(w, row);
        // Flush per row so a guarding Writer sees growth incrementally (JsonWriter buffers small
        // amounts internally; without this the guard could miss per-row growth between polls).
        w.flush();
      }
      w.endArray();

      w.name("total").value(response.size());
      w.name("size").value(response.size());

      QueryProfile profile = QueryProfiling.current().finish();
      if (profile != null) {
        w.name("profile");
        writeProfile(w, profile);
      }

      w.endObject();
    }
  }

  private void writeSchema(JsonWriter w, QueryResult response) throws IOException {
    w.name("schema").beginArray();
    Map<String, String> colTypes = response.columnNameTypes();
    for (Map.Entry<String, String> entry : colTypes.entrySet()) {
      w.beginObject();
      w.name("name").value(entry.getKey());
      w.name("type").value(entry.getValue());
      w.endObject();
    }
    w.endArray();
  }

  private void writeRow(JsonWriter w, Object[] row) throws IOException {
    w.beginArray();
    for (Object cell : row) {
      writeValue(w, cell);
    }
    w.endArray();
  }

  private void writeValue(JsonWriter w, Object value) throws IOException {
    if (value == null) {
      w.nullValue();
    } else if (value instanceof Number num) {
      // JsonWriter.value(Number) preserves every Number subtype exactly (incl.
      // BigDecimal/BigInteger);
      // the old Double/Float-vs-longValue() branching truncated BigDecimal/BigInteger.
      w.value(num);
    } else if (value instanceof Boolean b) {
      w.value(b);
    } else if (value instanceof Map<?, ?> map) {
      // Nested object (e.g. an object field / tuple value) — emit real nested JSON, not a toString.
      w.beginObject();
      for (Map.Entry<?, ?> e : map.entrySet()) {
        w.name(String.valueOf(e.getKey()));
        writeValue(w, e.getValue());
      }
      w.endObject();
    } else if (value instanceof Iterable<?> iterable) {
      // Nested array (e.g. a multi-value / collection field) — emit a real JSON array.
      w.beginArray();
      for (Object element : iterable) {
        writeValue(w, element);
      }
      w.endArray();
    } else {
      w.value(value.toString());
    }
  }

  private void writeProfile(JsonWriter w, QueryProfile profile) throws IOException {
    // The profile is a small metadata object; serialize it via Gson in the formatter's own style.
    w.jsonValue(style == PRETTY ? prettyJsonify(profile) : compactJsonify(profile));
  }
}
