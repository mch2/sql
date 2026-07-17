/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Map;
import org.opensearch.sql.monitor.AlwaysHealthyMonitor;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * JSON response format with schema header and data rows. Writes JSON incrementally via {@link
 * JsonWriter} so that the full response is never materialized as a Gson object tree — peak heap is
 * bounded to ~one row rather than the entire result set.
 *
 * <p>As the buffer grows past each {@link #BYTES_PER_HEALTH_CHECK} boundary the {@link
 * #resourceMonitor} is polled (mirroring {@code ResourceMonitorPlan}'s in-loop check on the v2
 * PhysicalPlan path, which the analytics-engine route bypasses), and a hard per-response byte cap
 * bounds any single response. Either guard rejects with an {@link IllegalStateException} rather
 * than letting the buffer grow to an OOM.
 *
 * <p>Output format is unchanged from the original Gson-based serialization:
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
public class SimpleJsonResponseFormatter extends JsonResponseFormatter<QueryResult> {

  /**
   * Poll the heap monitor each time the response buffer grows past another multiple of this many
   * bytes. Byte-based (not row-based) so a query with few but very wide rows is checked just as
   * often as one with many narrow rows — the previous row-interval check let wide-row results grow
   * the buffer unbounded between polls.
   */
  private static final int BYTES_PER_HEALTH_CHECK = 4 * 1024 * 1024; // 4 MB

  /**
   * Hard ceiling on a single response's serialized size, as a fraction of max heap. Even with the
   * heap monitor, N concurrent queries can each pass their heap check and then collectively OOM
   * while all holding a full StringWriter buffer (the transport response is a single String, so the
   * whole thing is buffered). This per-query cap bounds any one response so the concurrent sum
   * stays survivable; a query that would exceed it is rejected with a clear error instead of
   * crashing.
   */
  private static final double MAX_RESULT_HEAP_FRACTION = 0.20;

  private final Style style;
  private final ResourceMonitor resourceMonitor;
  private final long maxResultBytes;
  private final long bytesPerHealthCheck;

  public SimpleJsonResponseFormatter(Style style) {
    this(style, new AlwaysHealthyMonitor());
  }

  /**
   * @param style pretty vs compact
   * @param resourceMonitor polled as the response buffer grows during serialization. On the
   *     analytics-engine route this is the real {@code OpenSearchResourceMonitor}; the v2 route
   *     passes {@link AlwaysHealthyMonitor} since ResourceMonitorPlan already polls.
   */
  public SimpleJsonResponseFormatter(Style style, ResourceMonitor resourceMonitor) {
    this(
        style,
        resourceMonitor,
        (long) (Runtime.getRuntime().maxMemory() * MAX_RESULT_HEAP_FRACTION),
        BYTES_PER_HEALTH_CHECK);
  }

  /**
   * Package-private constructor exposing the byte thresholds so tests can drive the hard-cap and
   * heap-poll branches with small, deterministic inputs (the production thresholds are MBs / a
   * fraction of heap, impractical to hit in a unit test).
   *
   * @param maxResultBytes hard per-response byte cap
   * @param bytesPerHealthCheck buffer-growth interval between heap-monitor polls
   */
  SimpleJsonResponseFormatter(
      Style style, ResourceMonitor resourceMonitor, long maxResultBytes, long bytesPerHealthCheck) {
    super(style);
    this.style = style;
    this.resourceMonitor = resourceMonitor;
    this.maxResultBytes = maxResultBytes;
    this.bytesPerHealthCheck = bytesPerHealthCheck;
  }

  /**
   * Overrides the base class's {@code buildJsonObject → Gson.toJson(whole)} path to write JSON
   * incrementally. The result is still a single {@code String} (the transport contract requires
   * it), but it is built row-by-row through a streaming {@link JsonWriter} — no intermediate {@code
   * Object[][]} copy, no Gson reflection tree, and a heap-health check every N rows so we can abort
   * mid-serialization before OOMing.
   */
  @Override
  public String format(QueryResult response) {
    ProfileMetric formatMetric = QueryProfiling.current().getOrCreateMetric(MetricName.FORMAT);
    long formatTime = System.nanoTime();

    try {
      String result = writeJson(response);
      formatMetric.set(System.nanoTime() - formatTime);
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize query response", e);
    }
  }

  /**
   * Still needed for the error-formatting path (base class's {@code format(Throwable)} delegates to
   * {@code jsonify(buildJsonObject(...))}). The success path now goes through {@link
   * #format(QueryResult)} directly and never calls this.
   */
  @Override
  protected Object buildJsonObject(QueryResult response) {
    // Unreachable on the success path (format(QueryResult) is overridden); kept to satisfy the
    // abstract contract for subclass compatibility / error formatting.
    throw new UnsupportedOperationException(
        "SimpleJsonResponseFormatter uses incremental format(); buildJsonObject should not be"
            + " called");
  }

  // ─── Internal streaming writer ─────────────────────────────────────────────────

  /**
   * Allocates the response buffer. A seam so tests can inject a writer that fails (e.g. throws from
   * {@code close()}) to exercise the {@link IOException} path in {@link #format}; production always
   * uses a plain in-memory {@link StringWriter}, which never throws.
   */
  StringWriter newResponseBuffer(QueryResult response) {
    return new StringWriter(Math.min(response.size() * 64, 1024 * 1024));
  }

  private String writeJson(QueryResult response) throws IOException {
    StringWriter sw = newResponseBuffer(response);
    try (JsonWriter w = new JsonWriter(sw)) {
      if (style == Style.PRETTY) {
        w.setIndent("  ");
      }
      w.beginObject();

      // schema
      writeSchema(w, response);

      // datarows — the hot loop with byte-based guards.
      w.name("datarows").beginArray();
      long nextHealthCheckAt = bytesPerHealthCheck;
      for (Object[] row : response) {
        writeRow(w, row);
        w.flush();
        long bufBytes = (long) sw.getBuffer().length(); // chars ≈ bytes for the JSON we emit
        // Hard per-query cap: reject before this single response can threaten the heap on its own.
        if (bufBytes > maxResultBytes) {
          throw new IllegalStateException(
              "Query result too large to serialize: exceeded "
                  + (maxResultBytes / (1024 * 1024))
                  + " MB. Reduce the result set (add a smaller head/limit or narrower fields).");
        }
        // Byte-interval heap check: as the buffer crosses each threshold, poll the monitor so a
        // node already under heap pressure (e.g. from concurrent queries) sheds this one via 429
        // instead of every query growing its buffer to OOM.
        if (bufBytes >= nextHealthCheckAt) {
          if (!resourceMonitor.isHealthy()) {
            throw new IllegalStateException(
                "Insufficient memory to serialize the query result (buffered "
                    + (bufBytes / (1024 * 1024))
                    + " MB). Adjust 'plugins.query.memory_limit' or reduce the result set.");
          }
          nextHealthCheckAt = bufBytes + bytesPerHealthCheck;
        }
      }
      w.endArray();

      // total / size
      w.name("total").value(response.size());
      w.name("size").value(response.size());

      // profile (if present)
      QueryProfile profile = QueryProfiling.current().finish();
      if (profile != null) {
        w.name("profile");
        writeProfile(w, profile);
      }

      w.endObject();
    }
    return sw.toString();
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
      if (value instanceof Double || value instanceof Float) {
        w.value(num.doubleValue());
      } else {
        w.value(num.longValue());
      }
    } else if (value instanceof Boolean b) {
      w.value(b);
    } else if (value instanceof Map<?, ?> map) {
      // Nested object (e.g. an object field / tuple value) — emit real nested JSON, matching Gson's
      // behavior, not a toString. Keys are rendered in map iteration order.
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
      // String or any scalar — write as a JSON string.
      w.value(value.toString());
    }
  }

  private void writeProfile(JsonWriter w, QueryProfile profile) throws IOException {
    // The profile is a small metadata object; serialize it via Gson (cheap, bounded).
    w.jsonValue(ErrorFormatter.prettyJsonify(profile));
  }
}
