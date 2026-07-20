/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import java.io.IOException;
import java.io.Writer;
import org.opensearch.sql.monitor.ResourceMonitor;

/**
 * A {@link Writer} decorator that bounds a buffered response as it is written, so the analytics
 * route (which has no v2 {@code ResourceMonitorPlan} polling the heap on its next()-loop) can abort
 * a response before it OOMs the node. Two guards:
 *
 * <ul>
 *   <li>a hard per-response cap — even with the heap monitor, N concurrent queries can each pass
 *       their individual heap check and then collectively OOM while all holding a full response
 *       buffer; this cap bounds any single response so the concurrent sum stays survivable;
 *   <li>a heap-monitor poll each time the written size crosses another interval, mirroring
 *       ResourceMonitorPlan's in-loop check on the v2 path.
 * </ul>
 *
 * <p>The cap is measured in <b>characters</b> (Java chars are UTF-16; the JSON we emit is
 * predominantly ASCII so chars ≈ bytes). Both guards throw {@link IllegalStateException} — the SAME
 * type v2's {@code ResourceMonitorPlan} throws on memory rejection, so both engines' rejections map
 * to the same HTTP status.
 *
 * <p>TODO: {@code ResourceMonitorPlan}'s {@code IllegalStateException} currently maps to HTTP 500,
 * not 429. Both should become a shared 429-mapped memory-rejection type together.
 *
 * <p>Cross-repo note: the analytics coordinator (OpenSearch repo) separately reserves result heap
 * on the REQUEST circuit breaker with an expansion factor intended to cover this response copy.
 * This writer is the last-hop guard on the buffered String; a future unification could charge that
 * same breaker here instead of polling the monitor.
 */
public class GuardedResponseWriter extends Writer {

  private final Writer delegate;
  private final ResourceMonitor resourceMonitor;
  private final long maxChars;
  private final long charsPerHealthCheck;

  private long count;
  private long nextHealthCheckAt;

  /**
   * @param delegate the underlying buffer (e.g. a StringWriter)
   * @param resourceMonitor polled as the buffer grows; on the analytics route the real
   *     OpenSearchResourceMonitor (heap usage vs. plugins.query.memory_limit)
   * @param maxChars hard per-response cap in characters
   * @param charsPerHealthCheck growth interval between heap-monitor polls, in characters
   */
  public GuardedResponseWriter(
      Writer delegate, ResourceMonitor resourceMonitor, long maxChars, long charsPerHealthCheck) {
    this.delegate = delegate;
    this.resourceMonitor = resourceMonitor;
    this.maxChars = maxChars;
    this.charsPerHealthCheck = charsPerHealthCheck;
    this.nextHealthCheckAt = charsPerHealthCheck;
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    delegate.write(cbuf, off, len);
    count += len;
    if (count > maxChars) {
      throw new IllegalStateException(
          "Query result too large to serialize: exceeded "
              + (maxChars / (1024 * 1024))
              + " MB (buffered "
              + (count / (1024 * 1024))
              + " MB). Reduce the result set (add a smaller head/limit or narrower fields).");
    }
    if (count >= nextHealthCheckAt) {
      if (!resourceMonitor.isHealthy()) {
        throw new IllegalStateException(
            "Insufficient memory to serialize the query result (buffered "
                + (count / (1024 * 1024))
                + " MB). Adjust 'plugins.query.memory_limit' or reduce the result set.");
      }
      nextHealthCheckAt = count + charsPerHealthCheck;
    }
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
