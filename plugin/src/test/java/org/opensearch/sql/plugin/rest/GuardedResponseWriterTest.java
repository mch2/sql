/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.Test;
import org.opensearch.sql.monitor.ResourceMonitor;

public class GuardedResponseWriterTest {

  private static final class FixedMonitor extends ResourceMonitor {
    private final boolean healthy;

    FixedMonitor(boolean healthy) {
      this.healthy = healthy;
    }

    @Override
    protected boolean isHealthyImpl() {
      return healthy;
    }
  }

  /** Hard cap: writing past maxChars throws with the "too large" message. */
  @Test(expected = IllegalStateException.class)
  public void tripsHardCap() throws IOException {
    GuardedResponseWriter w =
        new GuardedResponseWriter(new StringWriter(), new FixedMonitor(true), 10L, Long.MAX_VALUE);
    w.write("0123456789abcdef");
  }

  /** Interval poll: past the interval, an unhealthy monitor throws with memory-limit message. */
  @Test
  public void tripsWhenMonitorUnhealthyPastInterval() {
    GuardedResponseWriter w =
        new GuardedResponseWriter(new StringWriter(), new FixedMonitor(false), Long.MAX_VALUE, 1L);
    try {
      w.write("hello");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
      assertTrue(e.getMessage().contains("Insufficient memory"));
      assertTrue(e.getMessage().contains("plugins.query.memory_limit"));
      return;
    }
    throw new AssertionError("Expected IllegalStateException");
  }

  /** Healthy monitor at a tiny interval is polled repeatedly and the write completes. */
  @Test
  public void completesWhenHealthyAtTinyInterval() throws IOException {
    StringWriter sw = new StringWriter();
    GuardedResponseWriter w =
        new GuardedResponseWriter(sw, new FixedMonitor(true), Long.MAX_VALUE, 1L);
    for (int i = 0; i < 20; i++) {
      w.write("row");
    }
    assertEquals(60, sw.toString().length());
  }

  /** Char counting matches what was written and passes through to the delegate verbatim. */
  @Test
  public void countsAndPassesThroughExactly() throws IOException {
    StringWriter sw = new StringWriter();
    GuardedResponseWriter w =
        new GuardedResponseWriter(sw, new FixedMonitor(true), Long.MAX_VALUE, Long.MAX_VALUE);
    w.write("abc");
    w.write("defgh".toCharArray(), 1, 3); // "efg"
    assertEquals("abcefg", sw.toString());
  }
}
