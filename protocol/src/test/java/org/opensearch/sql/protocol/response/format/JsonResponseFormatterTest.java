/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JsonResponseFormatter}'s inherited {@code format(Throwable)} — the error path a
 * subclass gets for free unless it overrides. Exercised here via a minimal subclass, since the
 * production subclasses (Jdbc/Command/Visualization) all override it and SimpleJson no longer
 * extends this base.
 */
class JsonResponseFormatterTest {

  /** Minimal subclass that keeps the inherited format(Throwable) and a no-op success path. */
  private static class TestFormatter extends JsonResponseFormatter<String> {
    TestFormatter(Style style) {
      super(style);
    }

    @Override
    protected Object buildJsonObject(String response) {
      return response;
    }
  }

  @Test
  void formatThrowableCompact() {
    assertEquals(
        "{\"type\":\"RuntimeException\",\"reason\":\"boom\"}",
        new TestFormatter(COMPACT).format(new RuntimeException("boom")));
  }

  @Test
  void formatThrowablePretty() {
    assertEquals(
        "{\n  \"type\": \"RuntimeException\",\n  \"reason\": \"boom\"\n}",
        new TestFormatter(PRETTY).format(new RuntimeException("boom")));
  }
}
