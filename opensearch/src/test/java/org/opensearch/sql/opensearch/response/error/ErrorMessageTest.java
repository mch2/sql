/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ErrorMessageTest {

  @Test
  public void testToString() {
    ErrorMessage errorMessage =
        new ErrorMessage(
            new IllegalStateException("illegal state"), SERVICE_UNAVAILABLE.getStatus());
    assertEquals(
        "{\n"
            + "  \"error\": {\n"
            + "    \"reason\": \"There was internal problem at backend\",\n"
            + "    \"details\": \"illegal state\",\n"
            + "    \"type\": \"IllegalStateException\"\n"
            + "  },\n"
            + "  \"status\": 503\n"
            + "}",
        errorMessage.toString());
  }

  @Test
  public void testBadRequestToString() {
    ErrorMessage errorMessage =
        new ErrorMessage(new IllegalStateException(), BAD_REQUEST.getStatus());
    assertEquals(
        "{\n"
            + "  \"error\": {\n"
            + "    \"reason\": \"Invalid Query\",\n"
            + "    \"details\": \"\",\n"
            + "    \"type\": \"IllegalStateException\"\n"
            + "  },\n"
            + "  \"status\": 400\n"
            + "}",
        errorMessage.toString());
  }

  @Test
  public void testToStringWithEmptyErrorMessage() {
    ErrorMessage errorMessage =
        new ErrorMessage(new IllegalStateException(), SERVICE_UNAVAILABLE.getStatus());
    assertEquals(
        "{\n"
            + "  \"error\": {\n"
            + "    \"reason\": \"There was internal problem at backend\",\n"
            + "    \"details\": \"\",\n"
            + "    \"type\": \"IllegalStateException\"\n"
            + "  },\n"
            + "  \"status\": 503\n"
            + "}",
        errorMessage.toString());
  }

  /**
   * "<type>:<v> in unsupported format, please use '<pattern>'" → type remaps to
   * ExpressionEvaluationException.
   */
  @Test
  public void testFormatHintRemapDate() {
    ErrorMessage errorMessage =
        new ErrorMessage(
            new IllegalArgumentException(
                "date:2025-13-02 in unsupported format, please use 'yyyy-MM-dd'"),
            BAD_REQUEST.getStatus());
    assertEquals("ExpressionEvaluationException", errorMessage.getType());
  }

  @Test
  public void testFormatHintRemapTime() {
    ErrorMessage errorMessage =
        new ErrorMessage(
            new IllegalArgumentException(
                "time:2020-08-26 in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'"),
            BAD_REQUEST.getStatus());
    assertEquals("ExpressionEvaluationException", errorMessage.getType());
  }

  @Test
  public void testFormatHintRemapTimestamp() {
    ErrorMessage errorMessage =
        new ErrorMessage(
            new IllegalArgumentException(
                "timestamp:09:07:42 in unsupported format, please use 'yyyy-MM-dd"
                    + " HH:mm:ss[.SSSSSSSSS]'"),
            BAD_REQUEST.getStatus());
    assertEquals("ExpressionEvaluationException", errorMessage.getType());
  }

  /**
   * Generic IAE message keeps the original type — only the format-hint signature triggers remap.
   */
  @Test
  public void testGenericIaeMessageNotRemapped() {
    ErrorMessage errorMessage =
        new ErrorMessage(
            new IllegalArgumentException("not a format hint"), BAD_REQUEST.getStatus());
    assertEquals("IllegalArgumentException", errorMessage.getType());
  }
}
