/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.utils.MixedTypeComparator;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MAX(value1, value2, ...) returns the maximum value from the arguments. For mixed types, strings
 * have higher precedence than numbers.
 */
public class ScalarMaxFunction extends ImplementorUDF {

  public ScalarMaxFunction() {
    super(new MaxImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // Declare the least-restrictive common type of the operands rather than ANY. ANY maps to
    // ExprCoreType.UNDEFINED in the response schema and has no Substrait type, so on the
    // analytics route it surfaced as type=undefined and mixed numeric+string operands failed to
    // bind ("Cannot infer return type for GREATEST"). leastRestrictive yields the widened numeric
    // type for all-numeric args and VARCHAR when a string is present — matching MixedTypeComparator,
    // which ranks strings above numbers for MAX. Falls back to VARCHAR when no common type exists.
    // Mirrors EnhancedCoalesceFunction.
    return opBinding -> {
      var operandTypes = opBinding.collectOperandTypes();
      var commonType = opBinding.getTypeFactory().leastRestrictive(operandTypes);
      return commonType != null
          ? commonType
          : opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class MaxImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          MaxImplementor.class, "max", Expressions.newArrayInit(Object.class, translatedOperands));
    }

    public static Object max(Object[] args) {
      return findMax(args);
    }

    private static Object findMax(Object[] args) {
      if (args == null) {
        return null;
      }

      return Arrays.stream(args)
          .filter(Objects::nonNull)
          .max(MixedTypeComparator.INSTANCE)
          .orElse(null);
    }
  }
}
