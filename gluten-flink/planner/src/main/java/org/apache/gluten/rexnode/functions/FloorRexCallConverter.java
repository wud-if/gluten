package org.apache.gluten.rexnode.functions;

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

/**
 * Converts FLOOR(dateTime TO SECOND/MINUTE/HOUR/DAY) to velox date_trunc(unit, dateTime).
 * Enables computing window_end directly in SQL: FLOOR(dateTime TO SECOND) + INTERVAL '1' SECOND.
 */
class FloorRexCallConverter extends BaseRexCallConverter {

  FloorRexCallConverter() {
    super("date_trunc");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getKind() != SqlKind.FLOOR) {
      return ValidationResult.failure("Not FLOOR");
    }
    if (callNode.getOperands().size() != 2) {
      return ValidationResult.failure("FLOOR TO unit needs 2 operands");
    }
    if (!(callNode.getOperands().get(1) instanceof RexLiteral)) {
      return ValidationResult.failure("Second operand must be a TimeUnit literal");
    }
    RexLiteral unitLiteral = (RexLiteral) callNode.getOperands().get(1);
    Object value = unitLiteral.getValue();
    if (value == null) {
      return ValidationResult.failure("Unit literal has no value");
    }
    String unitStr = value.toString();
    if (!unitStr.equals("SECOND") && !unitStr.equals("MINUTE")
        && !unitStr.equals("HOUR") && !unitStr.equals("DAY")) {
      return ValidationResult.failure("Unsupported time unit: " + unitStr);
    }
    Type firstType = RexNodeConverter.toType(callNode.getOperands().get(0).getType());
    if (!(firstType instanceof TimestampType)) {
      return ValidationResult.failure("FLOOR requires TIMESTAMP operand");
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    TypedExpr dateTimeExpr =
        RexNodeConverter.toTypedExpr(callNode.getOperands().get(0), context);

    RexLiteral unitLiteral = (RexLiteral) callNode.getOperands().get(1);
    String unitStr = unitLiteral.getValue().toString().toLowerCase();

    // Same JSON as ConstantTypedExpr.create(Variant) but skips the
    // variantInferType JNI call: the type of a VarCharValue is always VARCHAR.
    ConstantTypedExpr unitExpr =
        new ConstantTypedExpr(new VarCharType(), new VarCharValue(unitStr), null);

    return new CallTypedExpr(
        new TimestampType(),
        List.of(unitExpr, dateTimeExpr),
        functionName);
  }
}
