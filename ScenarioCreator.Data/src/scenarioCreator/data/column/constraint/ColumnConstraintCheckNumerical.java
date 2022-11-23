package scenarioCreator.data.column.constraint;

import scenarioCreator.data.column.constraint.numerical.CheckExpression;

public record ColumnConstraintCheckNumerical(CheckExpression checkExpression) implements ColumnConstraint {
}