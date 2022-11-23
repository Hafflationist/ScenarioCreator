package scenarioCreator.data.column.constraint;

import scenarioCreator.data.column.constraint.regexy.RegularExpression;

public record ColumnConstraintCheckRegex(RegularExpression regularExpression) implements ColumnConstraint {
}