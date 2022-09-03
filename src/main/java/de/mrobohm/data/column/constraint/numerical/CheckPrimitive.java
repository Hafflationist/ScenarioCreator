package de.mrobohm.data.column.constraint.numerical;

public record CheckPrimitive(ComparisonType comparisonType, double value) implements CheckExpression {
}