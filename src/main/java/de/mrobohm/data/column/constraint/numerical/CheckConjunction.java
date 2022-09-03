package de.mrobohm.data.column.constraint.numerical;

import java.util.SortedSet;

public record CheckConjunction(SortedSet<CheckExpression> conjunctiveCheckExpressionSet) implements CheckExpression {
}