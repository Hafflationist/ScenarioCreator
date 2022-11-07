package scenarioCreator.data.column.constraint.numerical;

import java.util.SortedSet;

public record CheckDisjunction(SortedSet<CheckExpression> disjunctiveCheckExpressionSet) implements CheckExpression {
}