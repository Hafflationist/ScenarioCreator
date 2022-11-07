package scenarioCreator.data.column.constraint.numerical;

import org.jetbrains.annotations.NotNull;

public sealed interface CheckExpression extends Comparable<CheckExpression>
        permits
        CheckPrimitive,
        CheckConjunction,
        CheckDisjunction {

    @Override
    default int compareTo(@NotNull CheckExpression otherExpression) {
        return this.toString().compareTo(otherExpression.toString());
    }
}