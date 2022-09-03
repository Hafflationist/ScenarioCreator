package de.mrobohm.data.column.constraint.numerical;

import org.jetbrains.annotations.NotNull;

public interface CheckExpression extends Comparable<CheckExpression> {

    @Override
    default int compareTo(@NotNull CheckExpression otherExpression) {
        return this.toString().compareTo(otherExpression.toString());
    }
}
