package scenarioCreator.data.column.constraint;

import org.jetbrains.annotations.NotNull;

public sealed interface ColumnConstraint extends Comparable<ColumnConstraint>
        permits
        ColumnConstraintCheckNumerical,
        ColumnConstraintCheckRegex,
        ColumnConstraintForeignKey,
        ColumnConstraintForeignKeyInverse,
        ColumnConstraintUnique {

    @Override
    default int compareTo(@NotNull ColumnConstraint cc) {
        return this.toString().compareTo(cc.toString());
    }
}