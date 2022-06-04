package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public record ColumnCollection(StringPlus name,
                               List<Column> columnList,
                               Set<ColumnConstraint> constraintSet,
                               boolean isNullable) implements Column {

    @Override
    public int id() {
        return -1;
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withName(StringPlus newName) {
        return new ColumnCollection(newName, columnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withColumnList(List<Column> newColumnList) {
        return new ColumnCollection(name, newColumnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withConstraintSet(Set<ColumnConstraint> newConstraintSet) {
        return new ColumnCollection(name, columnList, newConstraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withIsNullable(boolean newIsNullable) {
        return new ColumnCollection(name, columnList, constraintSet, newIsNullable);
    }
}