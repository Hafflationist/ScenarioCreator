package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public record ColumnNode(int id,
                         StringPlus name,
                         List<Column> columnList,
                         Set<ColumnConstraint> constraintSet,
                         boolean isNullable) implements Column {

    @Contract(pure = true)
    @NotNull
    public ColumnNode withId(int newId) {
        return new ColumnNode(newId, name, columnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withName(StringPlus newName) {
        return new ColumnNode(id, newName, columnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withColumnList(List<Column> newColumnList) {
        return new ColumnNode(id, name, newColumnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withConstraintSet(Set<ColumnConstraint> newConstraintSet) {
        return new ColumnNode(id, name, columnList, newConstraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withIsNullable(boolean newIsNullable) {
        return new ColumnNode(id, name, columnList, constraintSet, newIsNullable);
    }
}