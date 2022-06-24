package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public record ColumnCollection(Id id,
                               StringPlus name,
                               List<Column> columnList,
                               Set<ColumnConstraint> constraintSet,
                               boolean isNullable) implements Column {

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withId(Id newId) {
        return new ColumnCollection(newId, name, columnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withName(StringPlus newName) {
        return new ColumnCollection(id, newName, columnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withColumnList(List<Column> newColumnList) {
        return new ColumnCollection(id, name, newColumnList, constraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withConstraintSet(Set<ColumnConstraint> newConstraintSet) {
        return new ColumnCollection(id, name, columnList, newConstraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withIsNullable(boolean newIsNullable) {
        return new ColumnCollection(id, name, columnList, constraintSet, newIsNullable);
    }
}