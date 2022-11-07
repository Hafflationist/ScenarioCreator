package scenarioCreator.data.column.nesting;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;

import java.util.List;
import java.util.SortedSet;

public record ColumnCollection(Id id,
                               StringPlus name,
                               List<Column> columnList,
                               SortedSet<ColumnConstraint> constraintSet,
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
    public ColumnCollection withConstraintSet(SortedSet<ColumnConstraint> newConstraintSet) {
        return new ColumnCollection(id, name, columnList, newConstraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withIsNullable(boolean newIsNullable) {
        return new ColumnCollection(id, name, columnList, constraintSet, newIsNullable);
    }
}