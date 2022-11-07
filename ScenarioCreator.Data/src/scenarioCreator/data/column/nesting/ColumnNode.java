package scenarioCreator.data.column.nesting;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;

import java.util.List;
import java.util.SortedSet;

public record ColumnNode(Id id,
                         StringPlus name,
                         List<Column> columnList,
                         SortedSet<ColumnConstraint> constraintSet,
                         boolean isNullable) implements Column {

    @Contract(pure = true)
    @NotNull
    public ColumnNode withId(Id newId) {
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
    public ColumnNode withConstraintSet(SortedSet<ColumnConstraint> newConstraintSet) {
        return new ColumnNode(id, name, columnList, newConstraintSet, isNullable);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withIsNullable(boolean newIsNullable) {
        return new ColumnNode(id, name, columnList, constraintSet, newIsNullable);
    }
}