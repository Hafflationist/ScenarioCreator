package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.ColumnConstraint;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public record ColumnCollection(List<Column> columnList,
                               Set<ColumnConstraint> constraintSet) implements Column {

    @Override
    public int id() {
        return -1;
    }

    @Override
    public String name() {
        return "[<Collection>]";
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withColumnList(List<Column> newColumnList) {
        return new ColumnCollection(newColumnList, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnCollection withContext(Set<ColumnConstraint> newConstraintSet) {
        return new ColumnCollection(columnList, newConstraintSet);
    }
}