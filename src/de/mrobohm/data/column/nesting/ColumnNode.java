package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.ColumnConstraint;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public record ColumnNode(int id,
                         String name,
                         List<Column> columnList,
                         Set<ColumnConstraint> constraintSet) implements Column {

    @Contract(pure = true)
    @NotNull
    public ColumnNode withId(int newId) {
        return new ColumnNode(newId, name, columnList, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withName(String newName) {
        return new ColumnNode(id, newName, columnList, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withColumnList(List<Column> newColumnList) {
        return new ColumnNode(id, name, newColumnList, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnNode withContext(Set<ColumnConstraint> newConstraintSet) {
        return new ColumnNode(id, name, columnList, newConstraintSet);
    }
}
