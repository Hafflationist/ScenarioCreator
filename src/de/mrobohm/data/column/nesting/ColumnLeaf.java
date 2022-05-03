package de.mrobohm.data.column.nesting;

import de.mrobohm.data.DataType;
import de.mrobohm.data.column.ColumnConstraint;
import de.mrobohm.data.column.ColumnContext;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public record ColumnLeaf(int id,
                         String name,
                         DataType dataType,
                         ColumnContext context,
                         Set<ColumnConstraint> constraintSet) implements Column {

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withId(int newId) {
        return new ColumnLeaf(newId, name, dataType, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withName(String newName) {
        return new ColumnLeaf(id, newName, dataType, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withDataType(DataType newDataType) {
        return new ColumnLeaf(id, name, newDataType, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withContext(ColumnContext newContext) {
        return new ColumnLeaf(id, name, dataType, newContext, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withConstraintSet(Set<ColumnConstraint> newConstraintSet) {
        return new ColumnLeaf(id, name, dataType, context, newConstraintSet);
    }
}