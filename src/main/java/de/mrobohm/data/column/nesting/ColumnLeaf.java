package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.utils.SSet;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.SortedSet;

public record ColumnLeaf(Id id,
                         StringPlus name,
                         DataType dataType,
                         SortedSet<Value> valueSet,
                         ColumnContext context,
                         SortedSet<ColumnConstraint> constraintSet) implements Column {

    public ColumnLeaf(Id id,
                      StringPlus name,
                      DataType dataType,
                      ColumnContext context,
                      SortedSet<ColumnConstraint> constraintSet) {
        this(id, name, dataType, SSet.of(), context, constraintSet);
    }

    @Override
    public boolean isNullable() {
        return dataType().isNullable();
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withId(Id newId) {
        return new ColumnLeaf(newId, name, dataType, valueSet, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withName(StringPlus newName) {
        return new ColumnLeaf(id, newName, dataType, valueSet, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withDataType(DataType newDataType) {
        return new ColumnLeaf(id, name, newDataType, valueSet, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withValueSet(SortedSet<Value> newValueSet) {
        return new ColumnLeaf(id, name, dataType, newValueSet, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withContext(ColumnContext newContext) {
        return new ColumnLeaf(id, name, dataType, valueSet, newContext, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withConstraintSet(SortedSet<ColumnConstraint> newConstraintSet) {
        return new ColumnLeaf(id, name, dataType, valueSet, context, newConstraintSet);
    }
}