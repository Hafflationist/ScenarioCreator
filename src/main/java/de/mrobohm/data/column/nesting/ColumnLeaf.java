package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import javax.swing.text.html.Option;
import java.util.Optional;
import java.util.Set;

public record ColumnLeaf(Id id,
                         StringPlus name,
                         DataType dataType,
                         Set<Value> valueSet,
                         ColumnContext context,
                         Set<ColumnConstraint> constraintSet) implements Column {

    public ColumnLeaf(Id id,
                      StringPlus name,
                      DataType dataType,
                      ColumnContext context,
                      Set<ColumnConstraint> constraintSet) {
        this(id, name, dataType, Set.of(), context, constraintSet);
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
    public ColumnLeaf withvalueSet(Set<Value> newValueSet) {
        return new ColumnLeaf(id, name, dataType, newValueSet, context, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withContext(ColumnContext newContext) {
        return new ColumnLeaf(id, name, dataType, valueSet, newContext, constraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnLeaf withConstraintSet(Set<ColumnConstraint> newConstraintSet) {
        return new ColumnLeaf(id, name, dataType, valueSet, context, newConstraintSet);
    }
}