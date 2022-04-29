package de.mrobohm.data.column;

import de.mrobohm.data.DataType;

import java.util.Set;

public record Column(int id,
                     String name,
                     DataType dataType,
                     ColumnContext context,
                     Set<ColumnConstraint> constraintSet) {

    public Column withId(int newId) {
        return new Column(newId, name, dataType, context, constraintSet);
    }

    public Column withName(String newName) {
        return new Column(id, newName, dataType, context, constraintSet);
    }

    public Column withDataType(DataType newDataType) {
        return new Column(id, name, newDataType, context, constraintSet);
    }

    public Column withContext(ColumnContext newContext) {
        return new Column(id, name, dataType, newContext, constraintSet);
    }

    public Column withConstraintList(Set<ColumnConstraint> newConstraintSet) {
        return new Column(id, name, dataType, context, newConstraintSet);
    }
}
