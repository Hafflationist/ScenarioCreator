package de.mrobohm.data.column.constraint;

import de.mrobohm.data.identification.Id;

public final class ColumnConstraintPrimaryKey extends ColumnConstraintUnique {

    public ColumnConstraintPrimaryKey(Id uniqueGroupId) {
        super(uniqueGroupId);
    }
}