package de.mrobohm.data.column.constraint;

import de.mrobohm.data.identification.Id;
import org.jetbrains.annotations.NotNull;

public final class ColumnConstraintPrimaryKey extends ColumnConstraintUnique {

    public ColumnConstraintPrimaryKey(Id uniqueGroupId) {
        super(uniqueGroupId);
    }

    @Override
    @NotNull
    public ColumnConstraintPrimaryKey withUniqueGroupId(Id newUniqueGroupId) {
        return new ColumnConstraintPrimaryKey(newUniqueGroupId);
    }
}