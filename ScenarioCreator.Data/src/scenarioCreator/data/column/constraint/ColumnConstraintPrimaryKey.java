package scenarioCreator.data.column.constraint;

import scenarioCreator.data.identification.Id;
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