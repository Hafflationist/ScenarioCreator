package scenarioCreator.data.column.constraint;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;

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