package scenarioCreator.data.column.constraint;

import scenarioCreator.data.identification.Id;

public sealed class ColumnConstraintUnique implements ColumnConstraint permits ColumnConstraintPrimaryKey {

    private final Id _uniqueGroupId;

    public ColumnConstraintUnique(Id uniqueGroupId){
        _uniqueGroupId = uniqueGroupId;
    }

    public ColumnConstraintUnique withUniqueGroupId(Id newUniqueGroupId) {
        return new ColumnConstraintUnique(newUniqueGroupId);
    }

    public Id getUniqueGroupId() {
        return _uniqueGroupId;
    }
}