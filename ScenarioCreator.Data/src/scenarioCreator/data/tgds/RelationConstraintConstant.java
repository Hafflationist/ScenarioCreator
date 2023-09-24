package scenarioCreator.data.tgds;

import scenarioCreator.data.column.nesting.Column;

public record RelationConstraintConstant(Column column, String constantValue) implements RelationConstraint {
}
