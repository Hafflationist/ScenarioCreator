package scenarioCreator.data.tgds;

import scenarioCreator.data.column.nesting.Column;

public record RelationConstraintNotConstant(Column column, String constantValue) implements RelationConstraint {
}
