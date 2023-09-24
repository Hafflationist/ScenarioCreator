package scenarioCreator.data.tgds;

import scenarioCreator.data.column.nesting.Column;

public record RelationConstraintEquality(Column column1, Column column2) implements RelationConstraint {
}
