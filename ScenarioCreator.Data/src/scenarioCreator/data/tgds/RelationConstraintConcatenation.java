package scenarioCreator.data.tgds;

import scenarioCreator.data.column.nesting.Column;

public record RelationConstraintConcatenation(Column column1, Column column2, Column column12) implements RelationConstraint {
}
