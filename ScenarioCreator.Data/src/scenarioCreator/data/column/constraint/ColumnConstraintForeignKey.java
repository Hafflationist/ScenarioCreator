package scenarioCreator.data.column.constraint;

import scenarioCreator.data.identification.Id;

public record ColumnConstraintForeignKey(Id foreignColumnId) implements ColumnConstraint {
}