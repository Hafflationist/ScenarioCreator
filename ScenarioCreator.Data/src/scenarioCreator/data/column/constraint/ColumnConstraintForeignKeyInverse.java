package scenarioCreator.data.column.constraint;

import scenarioCreator.data.identification.Id;

public record ColumnConstraintForeignKeyInverse(Id foreignColumnId) implements ColumnConstraint {
}