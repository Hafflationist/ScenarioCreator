package scenarioCreator.data.tgds;

public sealed interface RelationConstraint permits RelationConstraintConcatenation, RelationConstraintEquality, RelationConstraintConstant {
}