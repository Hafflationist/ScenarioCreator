package scenarioCreator.data.tgds;

import scenarioCreator.data.identification.Id;

public record RelationConstraintConcatenation(Id columnId1, Id columnId2) implements RelationConstraint {
}
