package scenarioCreator.data.tgds;

import scenarioCreator.data.identification.Id;

//TODO(F) Wie binde ich das in die Chateau-Schnittstelle ein?
public record RelationConstraintNotConstant (Id columnId, String constantValue) implements RelationConstraint {
}
