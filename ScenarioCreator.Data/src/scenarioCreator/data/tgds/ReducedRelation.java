package scenarioCreator.data.tgds;

import scenarioCreator.data.identification.Id;

import java.util.List;

public record ReducedRelation(Id relationId, List<Id> columnIdList) {
}
