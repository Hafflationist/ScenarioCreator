package scenarioCreator.data.tgds;

import java.util.List;

public record TupleGeneratingDependency(
        List<ReducedRelation> forallRows,
        List<ReducedRelation> existRows,
        List<RelationConstraint> constraints
) {
}