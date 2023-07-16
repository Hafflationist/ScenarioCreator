package scenarioCreator.generation.processing.tree;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.heterogeneity.Distance;

import java.util.List;
import java.util.Optional;

public record SchemaInForest (
        Optional<SchemaInForest> predecessorOpt,
        List<TupleGeneratingDependency> tgdList,
        Schema schema,
        List<Distance> distanceList,
        List<String> executedTransformationList
) implements Comparable<SchemaInForest> {
    @Override
    public int compareTo(@NotNull SchemaInForest o) {
        return schema.compareTo(o.schema);
    }
}
