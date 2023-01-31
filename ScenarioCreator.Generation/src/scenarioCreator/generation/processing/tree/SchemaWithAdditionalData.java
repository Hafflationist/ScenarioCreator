package scenarioCreator.generation.processing.tree;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.generation.heterogeneity.Distance;

import java.util.List;

public record SchemaWithAdditionalData(
        Schema schema,
        List<Distance> distanceList,
        List<String> executedTransformationList
) implements Comparable<SchemaWithAdditionalData> {
    @Override
    public int compareTo(@NotNull SchemaWithAdditionalData o) {
        return schema.compareTo(o.schema);
    }
}