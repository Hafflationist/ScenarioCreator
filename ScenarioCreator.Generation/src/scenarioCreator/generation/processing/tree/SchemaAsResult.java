package scenarioCreator.generation.processing.tree;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.processing.tree.TgdChainElement;

import java.util.List;

public record SchemaAsResult(
        Schema schema,
        List<TgdChainElement> tgdChain,
        List<Distance> distanceList,
        List<String> executedTransformationList,
        boolean wasTargetSchema,
        boolean isValidSchema
) implements Comparable<SchemaAsResult> {
    @Override
    public int compareTo(@NotNull SchemaAsResult o) {
        return schema.compareTo(o.schema);
    }
}
