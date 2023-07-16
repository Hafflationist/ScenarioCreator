package scenarioCreator.generation.processing.tree;

import scenarioCreator.data.Schema;
import scenarioCreator.data.tgds.TupleGeneratingDependency;

import java.util.List;

public record TgdChainElement(
    Schema predecessor,
    List<TupleGeneratingDependency> tgdList,
    Schema schema
) {
}
