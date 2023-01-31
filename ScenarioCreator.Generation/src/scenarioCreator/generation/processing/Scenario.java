package scenarioCreator.generation.processing;

import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.processing.tree.SchemaAsResult;

import java.util.SortedSet;

public record Scenario(SortedSet<SchemaAsResult> sarList, Distance avgDistance) {

}