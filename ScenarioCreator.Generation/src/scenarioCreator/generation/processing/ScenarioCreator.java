package scenarioCreator.generation.processing;

import scenarioCreator.data.Schema;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.processing.tree.*;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ScenarioCreator {

    private final DistanceDefinition _validDefinition;

    private final Forester.Injection _foresterInjection;

    /**
     * Besides of managing most lines in this class calculate the limits for target nodes
     * A definition of "target node" can be found in "Similarity-driven Schema Transformation for Test Data Generation" (4/6)
     */
    public ScenarioCreator(
            DistanceDefinition validDefinition,
            Forester.Injection foresterInjection
    ) {
        _validDefinition = validDefinition;
        _foresterInjection = foresterInjection;
    }

    public Scenario create(Schema startSchema, int sizeOfScenario, int newChildren, Random random, boolean debug) {
        final var sif = new SchemaInForest(null, List.of(), startSchema, List.of(), List.of());
        final var indexStream = Stream
                .iterate(0, i -> i + 1)
                .limit(sizeOfScenario);
        final var tgd = new TreeGenerationDefinition(
                true, true, true, true
        );
        final var sarSet = StreamExtensions.<SortedSet<SchemaAsResult>, Integer>foldLeft(indexStream, SSet.of(),
                (existingSchemaSet, existingSchemas) -> {
                    System.out.println("idx=" + existingSchemas + " | schemaSet=" + existingSchemaSet.size() + "    | (rnd:" + random.nextInt(1000) + ")");
                    if (existingSchemas != existingSchemaSet.size()) {
                        System.out.println("REEE");
                    }
                    assert existingSchemas == existingSchemaSet.size();
                    final var targetDefinition = calcTargetDefinition(
                            existingSchemaSet, sizeOfScenario
                    );
                    final var forester = _foresterInjection.get(
                            _validDefinition,
                            targetDefinition
                    );
                    final var existingSchemaPureSet = existingSchemaSet.stream()
                            .map(SchemaAsResult::schema)
                            .collect(Collectors.toCollection(TreeSet::new));
                    return recursiveNewSar(
                            forester, sif, tgd, existingSchemaPureSet, existingSchemaSet, newChildren, random, debug
                    );
                });
        return new Scenario(sarSet, avgDistance(sarSet));
    }

    private SortedSet<SchemaAsResult> recursiveNewSar(
            IForester forester,
            SchemaInForest root,
            TreeGenerationDefinition tgd,
            SortedSet<Schema> existingSchemaPureSet,
            SortedSet<SchemaAsResult> existingSchemaSet,
            int newChildren,
            Random random,
            boolean debug
    ) {
        final var newSar = forester.createNext(
                root, tgd, existingSchemaPureSet, newChildren, random, debug
        );
        final var newSarSet = SSet.prepend(newSar, existingSchemaSet);
        if (newSarSet.size() == existingSchemaSet.size()) {
            return recursiveNewSar(forester, root, tgd, existingSchemaPureSet, existingSchemaSet, newChildren, random, debug);
        }
        return newSarSet;
    }

    private DistanceDefinition calcTargetDefinition(
            SortedSet<SchemaAsResult> existingSchemaSet, int sizeOfScenario
    ) {
        return new DistanceDefinition(
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::structural),
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::linguistic),
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::constraintBased),
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::contextual)
        );
    }

    private DistanceDefinition.Target calcTargetDefinitionSingle(
            SortedSet<SchemaAsResult> existingSchemaSet,
            int sizeOfScenario,
            Function<Distance, Double> reduceDistance
    ) {
        final var validMin = reduceDistance.apply(_validDefinition.min());
        final var validAvg = reduceDistance.apply(_validDefinition.avg());
        final var validMax = reduceDistance.apply(_validDefinition.max());
        if (validAvg.isNaN()) {
            return new DistanceDefinition.Target(validMin, validAvg, validMax);
        }
        final var alreadyReachedHeterogeneity = existingSchemaSet.stream()
                .map(SchemaAsResult::distanceList)
                .flatMap(Collection::stream)
                .mapToDouble(reduceDistance::apply)
                .sum();

        final var existingSchemas = existingSchemaSet.size();
        final var heterogeneityWeWantToReach = missingDistanceRelations(0, sizeOfScenario) * validAvg;
        final var missingRelationsNextRun = missingDistanceRelations(existingSchemas + 1, sizeOfScenario);
        final var missingHeterogeneity = heterogeneityWeWantToReach - alreadyReachedHeterogeneity;
        final var targetMin = Math.max(
                validMin,
                (missingHeterogeneity - (validMax * missingRelationsNextRun)) / existingSchemas
        );
        final var targetMax = Math.min(
                validMax,
                (missingHeterogeneity - (validMin * missingRelationsNextRun)) / existingSchemas
        );
        return new DistanceDefinition.Target(targetMin, Double.NaN, targetMax);
    }

    private int missingDistanceRelations(int existingSchemas, int sizeOfScenario) {
        final var missingSchemas = sizeOfScenario - existingSchemas;
        final var fullyMissingRelations = (missingSchemas * (missingSchemas - 1)) / 2;
        final var semiMissingRelations = missingSchemas * existingSchemas;
        return fullyMissingRelations + semiMissingRelations;
    }


    private Distance avgDistance(SortedSet<SchemaAsResult> sarSet) {
        final var distanceList = sarSet.stream()
                .map(SchemaAsResult::distanceList)
                .flatMap(Collection::stream)
                .toList();
        return Distance.avg(distanceList);
    }
}
