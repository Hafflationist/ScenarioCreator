package de.mrobohm.processing;

import de.mrobohm.data.Schema;
import de.mrobohm.heterogeneity.Distance;
import de.mrobohm.processing.transformations.SingleTransformationExecutor;
import de.mrobohm.processing.transformations.TransformationCollection;
import de.mrobohm.processing.tree.*;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ScenarioCreator {
    private final SingleTransformationExecutor _singleTransformationExecutor;

    private final TransformationCollection _transformationCollection;

    private final DistanceMeasures _measures;

    private final DistanceDefinition _validDefinition;

    private final Forester.Injection _foresterInjection;

    public ScenarioCreator(
            SingleTransformationExecutor singleTransformationExecutor,
            TransformationCollection transformationCollection,
            DistanceMeasures measures,
            DistanceDefinition validDefinition,
            Forester.Injection foresterInjection
    ) {
        _singleTransformationExecutor = singleTransformationExecutor;
        _transformationCollection = transformationCollection;
        _measures = measures;
        _validDefinition = validDefinition;
        _foresterInjection = foresterInjection;
    }

    public SortedSet<Schema> create(Schema startSchema, int sizeOfScenario, Random random) {
        final var swad = new SchemaWithAdditionalData(startSchema, List.of());
        final var indexStream = Stream
                .iterate(0, i -> i + 1)
                .limit(sizeOfScenario);
        final var tgd = new TreeGenerationDefinition(
                true, true, true, false
        );
        return StreamExtensions.<SortedSet<SchemaWithAdditionalData>, Integer>foldLeft(indexStream, SSet.of(),
                        (existingSchemaSet, existingSchemas) -> {
                            assert existingSchemas == existingSchemaSet.size();
                            final var targetDefinition = calcTargetDefinition(
                                    existingSchemaSet, sizeOfScenario
                            );
                            final var forester = _foresterInjection.get(
                                    _singleTransformationExecutor,
                                    _transformationCollection,
                                    _measures,
                                    _validDefinition,
                                    targetDefinition
                            );
                            final var existingSchemaPureSet = existingSchemaSet.stream()
                                    .map(SchemaWithAdditionalData::schema)
                                    .collect(Collectors.toCollection(TreeSet::new));
                            final var newSwad = forester.createNext(
                                    swad, tgd, existingSchemaPureSet, random
                            );
                            return SSet.prepend(newSwad, existingSchemaSet);
                        })
                .stream()
                .map(SchemaWithAdditionalData::schema)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private DistanceDefinition calcTargetDefinition(SortedSet<SchemaWithAdditionalData> existingSchemaSet, int sizeOfScenario) {
        return new DistanceDefinition(
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::structural),
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::linguistic),
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::constraintBased),
                calcTargetDefinitionSingle(existingSchemaSet, sizeOfScenario, Distance::contextual)
        );
    }

    private DistanceDefinition.Target calcTargetDefinitionSingle(
            SortedSet<SchemaWithAdditionalData> existingSchemaSet,
            int sizeOfScenario,
            Function<Distance, Double> reduceDistance
    ) {
        final var validMin = reduceDistance.apply(_validDefinition.min());
        final var validAvg = reduceDistance.apply(_validDefinition.avg());
        final var validMax = reduceDistance.apply(_validDefinition.max());
        final var alreadyReachedHeterogeneity = existingSchemaSet.stream()
                .map(SchemaWithAdditionalData::distanceList)
                .flatMap(Collection::stream)
                .mapToDouble(reduceDistance::apply)
                .sum();

        final var existingSchemas = existingSchemaSet.size();
        final var heterogeneityWeWantToReach = missingDistanceRelations(existingSchemas, sizeOfScenario) * validAvg;
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
}
