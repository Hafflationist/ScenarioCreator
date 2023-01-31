package scenarioCreator.generation.heterogeneity.linguistic;

import scenarioCreator.data.Entity;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LinguisticDistanceMeasure {
    private LinguisticDistanceMeasure() {
    }

    public static double calculateDistanceToRootRelative(
            Schema schema1, Schema schema2, BiFunction<StringPlus, StringPlus, Double> diff
    ) {
        final var distanceAbsolute = calculateDistanceToRootAbsolute(schema1, schema2, diff);
        final var schema1Size = IdentificationNumberCalculator.getAllIds(schema1, true).count();
        final var schema2Size = IdentificationNumberCalculator.getAllIds(schema2, true).count();
        return (2.0 * distanceAbsolute) / (double) (schema1Size + schema2Size);
    }

    public static double calculateDistanceToRootAbsolute(
            Schema schema1, Schema schema2, BiFunction<StringPlus, StringPlus, Double> diff
    ) {
        final var entitySet1 = reduce(schema1);
        final var entitySet2 = reduce(schema2);
        final var intersectingIdSet = entitySet1.stream()
                .map(Entity::id)
                .filter(id -> entitySet2.stream().anyMatch(e -> e.id().equals(id)))
                .collect(Collectors.toCollection(TreeSet::new));
        final var easyMappingDist = entitySet1.stream()
                .filter(e1 -> intersectingIdSet.contains(e1.id()))
                .map(e1 -> new Pair<>(e1, entitySet2.stream().filter(e2 -> e2.id().equals(e1.id())).findFirst()))
                .filter(pair -> pair.second().isPresent()) // eigentlich unnötig, nur für den Kompilierer
                .mapToDouble(pair -> diff.apply(pair.first().name(), pair.second().get().name()))
                .sum();

        final var mapping1Stream = EntityHandler
                .getNameMapping(entitySet1, entitySet2, intersectingIdSet);
        final var mapping2Stream = EntityHandler
                .getNameMapping(entitySet2, entitySet1, intersectingIdSet);
        final var difficultMappingDist = EntityHandler.mappingToDistance(mapping1Stream, diff)
                + EntityHandler.mappingToDistance(mapping2Stream, diff);

        return easyMappingDist + difficultMappingDist;
    }

    private static SortedSet<Entity> reduce(Schema schema) {
        final var tableEntities = schema.tableSet().stream()
                .flatMap(t -> StreamExtensions
                        .prepend(
                                t.columnList().stream()
                                        .flatMap(LinguisticDistanceMeasure::reduceColumn),
                                t
                        ));

        return StreamExtensions.prepend(tableEntities, schema).collect(Collectors.toCollection(TreeSet::new));
    }

    private static Stream<Entity> reduceColumn(Column column) {
        return switch (column) {
            case ColumnLeaf ignore -> Stream.of(column);
            case ColumnNode node -> StreamExtensions.prepend(
                    node.columnList().stream().flatMap(LinguisticDistanceMeasure::reduceColumn),
                    column
            );
            case ColumnCollection col -> StreamExtensions.prepend(
                    col.columnList().stream().flatMap(LinguisticDistanceMeasure::reduceColumn),
                    column
            );
        };
    }
}