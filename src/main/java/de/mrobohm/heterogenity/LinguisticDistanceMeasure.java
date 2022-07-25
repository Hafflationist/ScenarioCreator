package de.mrobohm.heterogenity;

import de.mrobohm.data.Entity;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;

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
        var distanceAbsolute = calculateDistanceToRootAbsolute(schema1, schema2, diff);
        var schema1Size = IdentificationNumberCalculator.getAllIds(schema1, true).count();
        var schema2Size = IdentificationNumberCalculator.getAllIds(schema2, true).count();
        return (2.0 * distanceAbsolute) / (double) (schema1Size + schema2Size);
    }

    public static double calculateDistanceToRootAbsolute(
            Schema schema1, Schema schema2, BiFunction<StringPlus, StringPlus, Double> diff
    ) {
        var entityList1 = reduce(schema1);
        var entityList2 = reduce(schema2);
        // TODO: Vllt sollte man sich hier nochmal genauer überlegen, wie man die semantische Unterschiedlichkeit zweier ganzen Mengen darstellt.
        // Vermutung: Die gepaarten Spalten werden größtenteils eh semantisch ähnlich sein.
        return entityList1.stream()
                .map(e1 -> new Pair<>(e1, entityList2.stream().filter(e2 -> e2.id().equals(e1.id())).findFirst()))
                .filter(pair -> pair.second().isPresent())
                .mapToDouble(pair -> diff.apply(pair.first().name(), pair.second().get().name()))
                .sum();
    }

    private static SortedSet<Entity> reduce(Schema schema) {
        var tableEntities = schema.tableSet().stream()
                .flatMap(t -> StreamExtensions
                        .prepend(t.columnList().stream()
                                .flatMap(LinguisticDistanceMeasure::reduceColumn), t));

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