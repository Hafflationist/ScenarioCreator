package de.mrobohm.heterogenity;

import de.mrobohm.data.Schema;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConstraintBasedDistanceMeasure {
    private ConstraintBasedDistanceMeasure() {
    }

    public static double calculateDistanceToRootRelative(
            Schema schema1, Schema schema2, BiFunction<StringPlus, StringPlus, Double> diff
    ) {
        var distanceAbsolute = calculateDistanceToRootAbsolute(schema1, schema2, diff);
        var schema1Size = IdentificationNumberCalculator.getAllIds(schema1, false).count();
        var schema2Size = IdentificationNumberCalculator.getAllIds(schema2, false).count();
        return (2.0 * distanceAbsolute) / (double) (schema1Size + schema2Size);
    }

    public static double calculateDistanceToRootAbsolute(
            Schema schema1, Schema schema2, BiFunction<StringPlus, StringPlus, Double> diff
    ) {
        var weightedDistList = findCorrespondingTablePairs(schema1, schema2)
                .map(pair -> new Pair<>(
                        weight(pair.first(), pair.second()),
                        diffOfTables(pair.first(), pair.second())
                ))
                .toList();
        var weightSum = weightedDistList.stream().mapToDouble(Pair::first).sum();
        return weightedDistList.stream()
                .mapToDouble(pair -> pair.first() * pair.second() / weightSum)
                .sum();
    }

    private static Stream<Pair<Table, Table>> findCorrespondingTablePairs(Schema schema1, Schema schema2) {
        return schema1.tableSet().stream()
                .filter(t1 -> IdentificationNumberCalculator.extractIdSimple(t1.id()).count() > 2)
                .flatMap(t1 -> schema2.tableSet().stream()
                        .filter(t2 -> {
                            var idList2 = IdentificationNumberCalculator
                                    .extractIdSimple(t2.id())
                                    .collect(Collectors.toSet());
                            if (idList2.size() > 2) return false;
                            return IdentificationNumberCalculator.extractIdSimple(t1.id())
                                    .anyMatch(idList2::contains);

                        })
                        .map(t2 -> new Pair<>(t1, t2)));
    }


    private static SortedSet<Id> translateIdSet(SortedSet<Id> idSet) {
        return idSet.stream()
                .flatMap(IdentificationNumberCalculator::extractIdSimple)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static SortedSet<FunctionalDependency> translatedTableFd(Table table) {
        return FunctionalDependencyManager.minimalCover(
                table.functionalDependencySet().stream()
                        .map(fd -> {
                            var newLeft = translateIdSet(fd.left());
                            var newRight = translateIdSet(fd.right());
                            return new FunctionalDependency(newLeft, newRight);
                        })
                        .collect(Collectors.toCollection(TreeSet::new))
        );
    }

    private static double diffOfTables(Table table1, Table table2) {
        var fdSet1 = translatedTableFd(table1);
        var fdSet2 = translatedTableFd(table2);

        var memberShipPartition1 = StreamExtensions.partition(
                fdSet1.stream(),
                fd1 -> FunctionalDependencyManager.membership(fd1, fdSet2)
        );
        var memberShipPartition2 = StreamExtensions.partition(
                fdSet2.stream(),
                fd2 -> FunctionalDependencyManager.membership(fd2, fdSet1)
        );

        var nonIntersecting = Stream
                .concat(memberShipPartition1.no(), memberShipPartition2.no())
                .count();
        var intersecting = Stream
                .concat(memberShipPartition1.yes(), memberShipPartition2.yes())
                .count();
        var union = nonIntersecting + intersecting;
        // jaccard / IOU -> inverse
        // I chose linear inversion because intersection could be 0
        return 1 - (intersecting / (double) union);
    }

    private static double weight(Table table1, Table table2) {
        var table1Size = IdentificationNumberCalculator
                .tableToIdStream(table1, false)
                .count();
        var table2Size = IdentificationNumberCalculator
                .tableToIdStream(table2, false)
                .count();
        return table1Size + (double) table2Size;
    }
}