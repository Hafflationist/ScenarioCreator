package de.mrobohm.heterogenity.constraintBased;

import de.mrobohm.data.Schema;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.utils.MMath;
import de.mrobohm.utils.StreamExtensions;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FunctionalDependencyBasedDistanceMeasure {
    private FunctionalDependencyBasedDistanceMeasure() {
    }

    public static double calculateDistanceRelative(
            Schema schema1, Schema schema2
    ) {
        final var distanceAbsolute = calculateDistanceAbsolute(schema1, schema2);
        final var schema1Size = IdentificationNumberCalculator.getAllIds(schema1, false).count();
        final var schema2Size = IdentificationNumberCalculator.getAllIds(schema2, false).count();
        return (2.0 * distanceAbsolute) / (double) (schema1Size + schema2Size);
    }

    public static double calculateDistanceAbsolute(
            Schema schema1, Schema schema2
    ) {
        final var weightedDistStream = BasedConstraintBasedBase
                .findCorrespondingEntityPairs(schema1.tableSet().stream(), schema2.tableSet().stream())
                .map(pair -> new MMath.WeightedNumber(
                        weight(pair.first(), pair.second()),
                        diffOfTables(pair.first(), pair.second())
                ));
        return MMath.avgWeighted(weightedDistStream);
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
                            final var newLeft = translateIdSet(fd.left());
                            final var newRight = translateIdSet(fd.right());
                            return new FunctionalDependency(newLeft, newRight);
                        })
                        .collect(Collectors.toCollection(TreeSet::new))
        );
    }

    private static double diffOfTables(Table table1, Table table2) {
        final var fdSet1 = translatedTableFd(table1);
        final var fdSet2 = translatedTableFd(table2);

        final var memberShipPartition1 = StreamExtensions.partition(
                fdSet1.stream(),
                fd1 -> FunctionalDependencyManager.membership(fd1, fdSet2)
        );
        final var memberShipPartition2 = StreamExtensions.partition(
                fdSet2.stream(),
                fd2 -> FunctionalDependencyManager.membership(fd2, fdSet1)
        );

        final var nonIntersecting = Stream
                .concat(memberShipPartition1.no(), memberShipPartition2.no())
                .count();
        final var intersecting = Stream
                .concat(memberShipPartition1.yes(), memberShipPartition2.yes())
                .count();
        final var union = nonIntersecting + intersecting;
        // jaccard / IOU -> inverse
        // I chose linear inversion because intersection could be 0
        if (union == 0) return 0.0;
        return 1 - (intersecting / (double) union);
    }

    private static double weight(Table table1, Table table2) {
        final var table1Size = IdentificationNumberCalculator
                .tableToIdStream(table1, false)
                .count();
        final var table2Size = IdentificationNumberCalculator
                .tableToIdStream(table2, false)
                .count();
        return table1Size + (double) table2Size;
    }
}