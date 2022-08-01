package de.mrobohm.processing.transformations.constraintBased;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConstraintBasedTestingUtils {
    private ConstraintBasedTestingUtils() {
    }

    public static SortedSet<FunctionalDependency> generateFd(List<Column> columnList, Random random) {
        var allIdSet = columnList.stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .collect(Collectors.toCollection(TreeSet::new));
        if (allIdSet.size() < 2){
            return SSet.of();
        }
        return Stream
                .generate(() -> generateSingleFd(allIdSet, random))
                .limit(allIdSet.size())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static FunctionalDependency generateSingleFd(SortedSet<Id> allIdSet, Random random) {
        var count = allIdSet.size();
        var chosenSize = (random.nextInt(count / 2) + 1);
        var rte = new RuntimeException("FEHLER");
        var left = StreamExtensions
                .pickRandomOrThrowMultiple(allIdSet.stream(), chosenSize, rte, random)
                .collect(Collectors.toCollection(TreeSet::new));
        var right = StreamExtensions
                .pickRandomOrThrowMultiple(allIdSet.stream().filter(id -> !left.contains(id)), chosenSize, rte, random)
                .collect(Collectors.toCollection(TreeSet::new));
        return new FunctionalDependency(left, right);
    }
}