package scenarioCreator.generation.processing.transformations.constraintBased;

import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.FunctionalDependency;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

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
        final var allIdSet = columnList.stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .collect(Collectors.toCollection(TreeSet::new));
        if (allIdSet.size() < 2) {
            return SSet.of();
        }
        return Stream
                .generate(() -> generateSingleFd(allIdSet, random))
                .limit(allIdSet.size() / 2)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static FunctionalDependency generateSingleFd(SortedSet<Id> allIdSet, Random random) {
        final var count = allIdSet.size();
        final var chosenSizeLeft = 1; //(random.nextInt(Math.min(2, count / 2)) + 1);
        final var chosenSizeRight = (random.nextInt(Math.min(3, count / 2)) + 1);
        final var rte = new RuntimeException("FEHLER");
        final var left = StreamExtensions
                .pickRandomOrThrowMultiple(allIdSet.stream(), chosenSizeLeft, rte, random)
                .collect(Collectors.toCollection(TreeSet::new));
        final var right = StreamExtensions
                .pickRandomOrThrowMultiple(allIdSet.stream().filter(id -> !left.contains(id)), chosenSizeRight, rte, random)
                .collect(Collectors.toCollection(TreeSet::new));
        return new FunctionalDependency(left, right);
    }
}