package scenarioCreator.generation.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.utils.SSet;

import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

class SSetTest {

    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
            // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
    })
    void powerSet(int size) {
        // --- Arrange
        final var random = new Random();
        final var set = random.ints()
                .distinct()
                .limit(size)
                .boxed()
                .collect(Collectors.toCollection(TreeSet::new));

        // --- Act
        final var powerSet = SSet.powerSet(set);

        // --- Assert
        Assertions.assertEquals(0b1 << set.size(), powerSet.size());
        final var validElements = powerSet.stream().allMatch(set::containsAll);
        Assertions.assertTrue(validElements);
    }

    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
            // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
    })
    void foldLeft(int size) {
        // --- Arrange
        final var random = new Random();
        final var set = random.ints()
                .distinct()
                .limit(size)
                .boxed()
                .collect(Collectors.toCollection(TreeSet::new));

        // --- Act
        final var sum = SSet.foldLeft(set, 0, Integer::sum);

        // --- Assert
        Assertions.assertEquals(set.stream().mapToInt(x -> x).sum(), sum);
    }
}