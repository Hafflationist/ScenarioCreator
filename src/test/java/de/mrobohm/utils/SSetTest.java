package de.mrobohm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
        var random = new Random();
        var set = random.ints()
                .distinct()
                .limit(size)
                .boxed()
                .collect(Collectors.toCollection(TreeSet::new));

        // --- Act
        var powerSet = SSet.powerSet(set);

        // --- Assert
        Assertions.assertEquals(0b1 << set.size(), powerSet.size());
        var validElements = powerSet.stream().allMatch(set::containsAll);
        Assertions.assertTrue(validElements);
    }
}