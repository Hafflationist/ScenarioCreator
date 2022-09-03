package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.column.context.NumericalDistribution;
import de.mrobohm.utils.MMath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class CheckNumericalManagerTest {

    private static boolean isApproxSame(NumericalDistribution nd1, NumericalDistribution nd2) {
        if (nd1.stepSize() != nd2.stepSize()) {
            return false;
        }
        if (!nd1.stepToOccurrences().keySet().equals(nd2.stepToOccurrences().keySet())) {
            return false;
        }
        return nd1.stepToOccurrences().keySet().stream()
                .allMatch(step -> MMath.isApproxSame(
                        nd1.stepToOccurrences().get(step),
                        nd2.stepToOccurrences().get(step)
                ));
    }

    @Test
    void merge() {
        // --- Arrange
        var nd1 = new NumericalDistribution(1.0, Map.of(
                1, 0.2,
                2, 0.3,
                3, 0.4,
                4, 0.5
        ));
        var nd2 = new NumericalDistribution(0.5, Map.of(
                5, 0.8,
                6, 0.7,
                7, 0.6,
                8, 0.5
        ));

        // --- Act
        var newNd = CheckNumericalManager.merge(nd1, nd2);

        // --- Assert
        var expectedNd = new NumericalDistribution(0.5, Map.of(
                1, 0.2,
                2, 0.2,
                3, 0.3,
                4, 0.3,
                5, 1.2,
                6, 1.1,
                7, 1.1,
                8, 1.0
        ));
        Assertions.assertTrue(isApproxSame(expectedNd, newNd));
    }

    @Test
    void normalize() {
        // --- Arrange
        var nd = new NumericalDistribution(1.0, Map.of(
                1, 0.5,
                2, 0.3,
                3, 1.5,
                4, 0.8
        ));

        // --- Act
        var ndNormalized = CheckNumericalManager.normalize(nd);

        // -- Assert
        var expectedNdNormalized = new NumericalDistribution(1.0, Map.of(
                1, 0.5 / (0.5 + 0.3 + 1.5 + 0.8),
                2, 0.3 / (0.5 + 0.3 + 1.5 + 0.8),
                3, 1.5 / (0.5 + 0.3 + 1.5 + 0.8),
                4, 0.8 / (0.5 + 0.3 + 1.5 + 0.8)
        ));
        Assertions.assertTrue(isApproxSame(expectedNdNormalized, ndNormalized));
    }
}