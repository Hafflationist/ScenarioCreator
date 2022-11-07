package processing.transformations.constraintBased.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.generation.processing.transformations.constraintBased.base.StepIntervall;
import scenarioCreator.utils.MMath;
import scenarioCreator.utils.SSet;

import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

class StepIntervallTest {

    @Test
    void fromNumericalDistribution() {
        // --- Arrange
        final var nd = new NumericalDistribution(1.0, Map.of(
                -3, 0.1,
                -1, 0.1,
                1, 0.1,
                2, 0.2,
                4, 0.8,
                5, 0.5
        ));

        // --- Act
        final var siSet = StepIntervall
                .fromNumericalDistribution(nd)
                .collect(Collectors.toCollection(TreeSet::new));

        // --- Assert
        final var expectedSiSet = SSet.of(
                new StepIntervall(-3, -3.0, -2.0),
                new StepIntervall(-1, -1.0, 0.0),
                new StepIntervall(1, 0.0, 1.0),
                new StepIntervall(2, 1.0, 2.0),
                new StepIntervall(4, 3.0, 4.0),
                new StepIntervall(5, 4.0, 5.0)
        );
        Assertions.assertEquals(expectedSiSet.size(), siSet.size());
        final var same = expectedSiSet.stream().allMatch(esi -> siSet.stream().anyMatch(si -> isApproxSame(si, esi)));
        Assertions.assertTrue(same);
    }

    @Test
    void fromNumericalDistributionWithStepSize() {
        // --- Arrange
        final var nd = new NumericalDistribution(2.0, Map.of(
                -1, 0.1,
                1, 0.1,
                2, 0.2
        ));

        // --- Act
        final var siSet = StepIntervall
                .fromNumericalDistribution(nd, 0.75)
                .collect(Collectors.toCollection(TreeSet::new));

        // --- Assert
        final var expectedSiSet = SSet.of(
                new StepIntervall(-3, -2.25, -1.5),
                new StepIntervall(-2, -1.5, -0.75),
                new StepIntervall(-1, -0.75, 0.0),
                new StepIntervall(1, 0.0, 0.75),
                new StepIntervall(2, 0.75, 1.5),
                new StepIntervall(3, 1.5, 2.25),
                new StepIntervall(4, 2.25, 3.0),
                new StepIntervall(5, 3.0, 3.75),
                new StepIntervall(6, 3.75, 4.5)
        );
        Assertions.assertEquals(expectedSiSet.size(), siSet.size());
        final var same = expectedSiSet.stream().allMatch(esi -> siSet.stream().anyMatch(si -> isApproxSame(si, esi)));
        Assertions.assertTrue(same);
    }

    @ParameterizedTest
    @ValueSource(doubles = {5.0, 6.0})
    void fillHoles(double to) {
        // --- Arrange
        final var siSet = SSet.of(
                new StepIntervall(-3, -3.0, -2.0),
                new StepIntervall(-1, -1.0, 0.0),
                new StepIntervall(1, 0.0, 1.0),
                new StepIntervall(2, 1.0, 2.0),
                new StepIntervall(4, 3.0, 4.0),
                new StepIntervall(5, 4.0, 5.0)
        );

        // --- Act
        final var siSetWithoutHoles = StepIntervall.fillHoles(siSet, -5.0, to);

        // -- Assert
        final var expectedSiSet = SSet.concat(SSet.of(
                        new StepIntervall(Integer.MAX_VALUE, -5.0, -3.0),
                        new StepIntervall(-3, -3.0, -2.0),
                        new StepIntervall(Integer.MAX_VALUE, -2.0, -1.0),
                        new StepIntervall(-1, -1.0, 0.0),
                        new StepIntervall(1, 0.0, 1.0),
                        new StepIntervall(2, 1.0, 2.0),
                        new StepIntervall(Integer.MAX_VALUE, 2.0, 3.0),
                        new StepIntervall(4, 3.0, 4.0),
                        new StepIntervall(5, 4.0, 5.0)
                ),
                to == 5.0 ? SSet.of() : SSet.of(new StepIntervall(Integer.MAX_VALUE, 5.0, 6.0))
        );
        Assertions.assertEquals(expectedSiSet.size(), siSetWithoutHoles.size());
        final var same = expectedSiSet.stream().allMatch(esi -> siSetWithoutHoles.stream().anyMatch(si -> isApproxSame(si, esi)));
        Assertions.assertTrue(same);
    }

    @Test
    void intersecting() {
        // --- Arrange
        final var si1a = new StepIntervall(1, 1.0, 2.0);
        final var si1b = new StepIntervall(2, 0.0, 1.5);
        final var si2a = new StepIntervall(1, 1.0, 2.0);
        final var si2b = new StepIntervall(2, 1.3, 1.5);
        final var si3a = new StepIntervall(1, 1.0, 2.0);
        final var si3b = new StepIntervall(2, 0.0, 0.5);

        // --- Act
        final var intersecting1 = StepIntervall.intersecting(si1a, si1b);
        final var intersecting1i = StepIntervall.intersecting(si1b, si1a);
        final var intersecting2 = StepIntervall.intersecting(si2a, si2b);
        final var intersecting2i = StepIntervall.intersecting(si2b, si2a);
        final var intersecting3 = StepIntervall.intersecting(si3a, si3b);
        final var intersecting3i = StepIntervall.intersecting(si3b, si3a);

        // -- Assert
        Assertions.assertTrue(intersecting1);
        Assertions.assertTrue(intersecting1i);
        Assertions.assertTrue(intersecting2);
        Assertions.assertTrue(intersecting2i);
        Assertions.assertFalse(intersecting3);
        Assertions.assertFalse(intersecting3i);
    }

    @Test
    void intersectionLength() {
        // --- Arrange
        final var si1a = new StepIntervall(1, 1.0, 2.0);
        final var si1b = new StepIntervall(2, 0.0, 1.5);
        final var si2a = new StepIntervall(1, 1.0, 2.0);
        final var si2b = new StepIntervall(2, 1.3, 1.55);
        final var si3a = new StepIntervall(1, 1.0, 2.0);
        final var si3b = new StepIntervall(2, 0.0, 0.5);

        // --- Act
        final var intersecting1 = StepIntervall.intersectionLength(si1a, si1b);
        final var intersecting1i = StepIntervall.intersectionLength(si1b, si1a);
        final var intersecting2 = StepIntervall.intersectionLength(si2a, si2b);
        final var intersecting2i = StepIntervall.intersectionLength(si2b, si2a);
        final var intersecting3 = StepIntervall.intersectionLength(si3a, si3b);
        final var intersecting3i = StepIntervall.intersectionLength(si3b, si3a);

        // -- Assert
        Assertions.assertEquals(0.5, intersecting1);
        Assertions.assertEquals(0.5, intersecting1i);
        Assertions.assertEquals(0.25, intersecting2);
        Assertions.assertEquals(0.25, intersecting2i);
        Assertions.assertEquals(0.0, intersecting3);
        Assertions.assertEquals(0.0, intersecting3i);
    }

    @Test
    void isApproxSame() {
        // --- Arrange
        final var si0 = new StepIntervall(-3, -3.0, -2.0);
        final var si1 = new StepIntervall(-1, -1.0, 0.0);
        final var si2 = new StepIntervall(1, 0.0, 1.0);
        final var si3 = new StepIntervall(2, 1.0, 2.0);
        final var si4 = new StepIntervall(4, 3.0, 4.0);
        final var si5 = new StepIntervall(5, 4.0, 5.0);

        // --- Act & Assert
        Assertions.assertTrue(isApproxSame(si0, si0));
        Assertions.assertTrue(isApproxSame(si1, si1));
        Assertions.assertTrue(isApproxSame(si2, si2));
        Assertions.assertTrue(isApproxSame(si3, si3));
        Assertions.assertTrue(isApproxSame(si4, si4));
        Assertions.assertTrue(isApproxSame(si5, si5));
    }

    private boolean isApproxSame(StepIntervall si1, StepIntervall si2) {
        return si1.step() == si2.step()
                && MMath.isApproxSame(si1.start(), si2.start())
                && MMath.isApproxSame(si1.end(), si2.end());
    }
}