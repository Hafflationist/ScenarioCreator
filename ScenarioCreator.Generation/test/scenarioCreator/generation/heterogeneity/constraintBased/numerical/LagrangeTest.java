package heterogeneity.constraintBased.numerical;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.generation.heterogeneity.constraintBased.numerical.Lagrange;

import java.util.Map;

class LagrangeTest {

    @ParameterizedTest
    @ValueSource(doubles = {3.0})
        //, 3.8, 3.9, 4.0, 4.1, 4.2})
    void interpolate(double newValue) {
        // --- Arrange
        final var partialFunction = new Lagrange.PartialFunction(Map.of(
                0.0, 2.0,
                1.0, 3.0,
                2.0, 12.0,
                5.0, 147.0
        ));

        // -- Act
        final var result = Lagrange.polynomize(partialFunction).apply(newValue);

        // --- Assert
        Assertions.assertEquals(35.0, result);
    }

    @Test
    void partialFunctionCtor() {
        // --- Arrange
        final var nd = new NumericalDistribution(0.25, Map.of(
                1, 0.1,
                2, 0.2,
                4, 0.8,
                5, 0.5
        ));

        // --- Act
        final var partialFunction = new Lagrange.PartialFunction(nd);

        // --- Assert
        final var expectedPartialFunction = new Lagrange.PartialFunction(Map.of(
                0.125, 0.1,
                0.375, 0.2,
                0.625, 0.0,
                0.875, 0.8,
                1.125, 0.5
        ));
        Assertions.assertEquals(expectedPartialFunction.func().keySet().size() + 2, partialFunction.func().keySet().size());
        Assertions.assertEquals(0.1, partialFunction.func().get(0.125));
        Assertions.assertEquals(0.2, partialFunction.func().get(0.375));
        Assertions.assertEquals(0.0, partialFunction.func().get(0.625));
        Assertions.assertEquals(0.8, partialFunction.func().get(0.875));
        Assertions.assertEquals(0.5, partialFunction.func().get(1.125));
    }
}