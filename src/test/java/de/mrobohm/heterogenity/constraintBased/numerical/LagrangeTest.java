package de.mrobohm.heterogenity.constraintBased.numerical;

import de.mrobohm.data.column.context.NumericalDistribution;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

class LagrangeTest {

    @ParameterizedTest
    @ValueSource(doubles = {3.0})
        //, 3.8, 3.9, 4.0, 4.1, 4.2})
    void interpolate(double newValue) {
        // --- Arrange
        var partialFunction = new Lagrange.PartialFunction(Map.of(
                0.0, 2.0,
                1.0, 3.0,
                2.0, 12.0,
                5.0, 147.0
        ));

        // -- Act
        var result = Lagrange.polynomize(partialFunction).apply(newValue);

        // --- Assert
        Assertions.assertEquals(35.0, result);
    }

    @Test
    void partialFunctionCtor() {
        // --- Arrange
        var nd = new NumericalDistribution(0.25, Map.of(
                1, 0.1,
                2, 0.2,
                4, 0.8,
                5, 0.5
        ));

        // --- Act
        var partialFunction = new Lagrange.PartialFunction(nd);

        // --- Assert
        var expectedPartialFunction = new Lagrange.PartialFunction(Map.of(
                0.125, 0.1,
                0.375, 0.2,
                0.625, 0.0,
                0.875, 0.8,
                1.125, 0.5
        ));
        Assertions.assertEquals(expectedPartialFunction, partialFunction);
    }
}