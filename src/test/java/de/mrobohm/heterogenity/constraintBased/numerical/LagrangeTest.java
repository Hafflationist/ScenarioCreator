package de.mrobohm.heterogenity.constraintBased.numerical;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LagrangeTest {

    @ParameterizedTest
    @ValueSource(doubles = {3.0, 3.8, 3.9, 4.0, 4.1, 4.2})
    void interpolate(double newValue) {
        // --- Arrange
        var partialFunction = Map.of(
                0.0, 2.0,
                1.0, 3.0,
                2.0, 12.0,
                5.0, 147.0
        );

        // -- Act
        var result = Lagrange.polynomize(partialFunction).apply(newValue);

        // --- Assert
        Assertions.assertEquals(35.0, result);
    }
}