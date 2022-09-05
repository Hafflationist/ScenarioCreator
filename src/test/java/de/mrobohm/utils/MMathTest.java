package de.mrobohm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

class MMathTest {

    @Test
    void gcd() {
        // --- Arrange
        final var a = 0.4;
        final var b = 0.3;

        // --- Act
        final var c = MMath.gcd(a, b);
        final var cc = MMath.gcd(b, a);

        // --- Assert
        System.out.println(c);
        System.out.println(cc);
        Assertions.assertTrue(Math.abs(c - 0.1) <= 0.01);
    }

    private static Stream<Arguments> provideParameters() {
        final var setOfDoubles = Set.of(1.0, 0.3, 0.2, 1.54e-2, 2e2, 4e2, 3.32e2);
        return setOfDoubles.stream()
                        .flatMap(x -> setOfDoubles.stream()
                                .map(y -> Arguments.of(x, y)));
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void gcd2(double a, double b) {
        // --- Arrange
        // --- Act
        final var gcd = MMath.gcd(a, b);

        // --- Assert
        final var errorA =  (((a / gcd) + 0.1) % 1.0) - 0.1;
        final var errorB =  (((b / gcd) + 0.1) % 1.0) - 0.1;
        System.out.println((a / gcd) + " -> error: " + errorA);
        System.out.println((b / gcd) + " -> error: " + errorB);
        Assertions.assertTrue(Math.abs(errorA) <= 0.01);
        Assertions.assertTrue(Math.abs(errorB) <= 0.01);
    }
}