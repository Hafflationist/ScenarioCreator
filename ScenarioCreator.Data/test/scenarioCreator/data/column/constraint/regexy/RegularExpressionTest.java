package scenarioCreator.data.column.constraint.regexy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RegularExpressionTest {

    @Test
    void toStringTest() {
        // --- Arrange
        final var regexTree = new RegularKleene(
                new RegularConcatenation(
                        new RegularSum(
                                new RegularTerminal('a'),
                                new RegularKleene(
                                        new RegularConcatenation(
                                                new RegularTerminal('b'),
                                                new RegularTerminal('c')
                                        )
                                )
                        ),
                        new RegularTerminal('d')
                )
        );
        final var regexStringExpected = "(a|((bc)*)d)*";

        // --- Act
        var regexStringActual = regexTree.toString();

        // --- Assert
        Assertions.assertEquals(regexStringExpected, regexStringActual);
    }
}