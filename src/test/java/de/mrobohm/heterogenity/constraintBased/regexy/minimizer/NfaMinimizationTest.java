package de.mrobohm.heterogenity.constraintBased.regexy.minimizer;

import de.mrobohm.data.column.constraint.regexy.RegularConcatenation;
import de.mrobohm.data.column.constraint.regexy.RegularSum;
import de.mrobohm.data.column.constraint.regexy.RegularTerminal;
import de.mrobohm.heterogenity.constraintBased.regexy.NFA;
import de.mrobohm.heterogenity.constraintBased.regexy.RegexToNfa;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NfaMinimizationTest {

    @Test
    void removeEpsilonTransition() {
        // --- Arrange

        final var regex = new RegularConcatenation(
                new RegularSum(
                        new RegularTerminal('a'),
                        new RegularTerminal('b')
                ),
                new RegularSum(
                        new RegularTerminal('c'),
                        new RegularTerminal('d')
                )
        );

        final var nfa = RegexToNfa.convert(regex);

        // --- Act
        final var nfaWithoutEpsilon = NfaMinimization.removeEpsilonTransition(nfa);

        // --- Assert
        final var containsEpsilon = nfaWithoutEpsilon.getNfa().stream()
                .anyMatch(state -> state.getNextState().containsKey(NFA.EPSILON));
        Assertions.assertFalse(containsEpsilon);

        System.out.println("\nnfa:");
        System.out.println(nfa);

        System.out.println("\nnfa without EPSILON:");
        System.out.println(nfaWithoutEpsilon);
    }

    @Test
    void removeUnreachableStates() {
        // --- Arrange
        final var regex = new RegularConcatenation(
                new RegularSum(
                        new RegularTerminal('a'),
                        new RegularTerminal('b')
                ),
                new RegularSum(
                        new RegularTerminal('c'),
                        new RegularTerminal('d')
                )
        );

        final var nfa = RegexToNfa.convert(regex);
        final var nfaWithoutEpsilon = NfaMinimization.removeEpsilonTransition(nfa);

        // --- Act
        final var nfaWithoutEpsilonReduced = NfaMinimization.removeUnreachableStates(nfaWithoutEpsilon);

        // --- Assert
        System.out.println("\nnfa without EPSILON:");
        System.out.println(nfaWithoutEpsilon);

        System.out.println("\nnfa without EPSILON and with reduction:");
        System.out.println(nfaWithoutEpsilonReduced);
    }
}