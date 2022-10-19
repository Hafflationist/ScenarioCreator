package de.mrobohm.heterogeneity.constraintBased.regexy;

import de.mrobohm.data.column.constraint.regexy.RegularConcatenation;
import de.mrobohm.data.column.constraint.regexy.RegularKleene;
import de.mrobohm.data.column.constraint.regexy.RegularSum;
import de.mrobohm.data.column.constraint.regexy.RegularTerminal;
import org.junit.jupiter.api.Test;

class RegexToNfaTest {

    @Test
    void convert() {
        // --- Arrange
        final var regex1 = new RegularKleene(new RegularTerminal('a'));
        final var regex2 = new RegularConcatenation(
                new RegularTerminal('a'),
                new RegularTerminal('b')
        );
        final var regex3 = new RegularKleene(
                new RegularConcatenation(
                        new RegularTerminal('a'),
                        new RegularTerminal('b')
                )
        );
        final var regex4 = new RegularKleene(
                new RegularSum(
                        new RegularConcatenation(
                                new RegularTerminal('a'),
                                new RegularTerminal('b')
                        ),
                        new RegularConcatenation(
                                new RegularTerminal('c'),
                                new RegularTerminal('d')
                        )
                )
        );
        final var regex5 = new RegularConcatenation(
                new RegularSum(
                        new RegularTerminal('a'),
                        new RegularTerminal('b')
                ),
                new RegularSum(
                        new RegularTerminal('c'),
                        new RegularTerminal('d')
                )
        );

        // --- Act
        final var nfa1 = RegexToNfa.convert(regex1);
        final var nfa2 = RegexToNfa.convert(regex2);
        final var nfa3 = RegexToNfa.convert(regex3);
        final var nfa4 = RegexToNfa.convert(regex4);
        final var nfa5 = RegexToNfa.convert(regex5);

        // --- Assert
        System.out.println("\nnfa1:");
        System.out.println(nfa1);
        System.out.println("\nnfa2:");
        System.out.println(nfa2);
        System.out.println("\nnfa3:");
        System.out.println(nfa3);
        System.out.println("\nnfa4:");
        System.out.println(nfa4);
        System.out.println("\nnfa5:");
        System.out.println(nfa5);
    }
}