package de.mrobohm.heterogeneity.constraintBased.regexy;

import de.mrobohm.data.column.constraint.regexy.RegularExpression;
import de.mrobohm.data.column.constraint.regexy.RegularKleene;
import de.mrobohm.data.column.constraint.regexy.RegularTerminal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

class DfaToRandomStringTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
    void generateWildcard(int seed) {
        // --- Assert
        final var regex = RegularExpression.acceptsEverything();
        final var nfa = RegexToNfa.convert(regex);
        final var dfa = NfaToDfa.convert(nfa);
        final var random = new Random(seed);

        // --- Act
        final var randomString = DfaToRandomString.generate(dfa, random);

        // --- Assert
        Assertions.assertTrue(dfa.acceptsString(randomString));
        Assertions.assertTrue(dfa.acceptsString(randomString + 'b'));
        Assertions.assertFalse(dfa.negate().acceptsString(randomString));
        Assertions.assertFalse(dfa.negate().acceptsString(randomString + 'b'));
        System.out.println(randomString);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
    void generate(int seed) {
        // --- Assert
        final var regex = new RegularKleene(new RegularTerminal('a'));
        final var nfa = RegexToNfa.convert(regex);
        final var dfa = NfaToDfa.convert(nfa);
        final var random = new Random(seed);

        // --- Act
        final var randomString = DfaToRandomString.generate(dfa, random);

        // --- Assert
        Assertions.assertTrue(dfa.acceptsString(randomString));
        Assertions.assertFalse(dfa.acceptsString(randomString + 'b'));
        Assertions.assertFalse(dfa.negate().acceptsString(randomString));
        Assertions.assertTrue(dfa.negate().acceptsString(randomString + 'b'));
//        System.out.println(nfa);
//        System.out.println(dfa);
        System.out.println(randomString);
    }
}