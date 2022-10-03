package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.data.column.constraint.regexy.RegularKleene;
import de.mrobohm.data.column.constraint.regexy.RegularTerminal;
import org.junit.jupiter.api.Test;

import java.util.Random;

class DfaToRandomStringTest {

    @Test
    void generate() {
        // --- Assert
        final var regex = new RegularKleene(new RegularTerminal('a'));
        //final var regex = "(a|(bc)*d)*";
        final var nfa = RegexToNfa.convert(regex);
        final var dfa = NfaToDfa.convert(nfa);
        final var random = new Random();

        // --- Act
        final var randomString = DfaToRandomString.generate(dfa, random);
        final var randomString2 = DfaToRandomString.generate(dfa, random);
        final var randomString3 = DfaToRandomString.generate(dfa, random);
        final var randomString4 = DfaToRandomString.generate(dfa, random);

        // --- Assert
//        System.out.println(nfa);
//        System.out.println(dfa);
        System.out.println(randomString);
        System.out.println(randomString2);
        System.out.println(randomString3);
        System.out.println(randomString4);
    }
}