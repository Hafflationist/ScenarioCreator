package de.mrobohm.heterogenity.constraintBased.regexy;

import java.util.LinkedList;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NFA {

    public static final char EPSILON = '%';
    private final LinkedList<State> nfa = new LinkedList<>();

    public static boolean isInputCharacter(char c) {
        return Character.isDigit(c) || Character.isLetter(c) || c == NFA.EPSILON;
    }

    public static Stream<Character> inputAlphabet() {
        var alphabet = Stream.concat(
                Stream.of('ä', 'ö', 'ü', 'ß', 'Ä', 'Ö', 'Ü', 'ẞ'),
                Stream.concat(
                        IntStream.rangeClosed('A', 'Z').mapToObj(var -> (char) var),
                        IntStream.rangeClosed('a', 'z').mapToObj(var -> (char) var)
                )
        );
        var digits = IntStream.rangeClosed('0', '9').mapToObj(var -> (char) var);
        return Stream.concat(alphabet, digits).filter(NFA::isInputCharacter);
    }

    public LinkedList<State> getNfa() {
        return nfa;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NFA nfa1 = (NFA) o;
        return Objects.equals(nfa, nfa1.nfa);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nfa);
    }

    @Override
    public String toString() {
        return "NFA{" + "nfa=" + nfa + '}';
    }
}