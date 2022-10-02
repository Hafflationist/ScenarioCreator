package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.StreamExtensions;

import java.util.LinkedList;
import java.util.Objects;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NFA {

    public static final char EPSILON = '%';

    private final State _initState;
    private final LinkedList<State> nfa;

    public NFA(State initState, SortedSet<State> allStateSet) {
        _initState = initState;
        nfa = new LinkedList(StreamExtensions.prepend(allStateSet.stream().filter(s -> s.getStateId() != initState.getStateId()), initState).toList());
    }

    public NFA() {
        _initState = null;
        nfa = new LinkedList<>();
    }

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
        //return Stream.of('a', 'b', 'c');
    }

    public State getInitState() {
        return _initState == null ? nfa.getFirst() : _initState;
    }

    public LinkedList<State> getNfa() {
        return nfa;
    }

    public Stream<State> getAcceptStateSet() {
        return nfa.stream().filter(State::isAcceptState);
    }

    public DFA determinise() {
        return new DFA(nfa.stream().map(State::determinise).collect(Collectors.toCollection(LinkedList::new)));
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