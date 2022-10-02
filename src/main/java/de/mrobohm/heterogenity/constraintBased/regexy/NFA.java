package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public record NFA (State initState, SortedSet<State> stateSet){

    public static final char EPSILON = '-';

    public NFA(State initState, SortedSet<State> stateSet) {
        this.initState = initState;
        this.stateSet = StreamExtensions
                .prepend(stateSet.stream().filter(s -> s.id() != initState.id()), initState)
                .collect(Collectors.toCollection(TreeSet::new));
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

    public Stream<State> next(State state, char character) {
        return state.transitionMap().getOrDefault(character, SSet.of()).stream()
                .map(sid -> this.stateSet().stream().filter(s -> s.id() == sid).findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public Stream<State> next(State state) {
        return state.transitionMap().values().stream()
                .flatMap(SortedSet::stream)
                .map(sid -> this.stateSet().stream().filter(s -> s.id() == sid).findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NFA nfa1 = (NFA) o;
        return Objects.equals(stateSet, nfa1.stateSet) && Objects.equals(initState, nfa1.initState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateSet) ^ Objects.hash(initState);
    }

    @Override
    public String toString() {
        return "NFA{" + "nfa=" + stateSet + '}';
    }
}