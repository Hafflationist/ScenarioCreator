package scenarioCreator.generation.heterogeneity.constraintBased.regexy;

import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
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

    public Stream<State> getClosure(State state) {
        return getClosureInner(state, SSet.of());
    }

    private Stream<State> getClosureInner(State state, SortedSet<State> knownStateSet) {
        final var epsilonNext = next(state).toList();
        final var newKnownStateSet = SSet.concat(epsilonNext, knownStateSet);
        return StreamExtensions.prepend(
                epsilonNext.stream()
                        .filter(s -> !knownStateSet.contains(s))
                        .flatMap(s -> getClosureInner(s, newKnownStateSet)),
                state
        );
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
        return "NFA{ initId=" + initState.id() + "; nfa=" + stateSet + '}';
    }
}