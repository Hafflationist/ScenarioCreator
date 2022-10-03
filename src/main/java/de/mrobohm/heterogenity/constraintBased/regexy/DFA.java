package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record DFA(StateDet initState, SortedSet<StateDet> stateSet) {
    public DFA(StateDet initState, SortedSet<StateDet> stateSet) {
        this.initState = initState;
        this.stateSet = StreamExtensions
                .prepend(stateSet.stream().filter(s -> s.id() != initState.id()), initState)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    public Stream<StateDet> next(StateDet state) {
        return state.transitionMap().values().stream()
                .map(sid -> this.stateSet().stream().filter(s -> s.id() == sid).findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public Stream<StateDet> getClosure(StateDet state) {
        return getClosureInner(state, SSet.of());
    }

    private Stream<StateDet> getClosureInner(StateDet state, SortedSet<StateDet> knownStateSet) {
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
        DFA dfa1 = (DFA) o;
        return Objects.equals(stateSet, dfa1.stateSet) && Objects.equals(initState, dfa1.initState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateSet) ^ Objects.hash(initState);
    }

    @Override
    public String toString() {
        return "DFA{ initId=" + initState.id() + "; dfa=" + stateSet + '}';
    }
}