package scenarioCreator.generation.heterogeneity.constraintBased.regexy.minimizer;

import scenarioCreator.generation.heterogeneity.constraintBased.regexy.NFA;
import scenarioCreator.generation.heterogeneity.constraintBased.regexy.State;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class NfaMinimization {
    private NfaMinimization() {
    }

    public static NFA reduce(NFA nfa) {
        return removeUnreachableStates(removeEpsilonTransition(nfa));
    }

    public static NFA removeEpsilonTransition(NFA nfa) {
        final var partitionWithEpsilon = StreamExtensions.partition(
                nfa.stateSet().stream(),
                s -> s.transitionMap().containsKey(NFA.EPSILON)
        );
        final var statesWithEpsilonSet = partitionWithEpsilon.yes().toList();
        if (statesWithEpsilonSet.isEmpty()) {
            return nfa;
        }
        final var newStateSetHalf = statesWithEpsilonSet.stream().map(state -> {
            final var epsilonClosure = getEpsilonClosure(nfa, state).toList();
            final var epsilonClosureTransitionSet = epsilonClosure.stream()
                    .flatMap(s -> s.transitionMap().entrySet().stream())
                    .collect(Collectors.toSet());
            final var newNextState = epsilonClosureTransitionSet.stream().map(Map.Entry::getKey)
                    .filter(c -> c != NFA.EPSILON)
                    .distinct()
                    .collect(Collectors.toMap(
                            Function.identity(),
                            character -> (SortedSet<Integer>) epsilonClosureTransitionSet.stream()
                                    .filter(e -> e.getKey().equals(character))
                                    .map(Map.Entry::getValue)
                                    .flatMap(SortedSet::stream)
                                    .collect(Collectors.toCollection(TreeSet::new))
                    ));
            final var newIsAcceptState = epsilonClosure.stream().anyMatch(State::isAcceptState);
            return state
                    .withNextState(newNextState)
                    .withIsAcceptState(newIsAcceptState);
        });
        final var newStateSet = (SortedSet<State>) Stream
                .concat(newStateSetHalf, partitionWithEpsilon.no())
                .collect(Collectors.toCollection(TreeSet::new));
        final var newInitState = newStateSet.stream()
                .filter(s -> s.id() == nfa.initState().id())
                .findFirst();
        assert newInitState.isPresent();
        return new NFA(newInitState.get(), newStateSet);
    }

    private static Stream<State> getEpsilonClosure(NFA nfa, State state) {
        return getEpsilonClosureInner(nfa, state, SSet.of());
    }

    private static Stream<State> getEpsilonClosureInner(NFA nfa, State state, SortedSet<State> knownStateSet) {
        final var epsilonNext = nfa.next(state, NFA.EPSILON).toList();
        final var newKnownStateSet = SSet.concat(epsilonNext, knownStateSet);
        return StreamExtensions.prepend(
                epsilonNext.stream()
                        .filter(s -> !knownStateSet.contains(s))
                        .flatMap(s -> getEpsilonClosureInner(nfa, s, newKnownStateSet)),
                state
        );
    }

    public static NFA removeUnreachableStates(NFA nfa) {
        final var closureSet = nfa.getClosure(nfa.initState())
                .collect(Collectors.toCollection(TreeSet::new));
        return new NFA(nfa.initState(), closureSet);
    }
}