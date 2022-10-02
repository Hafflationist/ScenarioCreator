package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.data.column.constraint.regexy.*;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class RegexToNfa {

    private RegexToNfa() {
    }

    public static NFA convert(RegularExpression expression) {
        return switch (expression) {
            case RegularConcatenation rc -> {
                final var nfa1 = convert(rc.expression1());
                final var nfa2 = convert(rc.expression2());
                yield concatenate(nfa1, nfa2);
            }
            case RegularKleene rk -> {
                final var nfa = convert(rk.expression());
                yield kleene(nfa);
            }
            case RegularSum rs -> {
                final var nfa1 = convert(rs.expression1());
                final var nfa2 = convert(rs.expression2());
                yield sum(nfa1, nfa2);
            }
            case RegularTerminal rt -> terminal(rt.terminal());
        };
    }

    private static NFA concatenate(NFA nfa1, NFA preNfa2) {
        final var nfa2 = reidentification(nfa1, preNfa2);
        final var partitionedByAcceptState = StreamExtensions.partition(
                nfa1.stateSet().stream(), State::isAcceptState
        );
        final var newAcceptStateStream = partitionedByAcceptState.yes()
                .map(state -> state
                        .withAdditionalTransition(NFA.EPSILON, nfa2.initState().id())
                        .withIsAcceptState(false)
                );
        final var newStateStream1 = Stream.concat(partitionedByAcceptState.no(), newAcceptStateStream);
        final var newStateSet = Stream
                .concat(newStateStream1, nfa2.stateSet().stream())
                .collect(Collectors.toCollection(TreeSet::new));
        return new NFA(nfa1.initState(), newStateSet);
    }

    private static NFA kleene(NFA nfa) {
        final var partitionedByAcceptState = StreamExtensions.partition(
                nfa.stateSet().stream(), State::isAcceptState
        );
        final var newAcceptStateSet = partitionedByAcceptState.yes()
                .map(state -> state
                        .withAdditionalTransition(NFA.EPSILON, nfa.initState().id())
                        .withIsAcceptState(false) // it's unnecessary but reduces complexity
                )
                .collect(Collectors.toCollection(TreeSet::new));
        final var nonInitStateSet = partitionedByAcceptState.no().filter(s -> s != nfa.initState()).toList();
        final var newInitState = nfa.initState().withIsAcceptState(true);
        final var newStateSet = SSet.prepend(newInitState, SSet.concat(nonInitStateSet, newAcceptStateSet));
        return new NFA(newInitState, newStateSet);
    }

    private static NFA sum(NFA nfa1, NFA preNfa2) {
        final var nfa2 = reidentification(nfa1, preNfa2);
        final var minimalId1 = nfa1.stateSet().stream().mapToInt(State::id).min().orElse(0);
        final var minimalId2 = nfa2.stateSet().stream().mapToInt(State::id).min().orElse(0);
        final var minimalId = Math.min(minimalId1, minimalId2);
        final var transitionMap = Map.of(
                NFA.EPSILON, SSet.of(nfa1.initState().id(), nfa2.initState().id())
        );
        final var newInitState = new State(minimalId - 1, transitionMap, false);
        return new NFA(newInitState, SSet.prepend(newInitState, SSet.concat(nfa1.stateSet(), nfa2.stateSet())));
    }

    private static NFA terminal(Character character) {
        final var initState = new State(
                0, Map.of(character, SSet.of(1)), false
        );
        final var endState = new State(1, Map.of(), true);
        return new NFA(initState, SSet.of(initState, endState));
    }

    private static NFA reidentification(NFA baseNfa, NFA toBeRenamedNfa) {
        final var maxIdBase = baseNfa.stateSet().stream().mapToInt(State::id).max().orElse(0);
        final var idStream = Stream.iterate(maxIdBase + 1, i -> i + 1);
        final var translationMap= StreamExtensions
                .zip(
                        toBeRenamedNfa.stateSet().stream(),
                        idStream,
                        (state, newId) -> new Pair<>(state.id(), newId)
                )
                .collect(Collectors.toMap(Pair::first, Pair::second));
        final var newStateSet = toBeRenamedNfa.stateSet().stream()
                .map(state -> {
                    final var newNextStateMap = state.transitionMap()
                            .entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> (SortedSet<Integer>) entry.getValue().stream()
                                            .map(translationMap::get)
                                            .collect(Collectors.toCollection(TreeSet::new))
                            ));
                    return state
                            .withId(translationMap.get(state.id()))
                            .withNextState(newNextStateMap);
                })
                .collect(Collectors.toCollection(TreeSet::new));
        final var newInitStateOpt = newStateSet.stream()
                .filter(state -> state.id() == translationMap.get(toBeRenamedNfa.initState().id()))
                .findFirst();
        assert newInitStateOpt.isPresent();
        return new NFA(newInitStateOpt.get(), newStateSet);
    }
}