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
                nfa1.getNfa().stream(), State::isAcceptState
        );
        final var newAcceptStateStream = partitionedByAcceptState.yes()
                .map(state -> state
                        .withAdditionalTransition(NFA.EPSILON, nfa2.getInitState().getStateId())
                        .withIsAcceptState(false)
                );
        final var newStateSet1 = Stream.concat(partitionedByAcceptState.no(), newAcceptStateStream);
        final var newStateSet = Stream
                .concat(newStateSet1, nfa2.getNfa().stream())
                .collect(Collectors.toCollection(TreeSet::new));
        return new NFA(nfa1.getInitState(), newStateSet);
    }

    private static NFA kleene(NFA nfa) {
        final var partitionedByAcceptState = StreamExtensions.partition(
                nfa.getNfa().stream(), State::isAcceptState
        );
        final var newAcceptStateSet = partitionedByAcceptState.yes()
                .map(state -> state
                        .withAdditionalTransition(NFA.EPSILON, nfa.getInitState().getStateId())
                        .withIsAcceptState(false) // it's unnecessary but reduces complexity
                )
                .collect(Collectors.toCollection(TreeSet::new));
        final var nonInitStateSet = partitionedByAcceptState.no().filter(s -> s != nfa.getInitState()).toList();
        final var newInitState = nfa.getInitState().withIsAcceptState(true);
        final var newStateSet = SSet.prepend(newInitState, SSet.concat(nonInitStateSet, newAcceptStateSet));
        return new NFA(newInitState, newStateSet);
    }

    private static NFA sum(NFA nfa1, NFA preNfa2) {
        final var nfa2 = reidentification(nfa1, preNfa2);
        final var minimalId1 = nfa1.getNfa().stream().mapToInt(State::getStateId).min().orElse(0);
        final var minimalId2 = nfa2.getNfa().stream().mapToInt(State::getStateId).min().orElse(0);
        final var minimalId = Math.min(minimalId1, minimalId2);
        final var transitionMap = Map.of(
                NFA.EPSILON, SSet.of(nfa1.getInitState().getStateId(), nfa2.getInitState().getStateId())
        );
        final var newInitState = new State(minimalId - 1, transitionMap, SSet.of(), false);
        return new NFA(newInitState, SSet.prepend(newInitState, SSet.concat(nfa1.getNfa(), nfa2.getNfa())));
    }

    private static NFA terminal(Character character) {
        final var initState = new State(
                0, Map.of(character, SSet.of(1)), SSet.of(0, 1), false
        );
        final var endState = new State(1, Map.of(), SSet.of(0, 1), true);
        return new NFA(initState, SSet.of(initState, endState));
    }

    private static NFA reidentification(NFA baseNfa, NFA toBeRenamedNfa) {
        final var maxIdBase = baseNfa.getNfa().stream().mapToInt(State::getStateId).max().orElse(0);
        final var idStream = Stream.iterate(maxIdBase + 1, i -> i + 1);
        final var translationMap= StreamExtensions
                .zip(
                        toBeRenamedNfa.getNfa().stream(),
                        idStream,
                        (state, newId) -> new Pair<>(state.getStateId(), newId)
                )
                .collect(Collectors.toMap(Pair::first, Pair::second));
        final var newStateSet = toBeRenamedNfa.getNfa().stream()
                .map(state -> {
                    final var newNextStateMap = state.getNextState()
                            .entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> (SortedSet<Integer>) entry.getValue().stream()
                                            .map(translationMap::get)
                                            .collect(Collectors.toCollection(TreeSet::new))
                            ));
                    return state
                            .withId(translationMap.get(state.getStateId()))
                            .withNextState(newNextStateMap);
                })
                .collect(Collectors.toCollection(TreeSet::new));
        final var newInitStateOpt = newStateSet.stream()
                .filter(state -> state.getStateId() == translationMap.get(toBeRenamedNfa.getInitState().getStateId()))
                .findFirst();
        assert newInitStateOpt.isPresent();
        return new NFA(newInitStateOpt.get(), newStateSet);
    }
}