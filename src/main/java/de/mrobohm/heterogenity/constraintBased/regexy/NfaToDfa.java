package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.heterogenity.constraintBased.regexy.minimizer.NfaMinimization;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class NfaToDfa {
    private NfaToDfa() {
    }

    public static DFA convert(NFA preNfa) {
        final var nfa = NfaMinimization.reduce(preNfa);
        final var powerStateSet = SSet.powerSet(nfa.stateSet());
        final var multiStateSet= powerStateSet.stream()
                .map(stateSet -> {
                    final var fullTransition= inputAlphabet()
                            .collect(Collectors.toMap(
                                    Function.identity(),
                                    character -> (SortedSet<Integer>)stateSet.stream()
                                            .flatMap(s -> s.transitionMap().getOrDefault(character, SSet.of()).stream())
                                            .collect(Collectors.toCollection(TreeSet::new))
                            ));
                    return new MultiState(stateSet, fullTransition);
                })
                .collect(Collectors.toSet());
        final var initMultiStateOpt = multiStateSet.stream()
                .filter(multiState -> multiState.activeStateSet.size() == 1)
                .filter(multiState -> multiState.activeStateSet.contains(nfa.initState()))
                .findFirst();
        assert initMultiStateOpt.isPresent();
        return multiStatesToDfa(initMultiStateOpt.get(), multiStateSet);
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
//        return Stream.concat(alphabet, digits).filter(NFA::isInputCharacter);
        return Stream.of('a', 'b', 'c');
    }

    private static DFA multiStatesToDfa(MultiState initState, Set<MultiState> multiStateSet) {
        assert multiStateSet.contains(initState);
        final var idMap = StreamExtensions
                .zip(
                        Stream.iterate(0, i -> i + 1),
                        multiStateSet.stream(),
                        (i, multiState) -> new Pair<>(multiState, i)
                )
                .collect(Collectors.toMap(Pair::first, Pair::second));
        final var realTransitionMapMap = multiStateSet.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        multiState -> multiStateToTransitionMap(multiState, multiStateSet).entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> idMap.get(e.getValue())
                                ))
                ));
        final var newStateSet = multiStateSet.stream()
                .map(ms -> {
                    final var newId = idMap.get(ms);
                    final var newTransitionMap = realTransitionMapMap.get(ms);
                    return new StateDet(newId, newTransitionMap, ms.isAcceptState());
                })
                .collect(Collectors.toCollection(TreeSet::new));
        final var newInitStateOpt = newStateSet.stream()
                .filter(sd -> sd.id() == idMap.get(initState))
                .findFirst();
        assert newInitStateOpt.isPresent();
        return new DFA(newInitStateOpt.get(), newStateSet);
    }

    private static Map<Character, MultiState> multiStateToTransitionMap(
            MultiState multiState, Set<MultiState> multiStateSet
    ) {
        return multiState.transitionMap.keySet().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        character -> {
                            final var targetIdSet = multiState.transitionMap.get(character);
                            final var targetMultiStateOpt = multiStateSet.stream()
                                    .filter(ms -> ms.activeStateSet.size() == targetIdSet.size())
                                    .filter(ms -> ms.activeStateSet.stream().map(State::id).allMatch(targetIdSet::contains))
                                    .findFirst();
                            assert targetMultiStateOpt.isPresent();
                            return targetMultiStateOpt.get();
                        }
                ));
    }


    private record MultiState(SortedSet<State> activeStateSet, Map<Character, SortedSet<Integer>> transitionMap) {
        boolean isAcceptState() {
            return activeStateSet.stream().anyMatch(State::isAcceptState);
        }
    }
}