package de.mrobohm.heterogeneity.constraintBased.regexy;

import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

public record State(int id,
                    Map<Character, SortedSet<Integer>> transitionMap,
                    boolean isAcceptState) implements Comparable<State> {

    public State withId(int newId) {
        return new State(newId, transitionMap, isAcceptState);
    }

    public State withNextState(Map<Character, SortedSet<Integer>> newNextState) {
        return new State(id, newNextState, isAcceptState);
    }

    public State withIsAcceptState(boolean newIsAcceptState) {
        return new State(id, transitionMap, newIsAcceptState);
    }

    public State withAdditionalTransition(char character, int targetStateId) {
        final var newNextStateMap = StreamExtensions
                .prepend(
                        transitionMap.entrySet().stream()
                                .filter(e -> !e.getKey().equals(character))
                                .map(e -> new Pair<>(e.getKey(), e.getValue())),
                        new Pair<>(character, SSet.prepend(targetStateId, transitionMap.getOrDefault(character, SSet.of())))
                )
                .collect(Collectors.toMap(
                        Pair::first,
                        Pair::second
                ));
        return new State(id, newNextStateMap, isAcceptState);
    }

    @Override
    public String toString() {
        return "q{" +
                "id=" + id +
                ", (->)=" + nextStateToString() +
                ", accept=" + isAcceptState +
                '}';
    }

    private String nextStateToString() {
        return transitionMap.entrySet().stream().map(entry -> {
            final var stateIdStr = entry.getValue().stream()
                    .map(sid -> Integer.toString(sid))
                    .collect(Collectors.joining(", "));
            return "(" + id + "-" + entry.getKey() + "->" + stateIdStr + ")";
        }).collect(Collectors.joining(", "));
    }

    @Override
    public int compareTo(@NotNull State o) {
        return this.toString().compareTo(o.toString());
    }
}