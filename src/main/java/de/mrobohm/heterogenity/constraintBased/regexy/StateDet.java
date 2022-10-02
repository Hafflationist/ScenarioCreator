package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.stream.Collectors;

public record StateDet(int id,
                       Map<Character, Integer> transitionMap,
                       boolean isAcceptState) implements Comparable<StateDet> {

    public StateDet withId(int newId) {
        return new StateDet(newId, transitionMap, isAcceptState);
    }

    public StateDet withNextState(Map<Character, Integer> newNextState) {
        return new StateDet(id, newNextState, isAcceptState);
    }

    public StateDet withIsAcceptState(boolean newIsAcceptState) {
        return new StateDet(id, transitionMap, newIsAcceptState);
    }

    public StateDet withAdditionalTransition(char character, int targetStateId) {
        final var newNextStateMap = StreamExtensions
                .prepend(
                        transitionMap.entrySet().stream()
                                .filter(e -> !e.getKey().equals(character))
                                .map(e -> new Pair<>(e.getKey(), e.getValue())),
                        new Pair<>(character, targetStateId)
                )
                .collect(Collectors.toMap(
                        Pair::first,
                        Pair::second
                ));
        return new StateDet(id, newNextStateMap, isAcceptState);
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
            final var stateIdStr = entry.getValue().toString();
            return "(" + id + "-" + entry.getKey() + "->" + stateIdStr + ")";
        }).collect(Collectors.joining(", "));
    }

    @Override
    public int compareTo(@NotNull StateDet o) {
        return this.toString().compareTo(o.toString());
    }
}