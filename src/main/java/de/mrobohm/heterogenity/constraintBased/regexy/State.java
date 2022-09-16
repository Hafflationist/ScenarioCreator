package de.mrobohm.heterogenity.constraintBased.regexy;

import java.util.*;
import java.util.stream.Collectors;

public class State {
    private final int stateId;
    private final Map<Character, ArrayList<State>> nextState;
    private Set<State> stateSet;
    private boolean acceptState;

    // This constructor is used for NFA
    public State(int id) {
        this.stateId = id;
        this.nextState = new HashMap<>();
        this.acceptState = false;
    }

    // This constructor is used for DFA
    public State(Set<State> stateSet, int id) {
        this.stateSet = stateSet;
        this.stateId = id;
        this.nextState = new HashMap<>();

        // find if there is final state in this set of states
        for (State p : stateSet) {
            if (p.isAcceptState()) {
                this.acceptState = true;
                break;
            }
        }
    }

    public void addTransition(State next, char key) {
        this.nextState.computeIfAbsent(key, k -> new ArrayList<>()).add(next);
    }

    public ArrayList<State> getAllTransitions(char c) {
        return Optional.ofNullable(this.nextState.get(c)).orElse(new ArrayList<>());
    }

    public Map<Character, ArrayList<State>> getNextState() {
        return nextState;
    }

    public int getStateId() {
        return stateId;
    }

    public boolean isAcceptState() {
        return acceptState;
    }

    public void setAcceptState(boolean acceptState) {
        this.acceptState = acceptState;
    }

    public Set<State> getStateSet() {
        return stateSet;
    }

    @Override
    public String toString() {
        return "q{" +
                "id=" + stateId +
                ", (->)=" + nextStateToString() +
                ", accept=" + acceptState +
                '}';
    }

    private String nextStateToString() {
        return nextState.entrySet().stream().map(entry -> {
            final var stateIdStr = entry.getValue().stream()
                    .map(s -> Integer.toString(s.stateId))
                    .collect(Collectors.joining(", "));
            return "(" + entry.getKey() + "->" + stateIdStr + ")";
        }).collect(Collectors.joining(", "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final var state = (State) o;
        return stateId == state.stateId
                && acceptState == state.acceptState
                && nextStateToString().equals(state.nextStateToString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateId, nextState, acceptState);
    }
}