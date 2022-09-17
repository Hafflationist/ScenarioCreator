package de.mrobohm.heterogenity.constraintBased.regexy;

import java.util.*;
import java.util.stream.Collectors;

public class StateDet {
    private final int _stateId;
    private final Map<Character, Integer> _nextState;
    private boolean _isAcceptState;

    // This constructor is used for DFA
    public StateDet(int id, Map<Character, Integer> nextState, boolean isAcceptState) {
        this._stateId = id;
        this._nextState = nextState;
        this._isAcceptState = isAcceptState;
    }

    public void setTransition(StateDet next, char key) {
        this._nextState.put(key, next._stateId);
    }

    public Optional<Integer> getTransition(char c) {
        return Optional.ofNullable(this._nextState.get(c));
    }

    public int getStateId() {
        return _stateId;
    }

    public boolean isAcceptState() {
        return _isAcceptState;
    }

    public void setAcceptState(boolean acceptState) {
        this._isAcceptState = acceptState;
    }


    @Override
    public String toString() {
        return "q{" +
                "id=" + _stateId +
                ", (->)=" + nextStateToString() +
                ", accept=" + _isAcceptState +
                '}';
    }

    private String nextStateToString() {
        return _nextState.entrySet().stream()
                .map(entry -> "(" + entry.getKey() + "->" + entry.getValue() + ")")
                .collect(Collectors.joining(", "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final var state = (StateDet) o;
        return _stateId == state._stateId
                && _isAcceptState == state._isAcceptState
                && nextStateToString().equals(state.nextStateToString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(_stateId, _nextState, _isAcceptState);
    }
}