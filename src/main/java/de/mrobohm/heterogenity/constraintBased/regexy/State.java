package de.mrobohm.heterogenity.constraintBased.regexy;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class State {
    private final int _stateId;
    private final Map<Character, ArrayList<State>> _nextState;
    private Set<Integer> _stateSet;
    private boolean _isAcceptState;

    // This constructor is used for NFA
    public State(int id) {
        this._stateId = id;
        this._nextState = new HashMap<>();
        this._isAcceptState = false;
    }

    // This constructor is used for DFA
    public State(Set<State> stateSet, int id) {
        this._stateSet = stateSet.stream().map(State::getStateId).collect(Collectors.toSet());
        this._stateId = id;
        this._nextState = new HashMap<>();

        // find if there is final state in this set of states
        for (State p : stateSet) {
            if (p.isAcceptState()) {
                this._isAcceptState = true;
                break;
            }
        }
    }

    public void addTransition(State next, char key) {
        this._nextState.computeIfAbsent(key, k -> new ArrayList<>()).add(next);
    }

    public ArrayList<State> getAllTransitions(char c) {
        return Optional.ofNullable(this._nextState.get(c)).orElse(new ArrayList<>());
    }

    public Map<Character, ArrayList<State>> getNextState() {
        return _nextState;
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

    public Set<State> getStateSet(Stream<State> allStateSet) {
        return allStateSet
                .filter(s -> _stateSet.contains(s._stateId))
                .collect(Collectors.toSet());
    }


    public StateDet determinise() {
        final var deterministic = _nextState.values().stream().allMatch(arr -> arr.size() <= 1);
        assert deterministic : "NFA should be deterministic!";
        var newNextState = _nextState.keySet().stream()
                .filter(character -> !_nextState.get(character).isEmpty())
                .collect(Collectors.toMap(
                        Function.identity(),
                        character -> _nextState.get(character).stream().findFirst().orElseThrow().getStateId()
                ));
        return new StateDet(_stateId, newNextState, _isAcceptState);
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
        return _nextState.entrySet().stream().map(entry -> {
            final var stateIdStr = entry.getValue().stream()
                    .map(s -> Integer.toString(s._stateId))
                    .collect(Collectors.joining(", "));
            return "(" + entry.getKey() + "->" + stateIdStr + ")";
        }).collect(Collectors.joining(", "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final var state = (State) o;
        return _stateId == state._stateId
                && _isAcceptState == state._isAcceptState
                && nextStateToString().equals(state.nextStateToString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(_stateId, nextStateToString(), _isAcceptState);
    }
}