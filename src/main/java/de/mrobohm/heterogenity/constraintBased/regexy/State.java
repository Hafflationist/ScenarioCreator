package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class State implements Comparable<State> {

    private static final Map<Integer, State> _allInstances = new HashMap<>();
    private final int _stateId;
    private final Map<Character, SortedSet<Integer>> _nextState;
    private SortedSet<Integer> _stateSet;
    private boolean _isAcceptState;


    // This constructor is used for NFA
    public State(int id) {
        _allInstances.put(id, this);
        this._stateId = id;
        this._nextState = new HashMap<>();
        this._isAcceptState = false;
    }

    // The full ctor
    public State(int id, Map<Character, SortedSet<Integer>> nextState, SortedSet<Integer> stateSet, boolean isAcceptState) {
        _allInstances.put(id, this);
        this._stateId = id;
        this._nextState = nextState;
        this._stateSet = stateSet;
        this._isAcceptState = isAcceptState;
    }

    // This constructor is used for DFA
    public State(SortedSet<State> stateSet, int id) {
        _allInstances.put(id, this);
        this._stateSet = stateSet.stream().map(State::getStateId).collect(Collectors.toCollection(TreeSet::new));
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
        this._nextState.computeIfAbsent(key, k -> new TreeSet<>()).add(next._stateId);
    }

    public ArrayList<State> getAllTransitions(char c, Set<State> allStateSet) {
        final var stateIdList = this._nextState.get(c);
        if (stateIdList == null) {
            return new ArrayList<>();
        }
        return stateIdList.stream()
                .map(sid -> _allInstances.getOrDefault(sid, null))
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(ArrayList::new));
    }


    public int getStateId() {
        return _stateId;
    }

    public Map<Character, SortedSet<Integer>> getNextState() {
        return _nextState;
    }

    public boolean isAcceptState() {
        return _isAcceptState;
    }

    public void setAcceptState(boolean acceptState) {
        this._isAcceptState = acceptState;
    }

    public SortedSet<State> getStateSet(Stream<State> allStateStream) {
        return allStateStream
                .filter(s -> _stateSet.contains(s._stateId))
                .collect(Collectors.toCollection(TreeSet::new));
    }


    public StateDet determinise() {
        final var deterministic = _nextState.values().stream().allMatch(arr -> arr.size() <= 1);
        assert deterministic : "NFA should be deterministic!";
        var newNextState = _nextState.keySet().stream()
                .filter(character -> !_nextState.get(character).isEmpty())
                .collect(Collectors.toMap(
                        Function.identity(),
                        character -> _nextState.get(character).stream().findFirst().orElseThrow()
                ));
        return new StateDet(_stateId, newNextState, _isAcceptState);
    }

    public State withId(int newId){
        return new State(newId, _nextState, _stateSet, _isAcceptState);
    }

    public State withNextState(Map<Character, SortedSet<Integer>> newNextState) {
        return new State(_stateId, newNextState, _stateSet, _isAcceptState);
    }

    public State withIsAcceptState(boolean newIsAcceptState) {
        return new State(_stateId, _nextState, _stateSet, newIsAcceptState);
    }

    public State withAdditionalTransition(char character, int targetStateId) {
        final var newNextStateMap = StreamExtensions
                .prepend(
                        _nextState.entrySet().stream()
                                .filter(e -> !e.getKey().equals(character))
                                .map(e -> new Pair<>(e.getKey(), e.getValue())),
                        new Pair<>(character, SSet.prepend(targetStateId, _nextState.getOrDefault(character, SSet.of())))
                )
                .collect(Collectors.toMap(
                        Pair::first,
                        Pair::second
                ));
        return new State(_stateId, newNextStateMap, _stateSet, _isAcceptState);
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
                    .map(sid -> Integer.toString(sid))
                    .collect(Collectors.joining(", "));
            return "(" + _stateId + "-" + entry.getKey() + "->" + stateIdStr + ")";
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

    @Override
    public int compareTo(@NotNull State o) {
        return this.toString().compareTo(o.toString());
    }
}