package de.mrobohm.heterogenity.constraintBased.regexy;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.Stack;

public class NfaToDfa {
    private static Set<State> set1 = new HashSet<>();
    private static Set<State> set2 = new HashSet<>();

    public static DFA convert(NFA nfa) {
        // Creating the DFA
        NFA dfa = new NFA();

        // Clearing all the states ID for the DFA
        int stateId = 0;

        // Create an arrayList of unprocessed States
        LinkedList<State> unprocessed = new LinkedList<>();

        // Create sets
        set1 = new HashSet<>();
        set2 = new HashSet<>();

        // Add first state to the set1
        set1.add(nfa.getNfa().getFirst());

        // Run the first remove Epsilon the get states that
        // run with epsilon
        removeEpsilonTransition(new HashSet<>(nfa.getNfa()));

        // Create the start state of DFA and add to the stack
        State dfaStart = new State(set2, stateId++);

        dfa.getNfa().addLast(dfaStart);
        unprocessed.addLast(dfaStart);

        // While there is elements in the stack
        while (!unprocessed.isEmpty()) {
            // Process and remove last state in stack
            State state = unprocessed.removeLast();

            // Check if input symbol
            for (Character symbol : NFA.inputAlphabet().toList()) {
                set1 = new HashSet<>();
                set2 = new HashSet<>();

                moveStates(symbol, state.getStateSet(nfa.getNfa().stream()), set1);
                removeEpsilonTransition(new HashSet<>(nfa.getNfa()));

                boolean found = false;
                State st = null;

                for (int i = 0; i < dfa.getNfa().size(); i++) {
                    st = dfa.getNfa().get(i);

                    if (st.getStateSet(nfa.getNfa().stream()).containsAll(set2)) {
                        found = true;
                        break;
                    }
                }

                // Not in the DFA set, add it
                if (!found) {
                    State p = new State(set2, stateId++);
                    unprocessed.addLast(p);
                    dfa.getNfa().addLast(p);
                    state.addTransition(p, symbol);

                    // Already in the DFA set
                } else {
                    state.addTransition(st, symbol);
                }
            }
        }
        return dfa.determinise();
    }

    private static void removeEpsilonTransition(Set<State> allStateSet) {
        Stack<State> stack = new Stack<>();
        set2 = set1;

        for (State st : set1) {
            stack.push(st);
        }

        while (!stack.isEmpty()) {
            State st = stack.pop();
            var epsilonStates = st.getAllTransitions(NFA.EPSILON);

            for (State p : epsilonStates) {
                if (set2.contains(p)) continue;
                set2.add(p);
                stack.push(p);
            }
        }
    }

    private static void moveStates(Character c, Set<State> states, Set<State> set) {
        for (State st : states) {
            var allStates = st.getAllTransitions(c);

            set.addAll(allStates);
        }
    }
}