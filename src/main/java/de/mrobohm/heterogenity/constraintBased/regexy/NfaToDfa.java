package de.mrobohm.heterogenity.constraintBased.regexy;

import java.util.*;

public class NfaToDfa {
    private static Set<State> set1 = new HashSet<>();
    private static Set<State> set2 = new HashSet<>();

    public static DFA convert(NFA nfa) {
        // Creating the DFA
        DFA dfa = new DFA();

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
        removeEpsilonTransition();

        // Create the start state of DFA and add to the stack
        State dfaStart = new State(set2, stateId++);

        dfa.getDfa().addLast(dfaStart);
        unprocessed.addLast(dfaStart);

        // While there is elements in the stack
        while (!unprocessed.isEmpty()) {
            // Process and remove last state in stack
            State state = unprocessed.removeLast();

            // Check if input symbol
            for (Character symbol : NFA.inputAlphabet().toList()) {
                set1 = new HashSet<>();
                set2 = new HashSet<>();

                moveStates(symbol, state.getStateSet(), set1);
                removeEpsilonTransition();

                boolean found = false;
                State st = null;

                for (int i = 0; i < dfa.getDfa().size(); i++) {
                    st = dfa.getDfa().get(i);

                    if (st.getStateSet().containsAll(set2)) {
                        found = true;
                        break;
                    }
                }

                // Not in the DFA set, add it
                if (!found) {
                    State p = new State(set2, stateId++);
                    unprocessed.addLast(p);
                    dfa.getDfa().addLast(p);
                    state.addTransition(p, symbol);

                    // Already in the DFA set
                } else {
                    state.addTransition(st, symbol);
                }
            }
        }
        return dfa;
    }

    private static void removeEpsilonTransition() {
        Stack<State> stack = new Stack<>();
        set2 = set1;

        for (State st : set1) {
            stack.push(st);
        }

        while (!stack.isEmpty()) {
            State st = stack.pop();
            ArrayList<State> epsilonStates = st.getAllTransitions(NFA.EPSILON);

            for (State p : epsilonStates) {
                if (set2.contains(p)) continue;
                set2.add(p);
                stack.push(p);
            }
        }
    }

    private static void moveStates(Character c, Set<State> states, Set<State> set) {
        ArrayList<State> temp = new ArrayList<>(states);
        for (State st : temp) {
            ArrayList<State> allStates = st.getAllTransitions(c);

            set.addAll(allStates);
        }
    }
}