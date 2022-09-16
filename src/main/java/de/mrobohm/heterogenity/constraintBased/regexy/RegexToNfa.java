package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.PersistentStack;

import java.util.*;

public class RegexToNfa {

    public static NFA generateNFA(String preRegular) {
        // Generate regular expression with the concatenation
        final var regular = AddConcat(preRegular);

        // Cleaning stacks
        var incs = IntermediateNfaCreationState.create();
        final var operator = new Stack<Character>();

        for (int i = 0; i < regular.length(); i++) {

            if (NFA.isInputCharacter(regular.charAt(i))) {
                incs = pushStack(incs, regular.charAt(i));

            } else if (operator.isEmpty()) {
                operator.push(regular.charAt(i));

            } else if (regular.charAt(i) == '(') {
                operator.push(regular.charAt(i));

            } else if (regular.charAt(i) == ')') {
                while (operator.get(operator.size() - 1) != '(') {
                    incs = doOperation(incs, operator.pop()).get();
                }

                // Pop the '(' left parenthesis
                operator.pop();

            } else {
                while (!operator.isEmpty() &&
                        priority(regular.charAt(i), operator.get(operator.size() - 1))) {
                    incs = doOperation(incs, operator.pop()).get();
                }
                operator.push(regular.charAt(i));
            }
        }

        // Clean the remaining elements in the stack
        while (!operator.isEmpty()) {
            incs = doOperation(incs, operator.pop()).get();
        }

        // Get the complete nfa
        NFA completeNfa = incs.nfaStack.peek().get();

        // add the accpeting state to the end of NFA
        completeNfa.getNfa().get(completeNfa.getNfa().size() - 1).setAcceptState(true);

        // return the nfa
        return completeNfa;
    }

    private static boolean priority(char first, Character second) {
        if (first == second) {
            return true;
        }
        if (first == '*') {
            return false;
        }
        if (second == '*') {
            return true;
        }
        if (first == '.') {
            return false;
        }
        if (second == '.') {
            return true;
        }
        return first != '|';
    }

    private static Optional<IntermediateNfaCreationState> doOperation(IntermediateNfaCreationState incs, char operator) {
        return switch (operator) {
            case ('|') -> union(incs);
            case ('.') -> concatenation(incs);
            case ('*') -> star(incs);
            default -> {
                System.out.println("Unkown symbol! (" + operator + ")");
                System.exit(1);
                yield Optional.empty();
            }
        };
    }

    private static Optional<IntermediateNfaCreationState> star(IntermediateNfaCreationState incs) {
        var nfaStack = incs.nfaStack;
        return nfaStack.peek().map(nfa -> {
            assert nfaStack.pop().isPresent() : "implementation of stack buggy!";

            // Create states for star operation
            final var start = new State(incs.lastUsedStateId + 1);
            final var end = new State(incs.lastUsedStateId + 2);

            // Add transition to start and end state
            start.addTransition(end, 'e');
            start.addTransition(nfa.getNfa().getFirst(), 'e');

            nfa.getNfa().getLast().addTransition(end, 'e');
            nfa.getNfa().getLast().addTransition(nfa.getNfa().getFirst(), 'e');

            nfa.getNfa().addFirst(start);
            nfa.getNfa().addLast(end);

            // Put nfa back in the nfaStack
            return incs
                    .withNfaStack(nfaStack.pop().get().push(nfa))
                    .withLastUsedStateId(incs.lastUsedStateId + 2);
        });
    }

    private static Optional<IntermediateNfaCreationState> concatenation(IntermediateNfaCreationState incs) {
        final var nfaStack = incs.nfaStack;
        return nfaStack.peek().flatMap(nfa2 -> {
            assert nfaStack.pop().isPresent() : "implementation of stack buggy!";
            return nfaStack.pop().get().peek().flatMap(nfa1 -> {
                assert nfaStack.pop().isPresent() : "implementation of stack buggy!";

                // Add transition to the end of nfa 1 to the beginning of nfa 2
                // the transition uses empty string
                nfa1.getNfa().getLast().addTransition(nfa2.getNfa().getFirst(), 'e');

                // Add all states in nfa2 to the end of nfa1
                for (State s : nfa2.getNfa()) {
                    nfa1.getNfa().addLast(s);
                }

                return nfaStack.pop().get().pop().map(ps -> incs
                        .withNfaStack(ps.push(nfa1))
                );
            });
        });
    }

    private static Optional<IntermediateNfaCreationState> union(IntermediateNfaCreationState incs) {
        // Load two NFA in stack into variables
        final var nfaStack = incs.nfaStack;
        return nfaStack.peek().flatMap(nfa2 -> {
            assert nfaStack.pop().isPresent() : "implementation of stack buggy!";
            return nfaStack.pop().get().peek().flatMap(nfa1 -> {
                assert nfaStack.pop().isPresent() : "implementation of stack buggy!";

                State start = new State(incs.lastUsedStateId + 1);
                State end = new State(incs.lastUsedStateId + 2);

                start.addTransition(nfa1.getNfa().getFirst(), NFA.EPSILON);
                start.addTransition(nfa2.getNfa().getFirst(), NFA.EPSILON);

                // Set transition to the end of each subNfa with empty string
                nfa1.getNfa().getLast().addTransition(end, NFA.EPSILON);
                nfa2.getNfa().getLast().addTransition(end, NFA.EPSILON);

                // Add start to the end of each nfa
                nfa1.getNfa().addFirst(start);
                nfa2.getNfa().addLast(end);

                // Add all states in nfa2 to the end of nfa1
                // in order
                for (State s : nfa2.getNfa()) {
                    nfa1.getNfa().addLast(s);
                }
                // Put NFA back to stack
                return nfaStack.pop().get().pop().map(ps -> incs
                        .withNfaStack(ps.push(nfa1))
                        .withLastUsedStateId(incs.lastUsedStateId + 2)
                );
            });
        });
    }

    private static IntermediateNfaCreationState pushStack(IntermediateNfaCreationState incs, char symbol) {
        State s0 = new State(incs.lastUsedStateId + 1);
        State s1 = new State(incs.lastUsedStateId + 2);

        // add transition from 0 to 1 with the symbol
        s0.addTransition(s1, symbol);

        // new temporary NFA
        NFA nfa = new NFA();
        nfa.getNfa().addLast(s0);
        nfa.getNfa().addLast(s1);

        // Put NFA back to nfaStack
        var newNfaStack = incs.nfaStack.push(nfa);
        return incs
                .withNfaStack(newNfaStack)
                .withLastUsedStateId(incs.lastUsedStateId + 2);
    }

    // add "." when is concatenation between 2 symbols that
    // concatenates to each other
    private static String AddConcat(String regular) {
        String newRegular = "";

        for (int i = 0; i < regular.length() - 1; i++) {
            if (NFA.isInputCharacter(regular.charAt(i)) && NFA.isInputCharacter(regular.charAt(i + 1))) {
                newRegular += regular.charAt(i) + ".";

            } else if (NFA.isInputCharacter(regular.charAt(i)) && regular.charAt(i + 1) == '(') {
                newRegular += regular.charAt(i) + ".";

            } else if (regular.charAt(i) == ')' && NFA.isInputCharacter(regular.charAt(i + 1))) {
                newRegular += regular.charAt(i) + ".";

            } else if (regular.charAt(i) == '*' && NFA.isInputCharacter(regular.charAt(i + 1))) {
                newRegular += regular.charAt(i) + ".";

            } else if (regular.charAt(i) == '*' && regular.charAt(i + 1) == '(') {
                newRegular += regular.charAt(i) + ".";

            } else if (regular.charAt(i) == ')' && regular.charAt(i + 1) == '(') {
                newRegular += regular.charAt(i) + ".";

            } else {
                newRegular += regular.charAt(i);
            }
        }
        newRegular += regular.charAt(regular.length() - 1);
        return newRegular;
    }

    private record IntermediateNfaCreationState(PersistentStack<NFA> nfaStack, int lastUsedStateId) {

        static IntermediateNfaCreationState create() {
            return new IntermediateNfaCreationState(PersistentStack.getEmpty(), -1);
        }

        IntermediateNfaCreationState withNfaStack(PersistentStack<NFA> newNfaStack) {
            return new IntermediateNfaCreationState(newNfaStack, lastUsedStateId);
        }

        IntermediateNfaCreationState withLastUsedStateId(int newLastUsedStateId) {
            return new IntermediateNfaCreationState(nfaStack, newLastUsedStateId);
        }
    }
}