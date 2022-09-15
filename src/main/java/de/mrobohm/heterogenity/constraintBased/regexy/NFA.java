package de.mrobohm.heterogenity.constraintBased.regexy;

import de.mrobohm.utils.Pair;

import java.util.HashSet;
import java.util.Stack;

public class NFA {

    //  Supported features:
    //  - kleen star *
    //  - coproduct (|)
    //  - precedence (())
    //  Example inputs:
    // "(A*B|AC)D"
    // "(A*B|AC)D"
    // "(a|(bc)*d)*"
    // "(a|(bc)*d)*"
    public static DirectedGraph regexToNfaGraph(String regex) {
        final var ops = new Stack<Integer>();
        final var edges = new HashSet<Pair<Integer, Integer>>();
        for (var i = 0; i < regex.length(); i++) {
            int lp = i;
            if (regex.charAt(i) == '(' || regex.charAt(i) == '|') {
                ops.push(i);
            } else if (regex.charAt(i) == ')') {
                int or = ops.pop();

                // 2-way or operator
                if (regex.charAt(or) == '|') {
                    lp = ops.pop();
                    edges.add(new Pair<>(lp, or + 1));
                    edges.add(new Pair<>(or, i));
                } else if (regex.charAt(or) == '(') {
                    lp = or;
                } else {
                    assert false;
                }
            }

            // closure op (uses one step lookahead)
            if (i < regex.length() - 1 && regex.charAt(i + 1) == '*') {
                edges.add(new Pair<>(lp, i + 1));
                edges.add(new Pair<>(i + 1, lp));
            }
            if (regex.charAt(i) == '(' || regex.charAt(i) == '*' || regex.charAt(i) == ')') {
                edges.add(new Pair<>(i, i + 1));
            }
        }
        if (ops.size() != 0) {
            throw new IllegalArgumentException("Invalid regular expression");
        }
        return new DirectedGraph(edges);
    }
}