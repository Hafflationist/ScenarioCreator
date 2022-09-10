package de.mrobohm.heterogenity.constraintBased.regexy;
/******************************************************************************
 *  Compilation:  javac NFA.java
 *  Execution:    java NFA regexp text
 *  Dependencies: Stack.java List.java DirectedGraph.java DirectedDepthFirstSearch.java
 *
 *  % java NFA "(A*B|AC)D" AAAABD
 *  true
 *
 *  % java NFA "(A*B|AC)D" AAAAC
 *  false
 *
 *  % java NFA "(a|(bc)*d)*" abcbcd
 *  true
 *
 *  % java NFA "(a|(bc)*d)*" abcbcbcdaaaabcbcdaaaddd
 *  true
 *
 *  Remarks
 *  -----------
 *  The following features are not supported:
 *    - The + operator
 *    - Multiway or
 *    - Metacharacters in the text
 *    - Character classes.
 *
 ******************************************************************************/

import java.util.Stack;

public class NFA {

    private final DirectedGraph graph;     // digraph of epsilon transitions
    private final String regexp;     // regular expression
    private final int m;       // number of characters in regular expression

    /**
     * Initializes the NFA from the specified regular expression.
     *
     * @param  regexp the regular expression
     */
    public NFA(String regexp) {
        this.regexp = regexp;
        m = regexp.length();
        Stack<Integer> ops = new Stack<Integer>();
        graph = new DirectedGraph(m+1);
        for (int i = 0; i < m; i++) {
            int lp = i;
            if (regexp.charAt(i) == '(' || regexp.charAt(i) == '|')
                ops.push(i);
            else if (regexp.charAt(i) == ')') {
                int or = ops.pop();

                // 2-way or operator
                if (regexp.charAt(or) == '|') {
                    lp = ops.pop();
                    graph.addEdge(lp, or+1);
                    graph.addEdge(or, i);
                }
                else if (regexp.charAt(or) == '(')
                    lp = or;
                else assert false;
            }

            // closure operator (uses 1-character lookahead)
            if (i < m-1 && regexp.charAt(i+1) == '*') {
                graph.addEdge(lp, i+1);
                graph.addEdge(i+1, lp);
            }
            if (regexp.charAt(i) == '(' || regexp.charAt(i) == '*' || regexp.charAt(i) == ')')
                graph.addEdge(i, i+1);
        }
        if (ops.size() != 0)
            throw new IllegalArgumentException("Invalid regular expression");
    }
}