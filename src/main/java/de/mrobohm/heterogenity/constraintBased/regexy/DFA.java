package de.mrobohm.heterogenity.constraintBased.regexy;

import java.util.LinkedList;
import java.util.Objects;

public class DFA {
    private final LinkedList<State> dfa = new LinkedList<> ();

    public DFA () {
    }

    public LinkedList<State> getDfa() {
        return dfa;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DFA dfa1 = (DFA) o;
        return Objects.equals(dfa, dfa1.dfa);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dfa);
    }

    @Override
    public String toString() {
        return "DFA{" + "dfa=" + dfa + '}';
    }
}