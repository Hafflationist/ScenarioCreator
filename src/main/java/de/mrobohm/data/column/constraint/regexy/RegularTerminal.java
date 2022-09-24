package de.mrobohm.data.column.constraint.regexy;

public record RegularTerminal(char terminal) implements RegularExpression {
    @Override
    public String toString() {
        return Character.toString(terminal);
    }
}