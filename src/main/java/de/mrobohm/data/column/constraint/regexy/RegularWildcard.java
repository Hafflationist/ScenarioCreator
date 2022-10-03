package de.mrobohm.data.column.constraint.regexy;

public record RegularWildcard() implements RegularExpression {

    @Override
    public String toString() {
        return "#";
    }
}