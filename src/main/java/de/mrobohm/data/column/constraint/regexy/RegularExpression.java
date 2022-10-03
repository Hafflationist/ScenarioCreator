package de.mrobohm.data.column.constraint.regexy;

public sealed interface RegularExpression
        permits
        RegularConcatenation,
        RegularKleene,
        RegularTerminal,
        RegularSum,
        RegularWildcard {
    default String toStringWithParentheses() {
        return switch (this) {
            case RegularTerminal ignore -> this.toString();
            case RegularWildcard ignore -> this.toString();
            default -> "(" + this + ")";
        };
    }

    static RegularExpression acceptsEverything() {
        return new RegularKleene(new RegularWildcard());
    }
}