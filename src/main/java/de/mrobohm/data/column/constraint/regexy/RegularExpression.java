package de.mrobohm.data.column.constraint.regexy;

public sealed interface RegularExpression permits RegularConcatenation, RegularKleene, RegularTerminal, RegularSum {
    default String toStringWithParentheses() {
        return switch (this) {
            case RegularTerminal ignore -> this.toString();
            default -> "(" + this + ")";
        };
    }
}