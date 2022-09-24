package de.mrobohm.data.column.constraint.regexy;

public record RegularSum(RegularExpression expression1, RegularExpression expression2) implements RegularExpression {
    @Override
    public String toString() {
        return expression1.toStringWithParentheses() + "|" + expression2.toStringWithParentheses();
    }
}