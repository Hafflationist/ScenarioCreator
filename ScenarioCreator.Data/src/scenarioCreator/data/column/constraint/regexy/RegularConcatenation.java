package scenarioCreator.data.column.constraint.regexy;

public record RegularConcatenation(RegularExpression expression1, RegularExpression expression2) implements RegularExpression {
    @Override
    public String toString() {
        return expression1.toString() + expression2.toString();
    }
}