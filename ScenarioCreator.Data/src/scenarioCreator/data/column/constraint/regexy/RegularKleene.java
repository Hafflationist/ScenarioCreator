package scenarioCreator.data.column.constraint.regexy;

public record RegularKleene(RegularExpression expression) implements RegularExpression {

    @Override
    public String toString() {
        return  expression().toStringWithParentheses() + "*";
    }
}