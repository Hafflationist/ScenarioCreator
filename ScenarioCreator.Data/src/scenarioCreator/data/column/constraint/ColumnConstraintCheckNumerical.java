package scenarioCreator.data.column.constraint;

import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.constraint.numerical.CheckExpression;
import scenarioCreator.data.dataset.Value;

import java.util.List;

public record ColumnConstraintCheckNumerical(CheckExpression checkExpression) implements ColumnConstraint {

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        throw new RuntimeException("Not implemented!");
    }
}