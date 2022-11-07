package scenarioCreator.data.column.constraint;

import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.constraint.regexy.RegularExpression;
import scenarioCreator.data.dataset.Value;

import java.util.List;

public record ColumnConstraintCheckRegex(RegularExpression regularExpression) implements ColumnConstraint {

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        throw new RuntimeException("Not implemented!");
    }
}