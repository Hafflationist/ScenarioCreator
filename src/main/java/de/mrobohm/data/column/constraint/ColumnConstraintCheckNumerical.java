package de.mrobohm.data.column.constraint;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.constraint.numerical.CheckExpression;
import de.mrobohm.data.dataset.Value;

import java.util.List;

public record ColumnConstraintCheckNumerical(CheckExpression checkExpression) implements ColumnConstraint {

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        throw new RuntimeException("Not implemented!");
    }
}