package de.mrobohm.data.column.constraint;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.dataset.Value;

import java.util.List;

public record ColumnConstraintCheckNumerical(ComparisonType comparisonType, double value) implements ColumnConstraint {

    enum ComparisonType {
        LowerThan, LowerEquals, GreaterEquals, GreaterThan
    }

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        return 0;
    }
}