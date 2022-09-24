package de.mrobohm.data.column.constraint;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.constraint.regexy.RegularExpression;
import de.mrobohm.data.dataset.Value;

import java.util.List;

public record ColumnConstraintCheckRegex(RegularExpression regularExpression) implements ColumnConstraint {

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        throw new RuntimeException("Not implemented!");
    }
}