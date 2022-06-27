package de.mrobohm.data.column.constraint;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.dataset.Value;

import java.util.List;

public final class ColumnConstraintLocalPredicate implements ColumnConstraint {

    // TODO: Hier wird problematisch, wie man genau diese Prädikäte darstellt.
    // Bei den Prädikaten stelle ich mir folgendes vor:
    // x -> x > 100
    // x -> x % 2 == 0
    // x -> x LIKE "%hugo%"
    // Die verschiedenen Typen erschweren eine einheitliche Darstellung.

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        return 0;
    }
}
