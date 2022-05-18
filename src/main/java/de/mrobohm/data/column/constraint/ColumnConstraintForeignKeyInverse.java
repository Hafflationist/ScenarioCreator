package de.mrobohm.data.column.constraint;

import de.mrobohm.data.DataType;
import de.mrobohm.data.dataset.Value;
import org.jetbrains.annotations.Contract;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public record ColumnConstraintForeignKeyInverse(int foreignColumnId,
                                                Set<Value> foreignValueSet) implements ColumnConstraint {

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        return switch (dataType) {
            case FLOAT16, FLOAT32, FLOAT64 -> kickedRatio(values, Double::valueOf);
            case DECIMAL -> kickedRatio(values, BigDecimal::new);
            case INT1 -> kickedRatio(values, Boolean::valueOf);
            case INT8, INT16, INT32, INT64 -> kickedRatio(values, Integer::valueOf);
            case DATETIME -> kickedRatio(values, Date::valueOf);
            case NVARCHAR -> kickedRatio(values, Function.identity());
        };
    }

    @Contract(pure = true)
    private <T> double kickedRatio(List<Value> values, Function<String, T> converter) {
        return 0;
    }
}