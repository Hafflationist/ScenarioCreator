package scenarioCreator.data.column.constraint;

import scenarioCreator.data.column.DataType;
import scenarioCreator.data.dataset.Value;
import scenarioCreator.data.identification.Id;
import org.jetbrains.annotations.Contract;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Function;

public record ColumnConstraintForeignKeyInverse(Id foreignColumnId,
                                                SortedSet<Value> foreignValueSet) implements ColumnConstraint {

    @Override
    public double estimateRatioOfKickedValues(List<Value> values, DataType dataType) {
        return switch (dataType.dataTypeEnum()) {
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

    public ColumnConstraintForeignKeyInverse withForeignColumnId(Id newForeignColumnId) {
        return new ColumnConstraintForeignKeyInverse(newForeignColumnId, foreignValueSet());
    }
}