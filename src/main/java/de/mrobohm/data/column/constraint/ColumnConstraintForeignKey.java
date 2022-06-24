package de.mrobohm.data.column.constraint;

import de.mrobohm.data.DataType;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.Id;
import org.jetbrains.annotations.Contract;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public record ColumnConstraintForeignKey(Id foreignColumnId, Set<Value> foreignValueSet) implements ColumnConstraint {

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
        var convertedForeignValueSet = foreignValueSet
                .stream()
                .map(v -> converter.apply(v.content()))
                .collect(Collectors.toSet());
        var obeyingValuesCount = values
                .stream()
                .map(v -> converter.apply(v.content()))
                .filter(convertedForeignValueSet::contains)
                .count();

        if (obeyingValuesCount == 0){
            return 0;
        }

        var valuesCount = values.size();
        return (double) (valuesCount - obeyingValuesCount) / valuesCount;
    }

    public ColumnConstraintForeignKey withForeignColumnId(Id newForeignColumnId) {
        return new ColumnConstraintForeignKey(newForeignColumnId, foreignValueSet());
    }
}