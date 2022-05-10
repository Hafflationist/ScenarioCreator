package de.mrobohm.operations.contextual;

import de.mrobohm.data.DataType;
import de.mrobohm.data.column.UnitOfMeasure;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChangeUnitOfMeasure implements ColumnTransformation {

    @Override
    @NotNull
    public List<Column> transform(Column column, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Column invalid! This exception is an indicator of bad checking. This should be stopped by <getCandidates>!");
        if (!(column instanceof ColumnLeaf columnLeaf)) {
            throw exception;
        }
        var newUnitOfMeasure = transformUnitOfMeasure(columnLeaf.context().unitOfMeasure(), random);
        var newColumn = columnLeaf.withContext(columnLeaf.context().withUnitOfMeasure(newUnitOfMeasure));
        return Collections.singletonList(newColumn);
    }

    @NotNull
    private UnitOfMeasure transformUnitOfMeasure(UnitOfMeasure unitOfMeasure, Random random) {
        var unitOfMeasureStream = Stream.of(UnitOfMeasure.Exa,
                UnitOfMeasure.Tera,
                UnitOfMeasure.Giga,
                UnitOfMeasure.Mega,
                UnitOfMeasure.Kilo,
                UnitOfMeasure.Hecto,
                UnitOfMeasure.Deca,
                UnitOfMeasure.Pure,
                UnitOfMeasure.Deci,
                UnitOfMeasure.Centi,
                UnitOfMeasure.Milli,
                UnitOfMeasure.Micro,
                UnitOfMeasure.Nano,
                UnitOfMeasure.Pico);
        return StreamExtensions.pickRandomOrThrow(unitOfMeasureStream, new RuntimeException("fehler"), random);
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream()
                .filter(c -> c instanceof ColumnLeaf)
                .map(c -> (ColumnLeaf) c)
                .filter(this::hasValidUnitOfMeasure)
                .filter(this::hasValidDataType)
                .collect(Collectors.toList());
        // Wenn man konkrete Werte für diese Spalte hätte, könnte man noch prüfen, ob es zu einem Überlauf kommen könnte.
    }

    private boolean hasValidUnitOfMeasure(ColumnLeaf column) {
        return !column.context().unitOfMeasure().equals(UnitOfMeasure.None);
    }

    private boolean hasValidDataType(ColumnLeaf column) {
        return Set.of(
                DataType.FLOAT16,
                DataType.FLOAT32,
                DataType.FLOAT64,
                DataType.INT8,
                DataType.INT16,
                DataType.INT32,
                DataType.INT64).contains(column.dataType());
    }
}
