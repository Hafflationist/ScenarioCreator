package de.mrobohm.processing.transformations.contextual;

import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.UnitOfMeasure;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.Id;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.ColumnTransformation;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChangeUnitOfMeasure implements ColumnTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
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
                DataTypeEnum.FLOAT16,
                DataTypeEnum.FLOAT32,
                DataTypeEnum.FLOAT64,
                DataTypeEnum.INT8,
                DataTypeEnum.INT16,
                DataTypeEnum.INT32,
                DataTypeEnum.INT64).contains(column.dataType().dataTypeEnum());
    }
}