package scenarioCreator.generation.processing.transformations.contextual;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.context.UnitOfMeasure;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.Collections;
import java.util.List;
import java.util.Random;
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
    public Pair<List<Column>, List<TupleGeneratingDependency>> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        final var exception = new TransformationCouldNotBeExecutedException("Column invalid! This exception is an indicator of bad checking. This should be stopped by <getCandidates>!");
        if (!(column instanceof ColumnLeaf columnLeaf)) {
            throw exception;
        }
        final var newUnitOfMeasure = transformUnitOfMeasure(columnLeaf.context().unitOfMeasure(), random);
        final var newColumn = (Column) columnLeaf.withContext(columnLeaf.context().withUnitOfMeasure(newUnitOfMeasure));
        final var newColumnList = Collections.singletonList(newColumn);
        final List<TupleGeneratingDependency> tgdList = List.of(); // Hier werden keine benötigt, da keine strukturelle Veränderung vorliegt.
        return new Pair<>(newColumnList, tgdList);
    }

    @NotNull
    private UnitOfMeasure transformUnitOfMeasure(UnitOfMeasure unitOfMeasure, Random random) {
        final var unitOfMeasureStream = Stream.of(UnitOfMeasure.Exa,
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
        return SSet.of(
                DataTypeEnum.FLOAT16,
                DataTypeEnum.FLOAT32,
                DataTypeEnum.FLOAT64,
                DataTypeEnum.INT8,
                DataTypeEnum.INT16,
                DataTypeEnum.INT32,
                DataTypeEnum.INT64).contains(column.dataType().dataTypeEnum());
    }
}