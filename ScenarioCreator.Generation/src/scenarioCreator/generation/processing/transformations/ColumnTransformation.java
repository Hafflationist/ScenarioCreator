package scenarioCreator.generation.processing.transformations;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Random;
import java.util.function.Function;


public non-sealed interface ColumnTransformation extends Transformation {

    @Contract(pure = true)
    @NotNull
    Pair<List<Column>, List<TupleGeneratingDependency>> transform(Column column, Function<Integer, Id[]> idGenerator, Random random);


    @Contract(pure = true)
    @NotNull
    List<Column> getCandidates(List<Column> columnList);
}