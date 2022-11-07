package scenarioCreator.generation.processing.transformations;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;

import java.util.Random;
import java.util.SortedSet;
import java.util.function.Function;

public non-sealed interface TableTransformation extends Transformation  {

    @Contract(pure = true)
    @NotNull
    SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random);


    @Contract(pure = true)
    @NotNull
    SortedSet<Table> getCandidates(SortedSet<Table> tableSet);
}