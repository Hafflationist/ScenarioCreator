package de.mrobohm.processing.transformations;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;

public non-sealed interface TableTransformation extends Transformation  {

    @Contract(pure = true)
    @NotNull
    Set<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random);


    @Contract(pure = true)
    @NotNull
    Set<Table> getCandidates(Set<Table> tableSet);
}