package de.mrobohm.operations;

import de.mrobohm.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;

public interface TableTransformation {

    @Contract(pure = true)
    boolean conservesFlatRelations();

    @Contract(pure = true)
    @NotNull
    Set<Table> transform(Table table, Set<Table> otherTableSet, Function<Integer, int[]> idGenerator, Random random);


    @Contract(pure = true)
    @NotNull
    Set<Table> getCandidates(Set<Table> tableSet);
}