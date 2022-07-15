package de.mrobohm.transformations;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.function.Function;


public non-sealed interface ColumnTransformation extends Transformation {

    @Contract(pure = true)
    @NotNull
    List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random);


    @Contract(pure = true)
    @NotNull
    List<Column> getCandidates(List<Column> columnList);
}