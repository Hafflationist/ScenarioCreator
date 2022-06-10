package de.mrobohm.operations;

import de.mrobohm.data.column.nesting.Column;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.function.Function;


public interface ColumnTransformation extends Transformation {

    @Contract(pure = true)
    boolean conservesFlatRelations();

    @Contract(pure = true)
    @NotNull
    List<Column> transform(Column column, Function<Integer, int[]> idGenerator, Random random);


    @Contract(pure = true)
    @NotNull
    List<Column> getCandidates(List<Column> columnList);
}