package de.mrobohm.operations;

import de.mrobohm.data.column.nesting.Column;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;


public interface ColumnTransformation {

    @Contract(pure = true)
    boolean conservesFlatRelations();

    @Contract(pure = true)
    @NotNull
    List<Column> transform(Column column, Random random);


    @Contract(pure = true)
    @NotNull
    List<Column> getCandidates(List<Column> columnList);
}