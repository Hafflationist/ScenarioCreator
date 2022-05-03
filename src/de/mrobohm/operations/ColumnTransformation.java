package de.mrobohm.operations;

import de.mrobohm.data.column.nesting.Column;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;


public interface ColumnTransformation {

    @Contract(pure = true)
    @NotNull
    List<Column> transform(Column column);


    @Contract(pure = true)
    @NotNull
    List<Column> getCandidates(List<Column> tableSet);
}