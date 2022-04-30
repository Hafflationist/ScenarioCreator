package de.mrobohm.operations;

import de.mrobohm.data.column.Column;
import de.mrobohm.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;


public interface ColumnTransformation {

    @Contract(pure = true)
    @NotNull
    List<Column> transform(Column column);


    @Contract(pure = true)
    @NotNull
    List<Column> getCandidates(List<Column> tableSet);
}