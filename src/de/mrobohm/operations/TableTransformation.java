package de.mrobohm.operations;

import de.mrobohm.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public interface TableTransformation {

    @Contract(pure = true)
    @NotNull
    Set<Table> transform(Table table);


    @Contract(pure = true)
    @NotNull
    Set<Table> getCandidates(Set<Table> tableSet);
}