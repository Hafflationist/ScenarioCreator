package de.mrobohm.data.table;

import de.mrobohm.data.Context;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public record Table(int id,
                    StringPlus name,
                    List<Column> columnList,
                    Context context,
                    Set<TableConstraint> tableConstraintList) {

    @Contract(pure = true)
    @NotNull
    public Table withId(int newId) {
        return new Table(newId, name, columnList, context, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withName(StringPlus newName) {
        return new Table(id, newName, columnList, context, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withColumnList(List<Column> newColumnList) {
        return new Table(id, name, newColumnList, context, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withContext(Context newContext) {
        return new Table(id, name, columnList, newContext, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withTableConstraintList(Set<TableConstraint> newTableConstraints) {
        return new Table(id, name, columnList, context, newTableConstraints);
    }
}
