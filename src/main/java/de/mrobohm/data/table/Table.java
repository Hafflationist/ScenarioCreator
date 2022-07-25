package de.mrobohm.data.table;

import de.mrobohm.data.Context;
import de.mrobohm.data.Entity;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.SortedSet;

public record Table(Id id,
                    StringPlus name,
                    List<Column> columnList,
                    Context context,
                    SortedSet<TableConstraint> tableConstraintSet) implements Entity {

    @Contract(pure = true)
    @NotNull
    public Table withId(Id newId) {
        return new Table(newId, name, columnList, context, tableConstraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public Table withName(StringPlus newName) {
        return new Table(id, newName, columnList, context, tableConstraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public Table withColumnList(List<Column> newColumnList) {
        return new Table(id, name, newColumnList, context, tableConstraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public Table withContext(Context newContext) {
        return new Table(id, name, columnList, newContext, tableConstraintSet);
    }

    @Contract(pure = true)
    @NotNull
    public Table withTableConstraintSet(SortedSet<TableConstraint> newTableConstraints) {
        return new Table(id, name, columnList, context, newTableConstraints);
    }
}
