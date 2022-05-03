package de.mrobohm.data.table;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.nesting.Column;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public record Table(int id,
                    String name,
                    List<Column> columnList,
                    Language language,
                    Context context,
                    List<TableConstraint> tableConstraintList) {

    @Contract(pure = true)
    @NotNull
    public Table withId(int newId) {
        return new Table(newId, name, columnList, language, context, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withName(String newName) {
        return new Table(id, newName, columnList, language, context, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withColumnList(List<Column> newColumnList) {
        return new Table(id, name, newColumnList, language, context, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withLanguage(Language newLanguage) {
        return new Table(id, name, columnList, newLanguage, context, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withContext(Context newContext) {
        return new Table(id, name, columnList, language, newContext, tableConstraintList);
    }

    @Contract(pure = true)
    @NotNull
    public Table withTableConstraintList(List<TableConstraint> newTableConstraints) {
        return new Table(id, name, columnList, language, context, newTableConstraints);
    }
}
