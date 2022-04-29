package de.mrobohm.data.table;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.Column;

import java.util.List;

public record Table(int id,
                    String name,
                    List<Column> columnList,
                    Language language,
                    Context context,
                    List<TableConstraint> tableConstraintList) {

    public Table withId(int newId) {
        return new Table(newId, name, columnList, language, context, tableConstraintList);
    }

    public Table withName(String newName) {
        return new Table(id, newName, columnList, language, context, tableConstraintList);
    }

    public Table withColumnList(List<Column> newColumnList) {
        return new Table(id, name, newColumnList, language, context, tableConstraintList);
    }

    public Table withLanguage(Language newLanguage) {
        return new Table(id, name, columnList, newLanguage, context, tableConstraintList);
    }

    public Table withContext(Context newContext) {
        return new Table(id, name, columnList, language, newContext, tableConstraintList);
    }

    public Table withTableConstraintList(List<TableConstraint> newTableConstraints) {
        return new Table(id, name, columnList, language, context, newTableConstraints);
    }
}
