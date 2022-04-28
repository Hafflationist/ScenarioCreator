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
}
