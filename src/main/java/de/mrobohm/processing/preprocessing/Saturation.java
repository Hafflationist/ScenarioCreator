package de.mrobohm.processing.preprocessing;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Saturation {
    private Saturation() {
    }


    public static Schema saturate(Schema schema,
                                  Function<Schema, Schema> saturateSchema,
                                  Function<Table, Table> saturateTable,
                                  Function<Column, Column> saturateColumn) {
        var newSchema = saturateSchema.apply(schema);
        return maybeWithTableSet(newSchema, newSchema.tableSet()
                .stream()
                .map(saturateTable)
                .map(t -> maybeWithColumnList(t, t.columnList().stream().map(saturateColumn).toList()))
                .collect(Collectors.toCollection(TreeSet::new)));
    }

    private static Schema maybeWithTableSet(Schema schema, SortedSet<Table> tableSet) {
        if (schema.tableSet().equals(tableSet)) {
            return schema;
        } else {
            return schema.withTables(tableSet);
        }
    }

    private static Table maybeWithColumnList(Table table, List<Column> columnList) {
        if (table.columnList().equals(columnList)) {
            return table;
        } else {
            return table.withColumnList(columnList);
        }
    }
}