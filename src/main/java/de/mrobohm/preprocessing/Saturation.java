package de.mrobohm.preprocessing;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;

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
        return newSchema.withTables(newSchema.tableSet()
                .stream()
                .map(saturateTable)
                .map(t -> t.withColumnList(t.columnList().stream().map(saturateColumn).toList()))
                .collect(Collectors.toSet()));
    }
}