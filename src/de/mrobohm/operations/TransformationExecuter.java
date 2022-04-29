package de.mrobohm.operations;


import de.mrobohm.data.Schema;
import de.mrobohm.data.table.Table;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformationExecuter {
    public Schema executeTransformation(Schema schema, Function<Schema, Schema> transformation) {
        return transformation.apply(schema);
    }

    public Schema executeTransformationTable(Schema schema, Function<Table, Set<Table>> transformation) {
        var targetTable = chooseTable(schema);
        var newTableSet = transformation.apply(targetTable);
        var filteredTableStream = schema.tableSet()
                .stream()
                .filter(table -> !table.equals(targetTable));
        var newTableStream = Stream.concat(filteredTableStream, newTableSet.stream());
        return schema.withTables(newTableStream.collect(Collectors.toUnmodifiableSet()));
    }


    private Table chooseTable(Schema schema) {
        // Hier soll eine Tabelle aus dem Schema gewählt werden, die bearbeitet werden soll.
        return null;
    }
}
