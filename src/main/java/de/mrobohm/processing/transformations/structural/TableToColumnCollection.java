package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.SchemaTransformation;
import de.mrobohm.processing.transformations.structural.base.IngestionBase;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;


public class TableToColumnCollection implements SchemaTransformation {

    private final IngestionBase.IngestionFlags _flags;

    public TableToColumnCollection(boolean shouldConserveAllRecords) {
        _flags = new IngestionBase.IngestionFlags(false, shouldConserveAllRecords);
    }

    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        return IngestionBase.fullRandomIngestion(schema, this::columnGenerator, _flags, random);
    }

    private Stream<Column> columnGenerator(Table ingestedTable, boolean isNullable) {
        return Stream.of(new ColumnCollection(
                ingestedTable.id(),
                ingestedTable.name(),
                ingestedTable.columnList(),
                Set.of(),
                isNullable
        ));
    }

    @Override
    public boolean isExecutable(Schema schema) {
        var tableSet = schema.tableSet();
        return tableSet.stream().anyMatch(t -> IngestionBase.canIngest(t, tableSet, _flags));
    }
}