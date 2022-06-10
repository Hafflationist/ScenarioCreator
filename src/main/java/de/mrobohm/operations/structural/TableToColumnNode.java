package de.mrobohm.operations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.SchemaTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.IngestionBase;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

public class TableToColumnNode implements SchemaTransformation {

    private final IngestionBase.IngestionFlags _flags;

    public TableToColumnNode(boolean shouldStayNormalized, boolean shouldConserveAllRecords) {
        _flags = new IngestionBase.IngestionFlags(shouldStayNormalized, shouldConserveAllRecords);
    }


    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given schema does not contain suitable tables!");
        return IngestionBase.fullRandomIngestion(
                schema, this::columnGenerator, _flags, exception, random
        );
    }

    private Stream<Column> columnGenerator(Table ingestedTable, boolean isNullable) {
        return Stream.of(new ColumnNode(
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