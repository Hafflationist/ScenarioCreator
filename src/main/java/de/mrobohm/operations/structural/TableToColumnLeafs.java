package de.mrobohm.operations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.SchemaTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.IngestionBase;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.stream.Stream;

// equivalent to vertical merge
public class TableToColumnLeafs implements SchemaTransformation {

    private final boolean _shouldStayNormalized;

    public TableToColumnLeafs(boolean shouldStayNormalized) {
        _shouldStayNormalized = shouldStayNormalized;
    }


    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given schema does not contain suitable tables!");
        // table name could be updated...
        return IngestionBase.fullRandomIngestion(
                schema, this::columnGenerator, _shouldStayNormalized, exception, random
        );
    }

    private Stream<Column> columnGenerator(Table ingestedTable) {
        return ingestedTable.columnList().stream();
    }

    @Override
    public boolean isExecutable(Schema schema) {
        var tableSet = schema.tableSet();
        return tableSet.stream().anyMatch(t -> IngestionBase.canIngest(t, tableSet, _shouldStayNormalized));
    }
}