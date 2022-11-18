package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.structural.base.IngestionBase;
import scenarioCreator.utils.SSet;

import java.util.Random;
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
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        return IngestionBase.fullRandomIngestion(
                schema, this::columnGenerator, _flags, random
        );
    }

    private Stream<Column> columnGenerator(Table ingestedTable, boolean isNullable) {
        return Stream.of(new ColumnNode(
                ingestedTable.id(),
                ingestedTable.name(),
                ingestedTable.columnList(),
                SSet.of(),
                isNullable
        ));
    }

    @Override
    public boolean isExecutable(Schema schema) {
        final var tableSet = schema.tableSet();
        return tableSet.stream().anyMatch(t -> IngestionBase.canIngest(t, tableSet, _flags));
    }
}