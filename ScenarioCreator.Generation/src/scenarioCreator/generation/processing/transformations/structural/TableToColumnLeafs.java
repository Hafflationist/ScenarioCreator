package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.structural.base.IngestionBase;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

// equivalent to vertical merge
// equivalent to flattening of vertical inheritance
public class TableToColumnLeafs implements SchemaTransformation {

    private final IngestionBase.IngestionFlags _flags;

    public TableToColumnLeafs(boolean shouldStayNormalized, boolean shouldConserveAllRecords) {
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
    public Pair<Schema, List<TupleGeneratingDependency>> transform(Schema schema, Random random) {
        // table name could be updated...
        final var newSchema = IngestionBase.fullRandomIngestion(
                schema, this::columnGenerator, _flags, random
        );
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        return new Pair<>(newSchema, tgdList);
    }

    private Stream<Column> columnGenerator(Table ingestedTable, boolean isNullable) {
        return ingestedTable.columnList().stream().map(column -> switch (column) {
            case ColumnLeaf leaf -> leaf.withDataType(leaf.dataType().withIsNullable(isNullable));
            case ColumnNode node -> node.withIsNullable(isNullable);
            case ColumnCollection col -> col.withIsNullable(isNullable);
        });
    }

    @Override
    public boolean isExecutable(Schema schema) {
        final var tableSet = schema.tableSet();
        return tableSet.stream().anyMatch(t -> IngestionBase.canIngest(t, tableSet, _flags));
    }
}