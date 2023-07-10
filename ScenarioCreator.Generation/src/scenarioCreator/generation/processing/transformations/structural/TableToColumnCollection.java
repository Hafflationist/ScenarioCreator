package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.structural.base.IngestionBase;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
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
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public Pair<Schema, List<TupleGeneratingDependency>> transform(Schema schema, Random random) {
        final var newSchema = IngestionBase.fullRandomIngestion(schema, this::columnGenerator, _flags, random).first();
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        return new Pair<>(newSchema, tgdList);
    }

    private Stream<Column> columnGenerator(Table ingestedTable, boolean isNullable) {
        return Stream.of(new ColumnCollection(
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
