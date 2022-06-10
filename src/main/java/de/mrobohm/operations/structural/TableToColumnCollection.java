package de.mrobohm.operations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.SchemaTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableToColumnCollection implements SchemaTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given schema does not contain suitable tables!");

        var expandableTableStream = schema.tableSet().stream()
                .filter(t -> isTableExpandable(t, schema.tableSet()));
        var chosenTable = StreamExtensions.pickRandomOrThrow(expandableTableStream, exception, random);
        var ingestionCandidates = getIngestionCandidates(chosenTable, schema.tableSet());
        var ingestingColumnStream = ingestionCandidates.keySet().stream();
        var chosenColumn = StreamExtensions.pickRandomOrThrow(ingestingColumnStream, exception, random);
        var ingestableTableStream = ingestionCandidates.get(chosenColumn).stream();
        var chosenIngestableTable = StreamExtensions.pickRandomOrThrow(ingestableTableStream, exception, random);
        var newTable = ingest(chosenTable, chosenIngestableTable);
        var newTableSet = StreamExtensions
                .replaceInStream(schema.tableSet().stream(), Stream.of(chosenTable, chosenIngestableTable), newTable)
                .collect(Collectors.toSet());
        return schema.withTables(newTableSet);
    }

    private Table ingest(Table ingestingTable, Table ingestedTable) {
        var ingestingColumnOpt = ingestingTable.columnList().stream()
                .filter(column -> column.constraintSet().stream()
                        .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                        .map(c -> (ColumnConstraintForeignKeyInverse) c)
                        .map(ColumnConstraintForeignKeyInverse::foreignColumnId)
                        .anyMatch(cid -> ingestedTable.columnList().stream()
                                .anyMatch(columnB -> columnB.constraintSet().stream()
                                        .filter(c -> c instanceof ColumnConstraintForeignKey)
                                        .anyMatch(c -> ((ColumnConstraintForeignKey) c).foreignColumnId() == cid))))
                .findFirst();

        assert ingestingColumnOpt.isPresent() :  "REEE!!";
        var ingestingColumn = ingestingColumnOpt.get();
        var newColumnCollection = new ColumnCollection(
                ingestedTable.id(),
                ingestedTable.name(),
                ingestedTable.columnList(),
                Set.of(),
                false
        );
        var newColumnList = StreamExtensions
                .replaceInStream(ingestingTable.columnList().stream(), ingestingColumn, newColumnCollection)
                .toList();
        return ingestedTable.withColumnList(newColumnList);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        var tableSet = schema.tableSet();
        return tableSet.stream().anyMatch(t -> isTableExpandable(t, tableSet));
    }

    private boolean isTableExpandable(Table table, Set<Table> tableSet) {
        var ingestionCandidates = getIngestionCandidates(table, tableSet);
        return ingestionCandidates.keySet().size() > 0;
    }

    private Map<Column, Set<Table>> getIngestionCandidates(Table table, Set<Table> tableSet) {
        var ingestionCandidates = table.columnList().stream().collect(Collectors.toMap(
                Function.identity(),
                // All tables pointing to <table> could be ingested by <table> into a collection
                column -> column.constraintSet().stream()
                        .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                        .map(c -> ((ColumnConstraintForeignKeyInverse) c).foreignColumnId())
                        .map(cid -> columnIdToTable(cid, tableSet))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(t -> hasSimpleRelationship(table, t))
                        .collect(Collectors.toSet())));

        return table.columnList().stream()
                .filter(column -> ingestionCandidates.get(column).size() > 0)
                .collect(Collectors.toMap(Function.identity(), ingestionCandidates::get));
    }


    private Optional<Table> columnIdToTable(int columnId, Set<Table> tableSet) {
        return tableSet.stream().filter(t -> t.columnList().stream().anyMatch(c -> c.id() == columnId)).findFirst();
    }

    private boolean hasSimpleRelationship(Table tableA, Table tableB) {
        var constraints = tableA.columnList().stream()
                .flatMap(column -> column.constraintSet().stream())
                .toList();
        var foreignColumnIdSet1 = constraints.stream()
                .filter(c -> c instanceof ColumnConstraintForeignKey)
                .map(c -> (ColumnConstraintForeignKey) c)
                .map(ColumnConstraintForeignKey::foreignColumnId);
        var foreignColumnIdSet2 = constraints.stream()
                .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                .map(c -> (ColumnConstraintForeignKeyInverse) c)
                .map(ColumnConstraintForeignKeyInverse::foreignColumnId);
        var connectionCount = Stream.concat(foreignColumnIdSet1, foreignColumnIdSet2)
                .distinct()
                .map(cid -> columnIdToTable(cid, Set.of(tableB)))
                .filter(Optional::isPresent)
                .count();
        return connectionCount <= 1;
    }
}