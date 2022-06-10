package de.mrobohm.operations.structural.base;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class IngestionBase {
    private IngestionBase() {
    }

    public static <TException extends Throwable> Schema fullRandomIngestion(
            Schema schema,
            Function<Table, Stream<Column>> columnGenerator,
            boolean insistOnOneToOne,
            TException ex,
            Random random) throws TException {
        var expandableTableStream = schema.tableSet().stream()
                .filter(t -> IngestionBase.canIngest(t, schema.tableSet(), insistOnOneToOne));
        var chosenTable = StreamExtensions.pickRandomOrThrow(expandableTableStream, ex, random);
        var ingestionCandidates = IngestionBase.getIngestionCandidates(chosenTable, schema.tableSet(), insistOnOneToOne);
        var ingestingColumnStream = ingestionCandidates.keySet().stream();
        var chosenColumn = StreamExtensions.pickRandomOrThrow(ingestingColumnStream, ex, random);
        var ingestableTableStream = ingestionCandidates.get(chosenColumn).stream();
        var chosenIngestableTable = StreamExtensions.pickRandomOrThrow(ingestableTableStream, ex, random);
        var newTable = ingest(chosenTable, chosenIngestableTable, columnGenerator);
        var newTableSet = StreamExtensions
                .replaceInStream(schema.tableSet().stream(), Stream.of(chosenTable, chosenIngestableTable), newTable)
                .collect(Collectors.toSet());
        return schema.withTables(newTableSet);
    }


    public static Table ingest(Table ingestingTable, Table ingestedTable, Function<Table, Stream<Column>> columnGenerator) {
        var ingestingColumnOpt = IngestionBase.getIngestingColumn(ingestingTable, ingestedTable);
        assert ingestingColumnOpt.isPresent() : "REEE!!";
        var ingestingColumn = ingestingColumnOpt.get();
        var newColumnStream = columnGenerator.apply(ingestedTable);
        var newColumnList = StreamExtensions
                .replaceInStream(ingestingTable.columnList().stream(), ingestingColumn, newColumnStream)
                .toList();
        return ingestedTable.withColumnList(newColumnList);
    }

    private static Optional<Column> getIngestingColumn(Table ingestingTable, Table ingestedTable) {
        assert ingestingTable != ingestedTable : "Table cannot ingest itself!";
        return ingestingTable.columnList().stream()
                .filter(column -> column.constraintSet().stream()
                        .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                        .map(c -> (ColumnConstraintForeignKeyInverse) c)
                        .map(ColumnConstraintForeignKeyInverse::foreignColumnId)
                        .anyMatch(cid -> ingestedTable.columnList().stream()
                                .anyMatch(columnB -> columnB.constraintSet().stream()
                                        .filter(c -> c instanceof ColumnConstraintForeignKey)
                                        .anyMatch(c -> ((ColumnConstraintForeignKey) c).foreignColumnId() == cid))))
                .findFirst();
    }

    public static boolean canIngest(Table table, Set<Table> tableSet, boolean insistOnOneToOne) {
        var ingestionCandidates = getIngestionCandidates(table, tableSet, insistOnOneToOne);
        return ingestionCandidates.keySet().size() > 0;
    }

    @NotNull
    public static Map<Column, Set<Table>> getIngestionCandidates(Table table, Set<Table> tableSet, boolean insistOnOneToOne) {
        var otherTableSet = tableSet.stream().filter(t -> t != table).collect(Collectors.toSet());
        var ingestionCandidates = table.columnList().stream().collect(Collectors.toMap(
                Function.identity(),
                // All tables pointing to <table> could be ingested by <table> into a collection
                column -> column.constraintSet().stream()
                        .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                        .map(c -> ((ColumnConstraintForeignKeyInverse) c).foreignColumnId())
                        .filter(cid -> !insistOnOneToOne || column.constraintSet().stream()
                                .filter(c -> c instanceof ColumnConstraintForeignKey)
                                .anyMatch(c -> ((ColumnConstraintForeignKey) c).foreignColumnId() == cid ))
                        .map(cid -> columnIdToTable(cid, otherTableSet))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(t -> hasSimpleRelationship(table, t))
                        .collect(Collectors.toSet())));

        return table.columnList().stream()
                .filter(column -> ingestionCandidates.get(column).size() > 0)
                .collect(Collectors.toMap(Function.identity(), ingestionCandidates::get));
    }

    @NotNull
    private static Optional<Table> columnIdToTable(int columnId, Set<Table> tableSet) {
        return tableSet.stream().filter(t -> t.columnList().stream().anyMatch(c -> c.id() == columnId)).findFirst();
    }

    private static boolean hasSimpleRelationship(Table tableA, Table tableB) {
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