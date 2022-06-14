package de.mrobohm.operations.structural.base;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class IngestionBase {
    private IngestionBase() {
    }

    public static Schema fullRandomIngestion(
            Schema schema,
            BiFunction<Table, Boolean, Stream<Column>> columnGenerator,
            IngestionFlags flags,
            Random random) {
        var ex = new TransformationCouldNotBeExecutedException("Given schema does not contain suitable tables!");
        var expandableTableStream = schema.tableSet().stream()
                .filter(t -> IngestionBase.canIngest(t, schema.tableSet(), flags));
        var chosenTable = StreamExtensions.pickRandomOrThrow(expandableTableStream, ex, random);
        var ingestionCandidates = IngestionBase.getIngestionCandidates(
                chosenTable, schema.tableSet(), flags
        );
        var ingestingColumnStream = ingestionCandidates.keySet().stream();
        var chosenColumn = StreamExtensions.pickRandomOrThrow(ingestingColumnStream, ex, random);
        var ingestableTableStream = ingestionCandidates.get(chosenColumn).stream();
        var chosenIngestableTable = StreamExtensions.pickRandomOrThrow(ingestableTableStream, ex, random);
        var newTable = ingest(chosenTable, chosenColumn, chosenIngestableTable, columnGenerator);
        var newTableSet = StreamExtensions
                .replaceInStream(schema.tableSet().stream(), Stream.of(chosenTable, chosenIngestableTable), newTable)
                .collect(Collectors.toSet());
        return schema.withTables(newTableSet);
    }

    public static Table ingest(
            Table ingestingTable,
            Column ingestingColumn,
            Table ingestedTable,
            BiFunction<Table, Boolean, Stream<Column>> columnGenerator) {
        assert ingestingTable.columnList().contains(ingestingColumn) : "ingesting column should be part of the ingesting table!";
        // if the ingesting column can be null, no records from the ingested table will reference the ingesting column.
        // That's why we use the nullability of the ingesting column to determine the nullability of the new columns.
        var newColumnStream = columnGenerator.apply(ingestedTable, ingestingColumn.isNullable());
        var newColumnList = StreamExtensions
                .replaceInStream(ingestingTable.columnList().stream(), ingestingColumn, newColumnStream)
                .toList();
        return ingestedTable.withColumnList(newColumnList);
    }

    public static boolean canIngest(Table table, Set<Table> tableSet, IngestionFlags flags) {
        var ingestionCandidates = getIngestionCandidates(table, tableSet, flags);
        return ingestionCandidates.keySet().size() > 0;
    }

    @NotNull
    private static Map<Column, Set<Table>> getIngestionCandidates(
            Table table,
            Set<Table> tableSet,
            IngestionFlags flags) {
        var otherTableSet = tableSet.stream().filter(t -> t != table).collect(Collectors.toSet());
        var ingestionCandidates = table.columnList().stream().collect(Collectors.toMap(
                Function.identity(),
                // All tables pointing to <table> could be ingested by <table> into a collection
                column -> column.constraintSet().stream()
                        .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                        .map(c -> ((ColumnConstraintForeignKeyInverse) c).foreignColumnId())
                        .filter(cid -> !flags.insistOnOneToOne() || column.constraintSet().stream()
                                .filter(c -> c instanceof ColumnConstraintForeignKey)
                                .anyMatch(c -> ((ColumnConstraintForeignKey) c).foreignColumnId() == cid))
                        .map(cid -> columnIdToTable(cid, otherTableSet))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(t -> hasSimpleRelationship(table, t, flags.shouldConserveAllRecords()))
                        .collect(Collectors.toSet())));

        return table.columnList().stream()
                .filter(column -> ingestionCandidates.get(column).size() > 0)
                .collect(Collectors.toMap(Function.identity(), ingestionCandidates::get));
    }

    @NotNull
    private static Optional<Table> columnIdToTable(int columnId, Set<Table> tableSet) {
        return tableSet.stream().filter(t -> t.columnList().stream().anyMatch(c -> c.id() == columnId)).findFirst();
    }

    private static boolean hasSimpleRelationship(Table tableA, Table tableB, boolean tableBNonNull) {
        var nonNull = tableB.columnList().stream().noneMatch(Column::isNullable);
        return getRelationshipCount(tableA, tableB) <= 1 && (!tableBNonNull || nonNull);
    }

    public static long getRelationshipCount(Table tableA, Table tableB){
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
        var cidExistingInB = Stream.concat(foreignColumnIdSet1, foreignColumnIdSet2)
                .distinct()
                .map(cid -> columnIdToTable(cid, Set.of(tableB)))
                .filter(Optional::isPresent);
        return cidExistingInB.count();
    }

    public record IngestionFlags(boolean insistOnOneToOne, boolean shouldConserveAllRecords) {

    }

}