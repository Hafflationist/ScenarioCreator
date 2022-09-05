package de.mrobohm.processing.transformations.structural.base;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.*;
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
        final var ex = new TransformationCouldNotBeExecutedException("Given schema does not contain suitable tables!");
        final var expandableTableStream = schema.tableSet().stream()
                .filter(t -> IngestionBase.canIngest(t, schema.tableSet(), flags));
        final var chosenTable = StreamExtensions.pickRandomOrThrow(expandableTableStream, ex, random);
        final var ingestionCandidates = IngestionBase.getIngestionCandidates(
                chosenTable, schema.tableSet(), flags
        );
        final var ingestingColumnStream = ingestionCandidates.keySet().stream();
        final var chosenColumn = StreamExtensions.pickRandomOrThrow(ingestingColumnStream, ex, random);
        final var ingestableTableStream = ingestionCandidates.get(chosenColumn).stream();
        final var chosenIngestableTable = StreamExtensions.pickRandomOrThrow(ingestableTableStream, ex, random);
        final var newTable = ingest(chosenTable, chosenColumn, chosenIngestableTable, columnGenerator);
        final var newTableSet = StreamExtensions
                .replaceInStream(schema.tableSet().stream(), Stream.of(chosenTable, chosenIngestableTable), newTable)
                .collect(Collectors.toCollection(TreeSet::new));
        return schema.withTableSet(newTableSet);
    }

    public static Table ingest(
            Table ingestingTable,
            Column ingestingColumn,
            Table ingestedTable,
            BiFunction<Table, Boolean, Stream<Column>> columnGenerator) {
        assert ingestingTable.columnList().contains(ingestingColumn) : "ingesting column should be part of the ingesting table!";

        final var ingestedColumnOptional = ingestedTable.columnList().stream()
                .filter(column -> ingestingColumn.constraintSet().stream()
                        .anyMatch(c -> c instanceof ColumnConstraintForeignKeyInverse ccfki
                                && ccfki.foreignColumnId().equals(column.id())))
                .findFirst();
        assert ingestedColumnOptional.isPresent();
        final var newIngestedColumn = freeColumnFromConstraints(ingestedColumnOptional.get(), ingestingTable.columnList());
        final var newIngestedColumnList = StreamExtensions.replaceInStream(
                ingestedTable.columnList().stream(),
                ingestedColumnOptional.get(),
                newIngestedColumn
        ).toList();

        // if the ingesting column can be null, no records from the ingested table will reference the ingesting column.
        // That's why we use the nullability of the ingesting column to determine the nullability of the new columns.
        final var extendingColumnStream = columnGenerator.apply(
                ingestedTable.withColumnList(newIngestedColumnList),
                ingestingColumn.isNullable()
        );
        final var newIngestingColumnWithOldId = freeColumnFromConstraints(ingestingColumn, ingestedTable.columnList());
        final var newIngestingColumn = fuseColumnWithIngestedTable(newIngestingColumnWithOldId, ingestedTable);
        // column should be removed, if its only purpose is to point to the ingested table
        final var newIngestingColumnList = countDependingColumns(newIngestingColumn) == 0
                ? StreamExtensions
                .replaceInStream(ingestingTable.columnList().stream(), ingestingColumn, extendingColumnStream)
                .toList()
                : StreamExtensions
                .replaceInStream(ingestingTable.columnList().stream(), ingestingColumn,
                        Stream.concat(Stream.of(newIngestingColumn), extendingColumnStream))
                .toList();

        final var allFdSet = Stream.concat(
                ingestingTable.functionalDependencySet().stream(),
                ingestedTable.functionalDependencySet().stream()
        ).collect(Collectors.toCollection(TreeSet::new));
        final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(allFdSet, newIngestingColumnList);
        return ingestingTable
                .withColumnList(newIngestingColumnList)
                .withFunctionalDependencySet(newFunctionalDependencySet);
    }

    private static Column fuseColumnWithIngestedTable(Column column, Table ingestedTable) {
        return column;
        // Ob diese Methode überhaupt sinnvoll ist, muss noch geklärt werden!
//        final var newId = new IdMerge(column.id(), ingestedTable.id(), MergeOrSplitType.And);
//        return switch (column) {
//            case ColumnLeaf leaf -> leaf.withId(newId);
//            case ColumnNode node -> node.withId(newId);
//            case ColumnCollection col -> col.withId(newId);
//        };
    }

    private static Column freeColumnFromConstraints(Column column, List<Column> columnListOfOtherTable) {
        final var otherColumnIdSet = columnListOfOtherTable.stream().map(Column::id).collect(Collectors.toCollection(TreeSet::new));
        final var newIngestingColumnConstraintSet = column.constraintSet().stream()
                .filter(c ->
                        !(c instanceof ColumnConstraintForeignKey ccfk && otherColumnIdSet.contains(ccfk.foreignColumnId()))
                                && !(c instanceof ColumnConstraintForeignKeyInverse ccfki && otherColumnIdSet.contains(ccfki.foreignColumnId())))
                .collect(Collectors.toCollection(TreeSet::new));
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withConstraintSet(newIngestingColumnConstraintSet);
            case ColumnNode node -> node.withConstraintSet(newIngestingColumnConstraintSet);
            case ColumnCollection col -> col.withConstraintSet(newIngestingColumnConstraintSet);
        };
    }

    private static int countDependingColumns(Column column) {
        final var foreignedIdStream = column.constraintSet().stream()
                .filter(c -> c instanceof ColumnConstraintForeignKey)
                .map(c -> (ColumnConstraintForeignKey) c)
                .map(ColumnConstraintForeignKey::foreignColumnId);
        final var inversedIdStream = column.constraintSet().stream()
                .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                .map(c -> (ColumnConstraintForeignKeyInverse) c)
                .map(ColumnConstraintForeignKeyInverse::foreignColumnId);
        return Stream.concat(foreignedIdStream, inversedIdStream).collect(Collectors.toCollection(TreeSet::new)).size();
    }

    public static boolean canIngest(Table table, SortedSet<Table> tableSet, IngestionFlags flags) {
        final var ingestionCandidates = getIngestionCandidates(table, tableSet, flags);
        return ingestionCandidates.keySet().size() > 0;
    }

    @NotNull
    private static Map<Column, SortedSet<Table>> getIngestionCandidates(
            Table table,
            SortedSet<Table> tableSet,
            IngestionFlags flags) {
        final var otherTableSet = tableSet.stream().filter(t -> t != table).collect(Collectors.toCollection(TreeSet::new));
        final var ingestionCandidates = table.columnList().stream().collect(Collectors.toMap(
                Function.identity(),
                // All tables pointing to <table> could be ingested by <table> into a collection
                column -> column.constraintSet().stream()
                        .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                        .map(c -> ((ColumnConstraintForeignKeyInverse) c).foreignColumnId())
                        .filter(cid -> !flags.insistOnOneToOne() || column.constraintSet().stream()
                                .filter(c -> c instanceof ColumnConstraintForeignKey)
                                .anyMatch(c -> ((ColumnConstraintForeignKey) c).foreignColumnId().equals(cid)))
                        .map(cid -> columnIdToTable(cid, otherTableSet))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(t -> hasSimpleRelationship(table, t, flags.shouldConserveAllRecords()))
                        .collect(Collectors.toCollection(TreeSet::new))));

        return table.columnList().stream()
                .filter(column -> ingestionCandidates.get(column).size() > 0)
                .collect(Collectors.toMap(Function.identity(), ingestionCandidates::get));
    }

    @NotNull
    private static Optional<Table> columnIdToTable(Id columnId, SortedSet<Table> tableSet) {
        return tableSet.stream().filter(t -> t.columnList().stream().anyMatch(c -> c.id().equals(columnId))).findFirst();
    }

    private static boolean hasSimpleRelationship(Table tableA, Table tableB, boolean tableBNonNull) {
        final var nonNull = tableB.columnList().stream().noneMatch(Column::isNullable);
        return getRelationshipCount(tableA, tableB) <= 1 && (!tableBNonNull || nonNull);
    }

    public static long getRelationshipCount(Table tableA, Table tableB) {
        final var constraints = tableA.columnList().stream()
                .flatMap(column -> column.constraintSet().stream())
                .toList();
        final var foreignColumnIdSet1 = constraints.stream()
                .filter(c -> c instanceof ColumnConstraintForeignKey)
                .map(c -> (ColumnConstraintForeignKey) c)
                .map(ColumnConstraintForeignKey::foreignColumnId);
        final var foreignColumnIdSet2 = constraints.stream()
                .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                .map(c -> (ColumnConstraintForeignKeyInverse) c)
                .map(ColumnConstraintForeignKeyInverse::foreignColumnId);
        final var cidExistingInB = Stream.concat(foreignColumnIdSet1, foreignColumnIdSet2)
                .distinct()
                .map(cid -> columnIdToTable(cid, SSet.of(tableB)))
                .filter(Optional::isPresent);
        return cidExistingInB.count();
    }

    public record IngestionFlags(boolean insistOnOneToOne, boolean shouldConserveAllRecords) {

    }

}