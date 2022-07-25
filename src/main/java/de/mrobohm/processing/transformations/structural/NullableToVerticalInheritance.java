package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.MergeOrSplitType;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.processing.transformations.structural.base.GroupingColumnsBase;
import de.mrobohm.processing.transformations.structural.base.NewTableBase;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NullableToVerticalInheritance implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return true;
    }

    @Override
    @NotNull
    public SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a nullable column!");
        if (!hasNullableColumns(table)) {
            throw exception;
        }

        var extractableColumnList = chooseExtendingColumns(table.columnList(), random);
        var primaryKeyColumnList = getPrimaryKeyColumns(table.columnList()).stream().map(Column::id).toList();
        var newIds = idGenerator.apply(primaryKeyColumnList.size() + 4);
        var primaryKeyColumnToNewId = Stream
                .iterate(0, x -> x + 1)
                .limit(primaryKeyColumnList.size())
                .collect(Collectors.toMap(primaryKeyColumnList::get, idx -> newIds[idx]));

        var newIdComplex = new NewIdComplex(
                newIds[newIds.length - 4],
                newIds[newIds.length - 3],
                newIds[newIds.length - 2],
                newIds[newIds.length - 1],
                primaryKeyColumnToNewId
        );
        var newBaseTable = createBaseTable(table, extractableColumnList, newIdComplex);
        var newDerivingTable = createDerivingTable(
                newBaseTable, extractableColumnList, newIdComplex, primaryKeyColumnList.isEmpty(), random
        );
        return SSet.of(newBaseTable, newDerivingTable);
    }

    private Column addForeignIfPrimaryKey(Column column, NewIdComplex newIdComplex) {
        if (!column.containsConstraint(ColumnConstraintPrimaryKey.class)) {
            return column;
        }
        assert newIdComplex.primaryKeyColumnToNewId().containsKey(column.id())
                : "Map should contain an id for every primary key column!";
        var newConstraint = new ColumnConstraintForeignKeyInverse(
                newIdComplex.primaryKeyColumnToNewId().get(column.id()), SSet.of()
        );
        var newConstraintSet = StreamExtensions
                .prepend(column.constraintSet().stream(), newConstraint)
                .collect(Collectors.toCollection(TreeSet::new));
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withConstraintSet(newConstraintSet);
            case ColumnNode node -> node.withConstraintSet(newConstraintSet);
            case ColumnCollection collection -> collection.withConstraintSet(newConstraintSet);
        };
    }

    private Table createBaseTable(Table originalTable, List<Column> extractableColumnList, NewIdComplex newIdComplex) {
        var newColumnList = originalTable.columnList().stream()
                .filter(c -> !extractableColumnList.contains(c))
                .map(c -> addForeignIfPrimaryKey(c, newIdComplex))
                .toList();
        var newId = new IdPart(originalTable.id(), 0, MergeOrSplitType.Other);

        if (getPrimaryKeyColumns(originalTable.columnList()).isEmpty()) {
            var newPrimaryColumnConstraintSet = SSet.of(
                    new ColumnConstraintPrimaryKey(newIdComplex.primaryKeyConstraintGroupId()),
                    new ColumnConstraintForeignKeyInverse(newIdComplex.primaryKeyDerivingColumnId(), SSet.of())
            );
            var newPrimaryColumn = NewTableBase.createNewIdColumn(
                    newIdComplex.primaryKeyColumnId(),
                    originalTable.name(),
                    newPrimaryColumnConstraintSet);
            return originalTable
                    .withId(newId)
                    .withColumnList(StreamExtensions.prepend(newColumnList.stream(), newPrimaryColumn).toList());
        }
        return originalTable
                .withId(newId)
                .withColumnList(newColumnList);
    }

    private Column modifyPrimaryKeyColumnsForDerivation(Column column, NewIdComplex newIdComplex) {
        if (!column.containsConstraint(ColumnConstraintPrimaryKey.class)) {
            return column;
        }
        var newConstraint = new ColumnConstraintForeignKey(column.id(), SSet.of());
        var newConstraintSet = StreamExtensions
                .prepend(column.constraintSet().stream(), newConstraint)
                .filter(c -> !(c instanceof ColumnConstraintForeignKeyInverse))
                .collect(Collectors.toCollection(TreeSet::new));
        var newId = newIdComplex.primaryKeyColumnToNewId.get(column.id());
        assert newId != null;
        return switch (column) {
            case ColumnLeaf leaf -> leaf
                    .withConstraintSet(newConstraintSet)
                    .withId(newId);
            case ColumnCollection collection -> collection
                    .withConstraintSet(newConstraintSet)
                    .withId(newId);
            case ColumnNode node -> node
                    .withConstraintSet(newConstraintSet)
                    .withId(newId);
        };
    }

    private Table createDerivingTable(Table baseTable, List<Column> extractableColumnList,
                                      NewIdComplex newIdComplex, boolean generateSurrogateKeys, Random random) {
        if (!(baseTable.id() instanceof IdPart baseTableIdPart)) {
            throw new RuntimeException();
        }
        var newId = new IdPart(
                baseTableIdPart.predecessorId(),
                baseTableIdPart.extensionNumber() + 1,
                MergeOrSplitType.Other
        );
        // TODO: Vielleicht könnte man hier nen besseren Namen generieren:
        var newName = LinguisticUtils.merge(
                baseTable.name(), GroupingColumnsBase.mergeNames(extractableColumnList, random), random
        );
        if (generateSurrogateKeys) {
            // In this case a surrogate key and column must be generated
            var newPrimaryColumnConstraintSet = SSet.of(
                    new ColumnConstraintPrimaryKey(newIdComplex.primaryKeyDerivingConstraintGroupId()),
                    new ColumnConstraintForeignKey(newIdComplex.primaryKeyColumnId(), SSet.of())
            );
            var newPrimaryColumn = NewTableBase.createNewIdColumn(
                    newIdComplex.primaryKeyDerivingColumnId(),
                    newName,
                    newPrimaryColumnConstraintSet);
            var newColumnList = Stream
                    .concat(Stream.of(newPrimaryColumn), extractableColumnList.stream())
                    .toList();
            return baseTable
                    .withId(newId)
                    .withName(newName)
                    .withColumnList(newColumnList);
        } else {
            // otherwise we take the primary key columns (with reassigned id and modified constraints)
            var newPrimaryKeyColumnList = getPrimaryKeyColumns(baseTable.columnList()).stream()
                    .map(c -> modifyPrimaryKeyColumnsForDerivation(c, newIdComplex));
            var newColumnList = Stream
                    .concat(newPrimaryKeyColumnList, extractableColumnList.stream())
                    .toList();
            return baseTable
                    .withId(newId)
                    .withName(newName)
                    .withColumnList(newColumnList);
        }
    }

    private List<Column> getPrimaryKeyColumns(List<Column> columnList) {
        return columnList.stream()
                .filter(column -> column.containsConstraint(ColumnConstraintPrimaryKey.class))
                .toList();
    }

    private List<Column> chooseExtendingColumns(List<Column> columnList, Random random) {
        var candidateColumnList = columnList.stream()
                .filter(column -> column.constraintSet().stream().noneMatch(c -> c instanceof ColumnConstraintPrimaryKey))
                .filter(Column::isNullable)
                .toList();
        assert !candidateColumnList.isEmpty();
        var num = random.nextInt(1, candidateColumnList.size() + 1);
        var runtimeException = new RuntimeException("Should not happen! (BUG)");
        return StreamExtensions
                .pickRandomOrThrowMultiple(candidateColumnList.stream(), num, runtimeException, random)
                .toList();
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream().filter(this::hasNullableColumns).collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean hasNullableColumns(Table table) {
        return table.columnList().size() >= 2 && table.columnList().stream().anyMatch(Column::isNullable);
    }

    private record NewIdComplex(Id primaryKeyColumnId,
                                Id primaryKeyConstraintGroupId,
                                Id primaryKeyDerivingColumnId,
                                Id primaryKeyDerivingConstraintGroupId,
                                Map<Id, Id> primaryKeyColumnToNewId) {
    }
}