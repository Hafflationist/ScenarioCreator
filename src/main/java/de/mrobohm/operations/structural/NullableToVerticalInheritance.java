package de.mrobohm.operations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.operations.structural.base.GroupingColumnsBase;
import de.mrobohm.operations.structural.base.NewTableBase;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NullableToVerticalInheritance implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet,
                                Function<Integer, int[]> idGenerator, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a nullable column!");
        if (!hasNullableColumns(table)) {
            throw exception;
        }

        var extractableColumnList = chooseExtendingColumns(table.columnList(), random);
        var primaryKeyColumnList = getPrimaryKeyColumns(table.columnList());
        var newIds = idGenerator.apply(primaryKeyColumnList.size() + 5);
        var primaryKeyColumnToNewId = Stream
                .iterate(0, x -> x + 1)
                .limit(primaryKeyColumnList.size())
                .collect(Collectors.toMap(primaryKeyColumnList::get, idx -> newIds[idx]));

        var newIdComplex = new NewIdComplex(
                newIds[newIds.length - 5],
                newIds[newIds.length - 4],
                newIds[newIds.length - 3],
                newIds[newIds.length - 2],
                newIds[newIds.length - 1],
                primaryKeyColumnToNewId
        );
        var newBaseTable = createBaseTable(table, extractableColumnList, newIdComplex);
        var newDerivingTable = createDerivingTable(newBaseTable, extractableColumnList, newIdComplex, random);
        return Set.of(newBaseTable, newDerivingTable);
    }

    private Column addForeignIfPrimaryKey(Column column, NewIdComplex newIdComplex) {
        if (column.constraintSet().stream().noneMatch(c -> c instanceof ColumnConstraintPrimaryKey)) {
            return column;
        }
        assert newIdComplex.primaryKeyColumnToNewId().containsKey(column) : "Map should contain an id for every primary key column!";
        var newConstraint = new ColumnConstraintForeignKeyInverse(newIdComplex.primaryKeyColumnToNewId().get(column), Set.of());
        var newConstraintSet = StreamExtensions
                .prepend(column.constraintSet().stream(), newConstraint)
                .collect(Collectors.toSet());
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

        if (getPrimaryKeyColumns(extractableColumnList).isEmpty()) {
            var newPrimaryColumnConstraintSet = Set.of(
                    new ColumnConstraintPrimaryKey(newIdComplex.primaryKeyConstraintGroupId()),
                    new ColumnConstraintForeignKeyInverse(newIdComplex.primaryKeyDerivingColumnId(), Set.of())
            );
            var newPrimaryColumn = NewTableBase.createNewIdColumn(
                    newIdComplex.primaryKeyColumnId(),
                    originalTable.name(),
                    newPrimaryColumnConstraintSet);
            return originalTable.withColumnList(StreamExtensions.prepend(newColumnList.stream(), newPrimaryColumn).toList());
        }
        return originalTable.withColumnList(newColumnList);
    }

    private Column modifyPrimaryKeyColumnsForDerivation(Column column, NewIdComplex newIdComplex) {
        if (column.constraintSet().stream().noneMatch(c -> c instanceof ColumnConstraintPrimaryKey)) {
            return column;
        }
        var newConstraint = new ColumnConstraintForeignKey(column.id(), Set.of());
        var newConstraintSet = StreamExtensions
                .prepend(column.constraintSet().stream(), newConstraint)
                .filter(c -> !(c instanceof ColumnConstraintForeignKeyInverse))
                .collect(Collectors.toSet());
        return switch (column) {
            case ColumnLeaf leaf -> leaf
                    .withConstraintSet(newConstraintSet)
                    .withId(newIdComplex.primaryKeyColumnToNewId.get(leaf));
            case ColumnCollection collection -> collection
                    .withConstraintSet(newConstraintSet)
                    .withId(newIdComplex.primaryKeyColumnToNewId.get(collection));
            case ColumnNode node -> node
                    .withConstraintSet(newConstraintSet)
                    .withId(newIdComplex.primaryKeyColumnToNewId.get(node));
        };
    }

    private Table createDerivingTable(Table baseTable, List<Column> extractableColumnList,
                                      NewIdComplex newIdComplex, Random random) {
        // TODO: Vielleicht könnte man hier nen besseren Namen generieren:
        var newName = LinguisticUtils.merge(baseTable.name(), GroupingColumnsBase.mergeNames(extractableColumnList, random), random);

        if (getPrimaryKeyColumns(baseTable.columnList()).size() == 0) {
            // In this case a surrogate key and column must be generated
            var newPrimaryColumnConstraintSet = Set.of(
                    new ColumnConstraintPrimaryKey(newIdComplex.primaryKeyDerivingConstraintGroupId()),
                    new ColumnConstraintForeignKey(newIdComplex.primaryKeyColumnId(), Set.of())
            );
            var newPrimaryColumn = NewTableBase.createNewIdColumn(
                    newIdComplex.primaryKeyDerivingColumnId(),
                    newName,
                    newPrimaryColumnConstraintSet);
            var newColumnList = Stream
                    .concat(Stream.of(newPrimaryColumn), extractableColumnList.stream())
                    .toList();
            return baseTable
                    .withId(newIdComplex.derivingTableId)
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
                    .withId(newIdComplex.derivingTableId)
                    .withName(newName)
                    .withColumnList(newColumnList);
        }
    }

    private List<Column> getPrimaryKeyColumns(List<Column> columnList) {
        return columnList.stream().filter(column -> column.constraintSet().stream()
                        .anyMatch(c -> c instanceof ColumnConstraintPrimaryKey))
                .toList();
    }

    private List<Column> chooseExtendingColumns(List<Column> columnList, Random random) {
        var candidateColumnList = columnList.stream()
                .filter(column -> column.constraintSet().stream().noneMatch(c -> c instanceof ColumnConstraintPrimaryKey))
                .filter(Column::isNullable)
                .toList();
        assert !candidateColumnList.isEmpty();
        var num = random.nextInt(1, candidateColumnList.size());
        var runtimeException = new RuntimeException("Should not happen! (BUG)");
        return StreamExtensions
                .pickRandomOrThrowMultiple(candidateColumnList.stream(), num, runtimeException, random)
                .toList();
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream().filter(this::hasNullableColumns).collect(Collectors.toSet());
    }

    private boolean hasNullableColumns(Table table) {
        return table.columnList().stream().anyMatch(Column::isNullable);
    }

    private record NewIdComplex(int primaryKeyColumnId,
                                int primaryKeyConstraintGroupId,
                                int primaryKeyDerivingColumnId,
                                int primaryKeyDerivingConstraintGroupId,
                                int derivingTableId,
                                Map<Column, Integer> primaryKeyColumnToNewId) {
    }
}