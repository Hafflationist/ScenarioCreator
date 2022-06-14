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
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NullableToHorizontalInheritance implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet,
                                Function<Integer, int[]> idGenerator, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a nullable column or is referenced by another column!");
        if (!hasNullableColumnsAndNoInverseConstraints(table)) {
            throw exception;
        }

        var extractableColumnList = chooseExtendingColumns(table.columnList(), random);
        var newIds = idGenerator.apply(table.columnList().size() - extractableColumnList.size());
        var doubledColumnList = table.columnList().stream()
                .filter(c -> !extractableColumnList.contains(c))
                .toList();
        var doubledColumnToNewId = Stream
                .iterate(0, x -> x + 1)
                .limit(doubledColumnList.size())
                .collect(Collectors.toMap(doubledColumnList::get, idx -> newIds[idx]));

        var newIdComplex = new NewIdComplex(
                doubledColumnToNewId
        );
        var newBaseTable = createBaseTable(table, extractableColumnList);
        var newDerivingTable = createDerivingTable(newBaseTable, extractableColumnList, newIdComplex, random);
        return Set.of(newBaseTable, newDerivingTable);
    }

    private Table createBaseTable(Table originalTable, List<Column> extractableColumnList) {
        var newColumnList = originalTable.columnList().stream()
                .filter(c -> !extractableColumnList.contains(c))
                .toList();

        return originalTable.withColumnList(newColumnList);
    }

    private Column modifyPrimaryKeyColumnsForDerivation(Column column, NewIdComplex newIdComplex) {
        if (!newIdComplex.doubledColumnToNewId.containsKey(column)) {
            return column;
        }
        var newId = newIdComplex.doubledColumnToNewId.get(column);
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withId(newId);
            case ColumnCollection col -> col.withId(newId);
            case ColumnNode node -> node.withId(newId);
        };
    }

    private Table createDerivingTable(Table baseTable, List<Column> extractableColumnList,
                                      NewIdComplex newIdComplex, Random random) {
        // TODO: Vielleicht könnte man hier nen besseren Namen generieren:
        var newName = LinguisticUtils.merge(baseTable.name(), GroupingColumnsBase.mergeNames(extractableColumnList, random), random);

        var newColumnList = baseTable.columnList().stream()
                .map(c -> modifyPrimaryKeyColumnsForDerivation(c, newIdComplex))
                .toList();
        return baseTable
                .withColumnList(newColumnList)
                .withName(newName);
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
        return tableSet.stream().filter(this::hasNullableColumnsAndNoInverseConstraints).collect(Collectors.toSet());
    }

    private boolean hasNullableColumnsAndNoInverseConstraints(Table table) {
        var hasNullableColumns = table.columnList().stream().anyMatch(Column::isNullable);
        // TODO: Das Problem bei ausgehenden Fremdschlüsselbeziehungen ist die notwendige Duplikation der Beziehung, falls die Fremdschlüsselspalte keine Primärschlüsselspalte ist.
        // So eine Beziehung würde eine schemaweite Transformation erfordern.
        // Die Lösung wirkt anstrengend :(
        var hasNoForeignKeyConstraints = table.columnList().stream()
                .allMatch(column -> column.constraintSet().stream()
                        .noneMatch(c -> c instanceof ColumnConstraintForeignKey));
        var hasNoInverseKeyConstraints = table.columnList().stream()
                .allMatch(column -> column.constraintSet().stream()
                        .noneMatch(c -> c instanceof ColumnConstraintForeignKeyInverse));
        return hasNullableColumns && hasNoForeignKeyConstraints && hasNoInverseKeyConstraints;
    }

    private record NewIdComplex(Map<Column, Integer> doubledColumnToNewId) {
    }
}