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
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.processing.transformations.structural.base.GroupingColumnsBase;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NullableToHorizontalInheritance implements TableTransformation {
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
    public SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a nullable column or is referenced by another column!");
        if (!hasNullableColumnsAndNoInverseConstraints(table)) {
            throw exception;
        }

        final var extractableColumnList = chooseExtendingColumns(table.columnList(), random);
        final var newIds = idGenerator.apply(table.columnList().size() - extractableColumnList.size() + 1);
        final var doubledColumnIdSet = table.columnList().stream()
                .filter(column -> !extractableColumnList.contains(column))
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        final var newIdComplex = new NewIdComplex(
                newIds[newIds.length - 1],
                doubledColumnIdSet
        );
        final var newBaseTable = createBaseTable(table, extractableColumnList);
        final var newDerivingTable = createDerivingTable(table, extractableColumnList, newIdComplex, random);
        return SSet.of(newBaseTable, newDerivingTable);
    }

    private Table createBaseTable(Table originalTable, List<Column> extractableColumnList) {
        final var newColumnList = originalTable.columnList().stream()
                .filter(column -> !extractableColumnList.contains(column))
                .map(column -> {
                    // the numerical distribution stays the same...
                    // the check constraints stay the same...
                    return appendExtensionNumberToId(column, 0);
                })
                .toList();
        final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                originalTable.functionalDependencySet(), newColumnList
        );
        return originalTable
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFunctionalDependencySet);
    }

    private Column modifyPrimaryKeyColumnsForDerivation(Column column, NewIdComplex newIdComplex) {
        if (!newIdComplex.doubledColumnIdSet.contains(column.id())) {
            return column;
        }
        return appendExtensionNumberToId(column, 1);
    }

    private Column appendExtensionNumberToId(Column column, int extensionNumber) {
        final var newId = new IdPart(column.id(), extensionNumber, MergeOrSplitType.Xor);
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withId(newId);
            case ColumnNode node -> {
                final var newColumnList = node.columnList().stream()
                        .map(columnInner -> appendExtensionNumberToId(columnInner, extensionNumber))
                        .toList();
                yield node
                        .withId(newId)
                        .withColumnList(newColumnList);
            }
            case ColumnCollection col -> {
                final var newColumnList = col.columnList().stream()
                        .map(columnInner -> appendExtensionNumberToId(columnInner, extensionNumber))
                        .toList();
                yield col
                        .withId(newId)
                        .withColumnList(newColumnList);
            }
        };
    }

    private Table createDerivingTable(Table baseTable, List<Column> extractableColumnList,
                                      NewIdComplex newIdComplex, Random random) {
        // TODO: Vielleicht könnte man hier nen besseren Namen generieren:
        final var newName = LinguisticUtils.merge(baseTable.name(), GroupingColumnsBase.mergeNames(extractableColumnList, random), random);

        final var newColumnList = baseTable.columnList().stream()
                .map(c -> modifyPrimaryKeyColumnsForDerivation(c, newIdComplex))
                .toList();
        final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                baseTable.functionalDependencySet(), newColumnList
        );
        return baseTable
                .withId(newIdComplex.derivingTableId())
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFunctionalDependencySet)
                .withName(newName);
    }

    private List<Column> chooseExtendingColumns(List<Column> columnList, Random random) {
        final var candidateColumnList = columnList.stream()
                .filter(column -> !column.containsConstraint(ColumnConstraintPrimaryKey.class))
                .filter(Column::isNullable)
                .toList();
        assert !candidateColumnList.isEmpty();
        final var num = random.nextInt(1, candidateColumnList.size() + 1);
        final var runtimeException = new RuntimeException("Should not happen! (BUG)");
        return StreamExtensions
                .pickRandomOrThrowMultiple(candidateColumnList.stream(), num, runtimeException, random)
                .toList();
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream().filter(this::hasNullableColumnsAndNoInverseConstraints).collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean hasNullableColumnsAndNoInverseConstraints(Table table) {
        final var hasNullableColumns = table.columnList().stream().anyMatch(Column::isNullable);
        final var hasEnoughColumns = table.columnList().size() >= 2;
        // TODO: Das Problem bei ausgehenden Fremdschlüsselbeziehungen ist die notwendige Duplikation der Beziehung, falls die Fremdschlüsselspalte keine Primärschlüsselspalte ist.
        // So eine Beziehung würde eine schemaweite Transformation erfordern.
        // Die Lösung wirkt anstrengend :(
        final var hasNoForeignKeyConstraints = table.columnList().stream()
                .noneMatch(column -> column.containsConstraint(ColumnConstraintForeignKey.class));
        final var hasNoInverseKeyConstraints = table.columnList().stream()
                .noneMatch(column -> column.containsConstraint(ColumnConstraintForeignKeyInverse.class));
        return hasNullableColumns && hasEnoughColumns && hasNoForeignKeyConstraints && hasNoInverseKeyConstraints;
    }

    private record NewIdComplex(Id derivingTableId, SortedSet<Id> doubledColumnIdSet) {
    }
}