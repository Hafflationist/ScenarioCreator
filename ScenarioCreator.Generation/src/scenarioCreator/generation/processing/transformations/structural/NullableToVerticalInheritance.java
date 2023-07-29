package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdPart;
import scenarioCreator.data.identification.MergeOrSplitType;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.generation.processing.transformations.structural.base.GroupingColumnsBase;
import scenarioCreator.generation.processing.transformations.structural.base.NewTableBase;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

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
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a nullable column!");
        if (!hasNullableColumns(table)) {
            throw exception;
        }

        final var extractableColumnList = chooseExtendingColumns(table.columnList(), random);
        final var primaryKeyColumnList = getPrimaryKeyColumns(table.columnList()).stream().map(Column::id).toList();
        final var newIds = idGenerator.apply(primaryKeyColumnList.size() + 4);
        final var primaryKeyColumnToNewId = Stream
                .iterate(0, x -> x + 1)
                .limit(primaryKeyColumnList.size())
                .collect(Collectors.toMap(primaryKeyColumnList::get, idx -> newIds[idx]));

        final var newIdComplex = new NewIdComplex(
                newIds[newIds.length - 4],
                newIds[newIds.length - 3],
                newIds[newIds.length - 2],
                newIds[newIds.length - 1],
                primaryKeyColumnToNewId
        );
        final var newBaseTable = createBaseTable(table, extractableColumnList, newIdComplex);
        final var newDerivingTable = createDerivingTable(
                newBaseTable, extractableColumnList, newIdComplex, primaryKeyColumnList.isEmpty(), random
        );
        final var newTableSet = SSet.of(newBaseTable, newDerivingTable);
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO(F): tgds
        return new Pair<>(newTableSet, tgdList);
    }

    private Column addForeignIfPrimaryKey(Column column, NewIdComplex newIdComplex) {
        if (!column.containsConstraint(ColumnConstraintPrimaryKey.class)) {
            return column;
        }
        assert newIdComplex.primaryKeyColumnToNewId().containsKey(column.id())
                : "Map should contain an id for every primary key column!";
        final var newConstraint = new ColumnConstraintForeignKeyInverse(
                newIdComplex.primaryKeyColumnToNewId().get(column.id())
        );
        final var newConstraintSet = StreamExtensions
                .prepend(column.constraintSet().stream(), newConstraint)
                .collect(Collectors.toCollection(TreeSet::new));
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withConstraintSet(newConstraintSet);
            case ColumnNode node -> node.withConstraintSet(newConstraintSet);
            case ColumnCollection collection -> collection.withConstraintSet(newConstraintSet);
        };
    }

    private Table createBaseTable(Table originalTable, List<Column> extractableColumnList, NewIdComplex newIdComplex) {
        final var newColumnList = originalTable.columnList().stream()
                .filter(c -> !extractableColumnList.contains(c))
                .map(c -> addForeignIfPrimaryKey(c, newIdComplex))
                .toList();
        final var newId = new IdPart(originalTable.id(), 0, MergeOrSplitType.Other);

        if (getPrimaryKeyColumns(originalTable.columnList()).isEmpty()) {
            final var newPrimaryColumnConstraintSet = SSet.of(
                    new ColumnConstraintPrimaryKey(newIdComplex.primaryKeyConstraintGroupId()),
                    new ColumnConstraintForeignKeyInverse(newIdComplex.primaryKeyDerivingColumnId())
            );
            final var newPrimaryColumn = NewTableBase.createNewIdColumn(
                    newIdComplex.primaryKeyColumnId(),
                    originalTable.name(),
                    newPrimaryColumnConstraintSet);
            final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                    originalTable.functionalDependencySet(), newColumnList
            );
            return originalTable
                    .withId(newId)
                    .withColumnList(StreamExtensions.prepend(newColumnList.stream(), newPrimaryColumn).toList())
                    .withFunctionalDependencySet(newFunctionalDependencySet);
        }
        final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                originalTable.functionalDependencySet(), newColumnList
        );
        return originalTable
                .withId(newId)
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFunctionalDependencySet);
    }

    private Column modifyPrimaryKeyColumnsForDerivation(Column column, NewIdComplex newIdComplex) {
        if (!column.containsConstraint(ColumnConstraintPrimaryKey.class)) {
            return column;
        }
        final var newConstraint = new ColumnConstraintForeignKey(column.id());
        final var newConstraintSet = StreamExtensions
                .prepend(column.constraintSet().stream(), newConstraint)
                .filter(c -> !(c instanceof ColumnConstraintForeignKeyInverse))
                .collect(Collectors.toCollection(TreeSet::new));
        final var newId = newIdComplex.primaryKeyColumnToNewId.get(column.id());
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
        final var newId = new IdPart(
                baseTableIdPart.predecessorId(),
                baseTableIdPart.extensionNumber() + 1,
                MergeOrSplitType.Other
        );
        final var newName = LinguisticUtils.merge(
                baseTable.name(), GroupingColumnsBase.mergeNames(extractableColumnList, random), random
        );
        if (generateSurrogateKeys) {
            // In this case a surrogate key and column must be generated
            final var newPrimaryColumnConstraintSet = SSet.of(
                    new ColumnConstraintPrimaryKey(newIdComplex.primaryKeyDerivingConstraintGroupId()),
                    new ColumnConstraintForeignKey(newIdComplex.primaryKeyColumnId())
            );
            final var newPrimaryColumn = NewTableBase.createNewIdColumn(
                    newIdComplex.primaryKeyDerivingColumnId(),
                    newName,
                    newPrimaryColumnConstraintSet);
            final var newColumnList = Stream
                    .concat(Stream.of(newPrimaryColumn), extractableColumnList.stream())
                    .toList();
            final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                    baseTable.functionalDependencySet(), newColumnList
            );
            return baseTable
                    .withId(newId)
                    .withName(newName)
                    .withColumnList(newColumnList)
                    .withFunctionalDependencySet(newFunctionalDependencySet);
        } else {
            // otherwise we take the primary key columns (with reassigned id and modified constraints)
            final var newPrimaryKeyColumnList = getPrimaryKeyColumns(baseTable.columnList()).stream()
                    .map(c -> modifyPrimaryKeyColumnsForDerivation(c, newIdComplex));
            final var newColumnList = Stream
                    .concat(newPrimaryKeyColumnList, extractableColumnList.stream())
                    .toList();
            final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                    baseTable.functionalDependencySet(), newColumnList
            );
            return baseTable
                    .withId(newId)
                    .withName(newName)
                    .withColumnList(newColumnList)
                    .withFunctionalDependencySet(newFunctionalDependencySet);
        }
    }

    private List<Column> getPrimaryKeyColumns(List<Column> columnList) {
        return columnList.stream()
                .filter(column -> column.containsConstraint(ColumnConstraintPrimaryKey.class))
                .toList();
    }

    private List<Column> chooseExtendingColumns(List<Column> columnList, Random random) {
        final var candidateColumnList = columnList.stream()
                .filter(column -> !column.containsConstraint(ColumnConstraintPrimaryKey.class))
                .filter(column -> !column.containsConstraint(ColumnConstraintForeignKey.class))
                .filter(column -> !column.containsConstraint(ColumnConstraintForeignKeyInverse.class))
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
        return tableSet.stream().filter(this::hasNullableColumns).collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean hasNullableColumns(Table table) {
        // TODO(80:20): handling of foreign keys could be added
        final var primaryKeyIsNotForeignKeyOrInverseForeignKey =
                table.columnList().stream()
                        .filter(column -> column.containsConstraint(ColumnConstraintPrimaryKey.class))
                        .allMatch(column -> !column.containsConstraint(ColumnConstraintForeignKey.class)
                        && !column.containsConstraint(ColumnConstraintForeignKeyInverse.class)
                        );
        return primaryKeyIsNotForeignKeyOrInverseForeignKey && table.columnList().size() >= 2 && table.columnList().stream()
                .anyMatch(column -> column.isNullable()
                        && !column.containsConstraint(ColumnConstraintForeignKey.class)
                        && !column.containsConstraint(ColumnConstraintForeignKeyInverse.class)
                );
    }

    private record NewIdComplex(Id primaryKeyColumnId,
                                Id primaryKeyConstraintGroupId,
                                Id primaryKeyDerivingColumnId,
                                Id primaryKeyDerivingConstraintGroupId,
                                Map<Id, Id> primaryKeyColumnToNewId) {
    }
}
