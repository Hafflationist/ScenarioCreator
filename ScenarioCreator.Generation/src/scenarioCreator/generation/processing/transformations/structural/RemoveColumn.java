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
import scenarioCreator.data.table.FunctionalDependency;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemoveColumn implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var rte = new RuntimeException("Could not find a valid column. This should not be possible!");
        final var candidateStream = table.columnList().stream()
                .filter(this::isRemovable);
        final var chosenColumn = StreamExtensions.pickRandomOrThrow(candidateStream, rte, random);
        final var newColumnList = table.columnList().stream().filter(column -> column != chosenColumn).toList();
        final var removeIdSet = getAllIds(chosenColumn);
        final var newFdSet = table.functionalDependencySet().stream()
                .flatMap(fd -> mapFunctionalDependency(removeIdSet, fd))
                .collect(Collectors.toCollection(TreeSet::new));

        final var newTableSet = SSet.of(
                table
                        .withColumnList(newColumnList)
                        .withFunctionalDependencySet(newFdSet)
        );
        final List<TupleGeneratingDependency> tgdList = List.of(); //TODO: tgds
        return new Pair<>(newTableSet, tgdList);
    }

    private SortedSet<Id> getAllIds(Column column) {
        return switch (column) {
            case ColumnLeaf leaf -> SSet.of(leaf.id());
            case ColumnNode node -> SSet.prepend(
                    node.id(),
                    node.columnList().stream()
                            .map(this::getAllIds)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toCollection(TreeSet::new))
            );
            case ColumnCollection col -> SSet.prepend(
                    col.id(),
                    col.columnList().stream()
                            .map(this::getAllIds)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toCollection(TreeSet::new))

            );
        };
    }

    private Stream<FunctionalDependency> mapFunctionalDependency(SortedSet<Id> removedIdSet, FunctionalDependency fd) {
        if (fd.left().stream().anyMatch(removedIdSet::contains)) {
            return Stream.of();
        }
        if (fd.right().stream().noneMatch(removedIdSet::contains)) {
            return Stream.of(fd);
        }
        final var newRight = fd.right().stream()
                .filter(id -> !removedIdSet.contains(id))
                .collect(Collectors.toCollection(TreeSet::new));
        return FunctionalDependency.tryCreate(fd.left(), newRight).stream();
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(this::isTableValid)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean isTableValid(Table table) {
        if (table.columnList().size() < 2) {
            return false;
        }
        return table.columnList().stream()
                .anyMatch(this::isRemovable);
    }

    private boolean isRemovable(Column column) {
        final var hasPrimaryKeyConstraint = column.containsConstraint(ColumnConstraintPrimaryKey.class);
        final var hasForeignKeyConstraint = column.containsConstraint(ColumnConstraintForeignKey.class);
        final var hasForeignKeyInversConstraint = column.containsConstraint(ColumnConstraintForeignKeyInverse.class);
        final var isContentRemovable = switch (column) {
            case ColumnLeaf ignore -> true;
            case ColumnNode node -> node.columnList().stream().allMatch(this::isRemovable);
            case ColumnCollection col -> col.columnList().stream().allMatch(this::isRemovable);
        };
        return !hasPrimaryKeyConstraint && !hasForeignKeyConstraint && !hasForeignKeyInversConstraint && isContentRemovable;
    }
}