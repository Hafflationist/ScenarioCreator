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
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
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
    public SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var rte = new RuntimeException("Could not find a valid column. This should not be possible!");
        final var candidateStream= table.columnList().stream()
                .filter(this::isRemovable);
        final var chosenColumn = StreamExtensions.pickRandomOrThrow(candidateStream, rte, random);
        final var newColumnList = table.columnList().stream().filter(column -> column != chosenColumn).toList();
        final var newFdSet = table.functionalDependencySet().stream()
                .flatMap(fd -> mapFunctionalDependency(chosenColumn.id(), fd))
                .collect(Collectors.toCollection(TreeSet::new));

        return SSet.of(
                table
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFdSet)
        );
    }

    private Stream<FunctionalDependency> mapFunctionalDependency(Id removedId, FunctionalDependency fd) {
        if(fd.left().stream().anyMatch(id -> id.equals(removedId))) {
            return Stream.of();
        }
        if(fd.right().stream().noneMatch(id -> id.equals(removedId))) {
            return Stream.of(fd);
        }
        final var newRight = fd.right().stream().filter(id -> !id.equals(removedId)).collect(Collectors.toCollection(TreeSet::new));
        return Stream.of(new FunctionalDependency(fd.left(), newRight));
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