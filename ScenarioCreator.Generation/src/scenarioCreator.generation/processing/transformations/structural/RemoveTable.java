package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.utils.SSet;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RemoveTable implements TableTransformation {

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
        assert freeOfRelationships(table) : "Table had foreign key constraints!";
        return SSet.of();
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        if (tableSet.size() <= 1){
            return SSet.of();
        }
        return tableSet.stream()
                .filter(this::freeOfRelationships)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean freeOfRelationships(Table table) {
        final var ownColumnIdSet = table.columnList().stream()
                .map(Column::id)
                .collect(Collectors.toCollection(TreeSet::new));
        return table.columnList().stream().allMatch(column -> freeOfRelationships(column, ownColumnIdSet));
    }

    private boolean freeOfRelationships(Column column, SortedSet<Id> ownColumnIdSet) {
        final var freeOfForeignKeys = column.constraintSet().stream()
                .filter(c -> c instanceof ColumnConstraintForeignKey)
                .map(c -> (ColumnConstraintForeignKey) c)
                .allMatch(c -> ownColumnIdSet.contains(c.foreignColumnId()));
        final var freeOfForeignKeysInverse = column.constraintSet().stream()
                .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                .map(c -> (ColumnConstraintForeignKeyInverse) c)
                .allMatch(c -> ownColumnIdSet.contains(c.foreignColumnId()));
        final var nestedResult = switch (column) {
            case ColumnLeaf ignore -> true;
            case ColumnNode node ->
                    node.columnList().stream().allMatch(columnInner -> freeOfRelationships(columnInner, ownColumnIdSet));
            case ColumnCollection col ->
                    col.columnList().stream().allMatch(columnInner -> freeOfRelationships(columnInner, ownColumnIdSet));
        };
        return freeOfForeignKeys && freeOfForeignKeysInverse && nestedResult;
    }
}