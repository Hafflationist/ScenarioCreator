package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.utils.SSet;
import org.jetbrains.annotations.NotNull;

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
                .anyMatch(c -> ownColumnIdSet.contains(c.foreignColumnId()));
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