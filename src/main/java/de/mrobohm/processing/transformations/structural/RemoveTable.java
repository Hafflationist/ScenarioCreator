package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
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
        return table.columnList().stream().noneMatch(column -> {
            final var hasForeignKeys = column.constraintSet().stream()
                    .filter(c -> c instanceof ColumnConstraintForeignKey)
                    .map(c -> (ColumnConstraintForeignKey) c)
                    .anyMatch(c -> !ownColumnIdSet.contains(c.foreignColumnId()));
            final var hasForeignKeysInverse = column.constraintSet().stream()
                    .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                    .map(c -> (ColumnConstraintForeignKeyInverse) c)
                    .anyMatch(c -> !ownColumnIdSet.contains(c.foreignColumnId()));
            return hasForeignKeys || hasForeignKeysInverse;
        });
    }
}