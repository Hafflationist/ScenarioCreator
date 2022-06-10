package de.mrobohm.operations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RemoveTable implements TableTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet,
                                Function<Integer, int[]> idGenerator, Random random) {
        assert freeOfRelationships(table) : "Table had foreign key constraints!";
        return new HashSet<>();
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream()
                .filter(this::freeOfRelationships)
                .collect(Collectors.toSet());
    }

    private boolean freeOfRelationships(Table table) {
        var ownColumnIdSet = table.columnList().stream()
                .map(Column::id)
                .collect(Collectors.toSet());
        return table.columnList().stream().noneMatch(column -> {
            var hasForeignKeys = column.constraintSet().stream()
                    .filter(c -> c instanceof ColumnConstraintForeignKey)
                    .map(c -> (ColumnConstraintForeignKey) c)
                    .anyMatch(c -> !ownColumnIdSet.contains(c.foreignColumnId()));
            var hasForeignKeysInverse = column.constraintSet().stream()
                    .filter(c -> c instanceof ColumnConstraintForeignKeyInverse)
                    .map(c -> (ColumnConstraintForeignKeyInverse) c)
                    .anyMatch(c -> !ownColumnIdSet.contains(c.foreignColumnId()));
            return hasForeignKeys || hasForeignKeysInverse;
        });
    }
}