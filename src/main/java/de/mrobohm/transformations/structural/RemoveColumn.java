package de.mrobohm.transformations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.transformations.ColumnTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class RemoveColumn implements ColumnTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        assert freeOfCriticalConstraints(column) : "Column had critical constraints!";
        return new ArrayList<>();
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        if (columnList.size() == 1) {
            return new ArrayList<>();
        }
        return columnList.stream().filter(this::freeOfCriticalConstraints).toList();
    }

    private boolean freeOfCriticalConstraints(Column column) {
        var hasPrimaryKeyConstraint = column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintPrimaryKey);
        var hasForeignKeyConstraint = column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintForeignKey);
        var hasForeignKeyInversConstraint = column.constraintSet().stream()
                .anyMatch(c -> c instanceof ColumnConstraintForeignKeyInverse);
        return !hasPrimaryKeyConstraint && !hasForeignKeyConstraint && !hasForeignKeyInversConstraint;
    }
}