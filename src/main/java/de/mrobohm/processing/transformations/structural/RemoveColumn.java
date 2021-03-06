package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.processing.transformations.ColumnTransformation;
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
    public boolean breaksSemanticSaturation() {
        return false;
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
        var hasPrimaryKeyConstraint = column.containsConstraint(ColumnConstraintPrimaryKey.class);
        var hasForeignKeyConstraint = column.containsConstraint(ColumnConstraintForeignKey.class);
        var hasForeignKeyInversConstraint = column.containsConstraint(ColumnConstraintForeignKeyInverse.class);
        return !hasPrimaryKeyConstraint && !hasForeignKeyConstraint && !hasForeignKeyInversConstraint;
    }
}