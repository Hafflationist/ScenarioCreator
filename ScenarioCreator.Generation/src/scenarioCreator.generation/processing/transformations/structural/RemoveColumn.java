package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;

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
        // TODO: handle functional dependencies!!! (This should be done in a TableTransformation)
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
        final var hasPrimaryKeyConstraint = column.containsConstraint(ColumnConstraintPrimaryKey.class);
        final var hasForeignKeyConstraint = column.containsConstraint(ColumnConstraintForeignKey.class);
        final var hasForeignKeyInversConstraint = column.containsConstraint(ColumnConstraintForeignKeyInverse.class);
        return !hasPrimaryKeyConstraint && !hasForeignKeyConstraint && !hasForeignKeyInversConstraint;
    }
}