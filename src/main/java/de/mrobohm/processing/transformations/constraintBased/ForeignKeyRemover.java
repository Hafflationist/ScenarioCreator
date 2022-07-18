package de.mrobohm.processing.transformations.constraintBased;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.processing.transformations.ColumnTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ForeignKeyRemover implements ColumnTransformation {

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
        if (!hasForeignKeyConstraint(column)) {
            throw new TransformationCouldNotBeExecutedException("No foreign key constraint found! Expected a column with a foreign key constraint!");
        }

        var newConstraintSet = column.constraintSet().stream()
                .filter( c -> !(c instanceof ColumnConstraintForeignKey))
                .collect(Collectors.toSet());

        return switch (column) {
            case ColumnCollection c -> List.of(c.withConstraintSet(newConstraintSet));
            case ColumnLeaf c -> List.of(c.withConstraintSet(newConstraintSet));
            case ColumnNode c -> List.of(c.withConstraintSet(newConstraintSet));
        };
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::hasForeignKeyConstraint).toList();
    }

    private boolean hasForeignKeyConstraint(Column column) {
        return column.containsConstraint(ColumnConstraintForeignKey.class);
    }
}
