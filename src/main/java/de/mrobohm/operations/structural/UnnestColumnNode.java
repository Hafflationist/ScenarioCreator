package de.mrobohm.operations.structural;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;

public class UnnestColumnNode implements ColumnTransformation {
    @Override
    @NotNull
    public List<Column> transform(Column column, Random random) {
        if (!(column instanceof ColumnNode node)) {
            throw new TransformationCouldNotBeExecutedException("Type of column wasn't ColumnNode!");
        }

        return node.columnList();
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::canBeUnested).toList();
    }

    private boolean canBeUnested(Column column) {
        return column instanceof ColumnLeaf;
    }
}
