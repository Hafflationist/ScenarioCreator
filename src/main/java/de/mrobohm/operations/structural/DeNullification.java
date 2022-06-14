package de.mrobohm.operations.structural;

import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class DeNullification implements ColumnTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, int[]> idGenerator, Random random) {
        if (!isValid(column)) {
            throw new TransformationCouldNotBeExecutedException("Column is not valid!");
        }

        var newColumn = switch (column) {
            case ColumnLeaf leaf -> leaf.withDataType(leaf.dataType().withIsNullable(false));
            case ColumnCollection col -> col.withIsNullable(false);
            default -> throw new TransformationCouldNotBeExecutedException("Column is not valid!");
        };

        return List.of(newColumn);
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::isValid).toList();
    }

    private boolean isValid(Column column) {
        var isNotNode = !(column instanceof ColumnNode);
        var isNullable = column.isNullable();
        var isNotForeignKey = column.constraintSet().stream().noneMatch(c -> c instanceof ColumnConstraintForeignKey);
        return isNotNode && isNullable && isNotForeignKey;
    }
}