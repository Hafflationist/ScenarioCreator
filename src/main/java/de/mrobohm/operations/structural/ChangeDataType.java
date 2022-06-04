package de.mrobohm.operations.structural;

import de.mrobohm.data.DataType;
import de.mrobohm.data.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

public class ChangeDataType implements ColumnTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, int[]> idGenerator, Random random) {
        if (!(column instanceof ColumnLeaf leaf)) {
            throw new TransformationCouldNotBeExecutedException("Type of column wasn't ColumnLeaf!");
        }

        var newDataType = generateNewDataType(leaf.dataType(), random);
        return List.of(leaf.withDataType(newDataType));
    }

    private DataType generateNewDataType(DataType dt, Random random) {
        var newDte = Stream
                .generate(() -> DataTypeEnum.getRandom(random))
                .dropWhile(proposal -> !dt.dataTypeEnum().isSmallerThan(proposal))
                .findFirst()
                .orElse(DataTypeEnum.NVARCHAR);
        return dt.withDataTypeEnum(newDte);
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::isValid).toList();
    }

    public boolean isValid(Column column) {
        var constraintStream = column.constraintSet().stream();
        var noPrimaryKey = constraintStream.noneMatch(c -> c instanceof ColumnConstraintPrimaryKey);
        var noForeignKey = constraintStream.noneMatch(c -> c instanceof ColumnConstraintForeignKey);
        var noForeignKeyInverse = constraintStream.noneMatch(c -> c instanceof ColumnConstraintForeignKeyInverse);
        var canBeWidened = column instanceof ColumnLeaf leaf && leaf.dataType().dataTypeEnum() != DataTypeEnum.NVARCHAR;
        return noPrimaryKey && noForeignKey && noForeignKeyInverse && canBeWidened;
    }
}