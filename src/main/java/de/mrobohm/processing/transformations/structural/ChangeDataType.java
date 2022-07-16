package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.Id;
import de.mrobohm.processing.transformations.ColumnTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
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
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
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

    private boolean isValid(Column column) {
        var constraintSet = column.constraintSet();
        var noPrimaryKey = constraintSet.stream().noneMatch(c -> c instanceof ColumnConstraintPrimaryKey);
        var noForeignKey = constraintSet.stream().noneMatch(c -> c instanceof ColumnConstraintForeignKey);
        var noForeignKeyInverse = constraintSet.stream().noneMatch(c -> c instanceof ColumnConstraintForeignKeyInverse);
        var canBeWidened = column instanceof ColumnLeaf leaf && leaf.dataType().dataTypeEnum() != DataTypeEnum.NVARCHAR;
        return noPrimaryKey && noForeignKey && noForeignKeyInverse && canBeWidened;
    }
}