package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.utils.Pair;

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
    public Pair<List<Column>, List<TupleGeneratingDependency>> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        if (!(column instanceof ColumnLeaf leaf)) {
            throw new TransformationCouldNotBeExecutedException("Type of column wasn't ColumnLeaf!");
        }

        final var newDataType = generateNewDataType(leaf.dataType(), random);
        final var newColumnList = List.of(
                (Column) leaf
                        .withDataType(newDataType)
                        .withContext(leaf.context().withNumericalDistribution(NumericalDistribution.getDefault()))
        );
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        return new Pair<>(newColumnList, tgdList);
    }

    private DataType generateNewDataType(DataType dt, Random random) {
        final var newDte = Stream
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
        final var constraintSet = column.constraintSet();
        final var noPrimaryKey = constraintSet.stream().noneMatch(c -> c instanceof ColumnConstraintPrimaryKey);
        final var noForeignKey = constraintSet.stream().noneMatch(c -> c instanceof ColumnConstraintForeignKey);
        final var noForeignKeyInverse = constraintSet.stream().noneMatch(c -> c instanceof ColumnConstraintForeignKeyInverse);
        final var canBeWidened = column instanceof ColumnLeaf leaf && leaf.dataType().dataTypeEnum() != DataTypeEnum.NVARCHAR;
        return noPrimaryKey && noForeignKey && noForeignKeyInverse && canBeWidened;
    }
}