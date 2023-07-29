package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class DeNullification implements ColumnTransformation {
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
        if (!isValid(column)) {
            throw new TransformationCouldNotBeExecutedException("Column is not valid!");
        }

        final var newColumn = (Column) switch (column) {
            case ColumnLeaf leaf -> leaf.withDataType(leaf.dataType().withIsNullable(false));
            case ColumnCollection col -> col.withIsNullable(false);
            default -> throw new TransformationCouldNotBeExecutedException("Column is not valid!");
        };
        final var newColumnList = List.of(newColumn);
        final List<TupleGeneratingDependency> tgdList = List.of(); // Hier wird keine tgd-relevante Änderung ausgeführt
        return new Pair<>(newColumnList, tgdList);
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::isValid).toList();
    }

    private boolean isValid(Column column) {
        final var isNotNode = !(column instanceof ColumnNode);
        final var isNullable = column.isNullable();
        final var isNotForeignKey = !column.containsConstraint(ColumnConstraintForeignKey.class);
        return isNotNode && isNullable && isNotForeignKey;
    }
}
