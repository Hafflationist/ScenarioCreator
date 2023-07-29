package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
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

public class UngroupColumnNodeToColumnLeafs implements ColumnTransformation {
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
        if (!(column instanceof ColumnNode node)) {
            throw new TransformationCouldNotBeExecutedException("Type of column wasn't ColumnNode!");
        }
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO(nosql): tgds
        if (node.isNullable()) {
            final var newColumnList = node.columnList().stream().map(co -> {
               if(co.isNullable()) {
                   return co;
               }
               return switch (co) {
                    case ColumnLeaf l -> l.withDataType(l.dataType().withIsNullable(true));
                    case ColumnNode n -> n.withIsNullable(true);
                    case ColumnCollection c -> c.withIsNullable(true);
               };
            }).toList();
            return new Pair<>(newColumnList, tgdList);
        }
        final var newColumnList = node.columnList();
        return new Pair<>(newColumnList, tgdList);
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::canBeUnested).toList();
    }

    private boolean canBeUnested(Column column) {
        return column instanceof ColumnNode;
    }
}
