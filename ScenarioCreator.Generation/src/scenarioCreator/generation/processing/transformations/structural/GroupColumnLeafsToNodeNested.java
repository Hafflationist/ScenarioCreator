package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.structural.base.GroupingColumnsBase;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

// TODO: Zurzeit werden hier nur auf der obersten Ebene Spalten zusammengefasst. Theoretisch wäre dies auf beliebigen Ebenen möglich.
public class GroupColumnLeafsToNodeNested implements ColumnTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        final var transEx = new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        if (!(hasColumnNodeOrCollectionGroupableColumns(column))) {
            throw transEx;
        }

        final var columnList = switch (column) {
            case ColumnNode node -> node.columnList();
            case ColumnCollection collection -> collection.columnList();
            default -> throw transEx;
        };

        final var groupableColumnList = GroupingColumnsBase.findGroupableColumns(columnList, random);
        final var newIds = idGenerator.apply(1);
        final var newColumn = GroupingColumnsBase.createNewColumnNode(newIds[0], groupableColumnList, random);

        final var newColumnList = StreamExtensions.replaceInStream(
                columnList.stream(),
                groupableColumnList.stream(),
                newColumn).toList();

        return List.of(switch (column) {
            case ColumnNode node -> node.withColumnList(newColumnList);
            case ColumnCollection collection -> collection.withColumnList(newColumnList);
            default -> throw transEx;
        });
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::hasColumnNodeOrCollectionGroupableColumns).toList();
    }

    private boolean hasColumnNodeOrCollectionGroupableColumns(Column column) {
        return switch (column) {
            case ColumnNode node -> GroupingColumnsBase.containsGroupableColumns(node.columnList());
            case ColumnCollection col -> GroupingColumnsBase.containsGroupableColumns(col.columnList());
            case ColumnLeaf ignore -> false;
        };
    }
}