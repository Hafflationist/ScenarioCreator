package de.mrobohm.operations.structural;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.GroupColumnLeafsToNodeBase;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

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
    @NotNull
    public List<Column> transform(Column column, Function<Integer, int[]> idGenerator, Random random) {
        var transEx = new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        if (!(hasColumnNodeOrCollectionGroupableColumns(column))) {
            throw transEx;
        }

        var columnList = switch (column) {
            case ColumnNode node -> node.columnList();
            case ColumnCollection collection -> collection.columnList();
            default -> throw transEx;
        };

        var groupableColumns = GroupColumnLeafsToNodeBase.findGroupableColumns(columnList, random);
        var newIds = idGenerator.apply(1);
        var newColumn = GroupColumnLeafsToNodeBase.createNewColumnNode(newIds[0], groupableColumns, random);

        var newColumnList = StreamExtensions.replaceInStream(
                columnList.stream(),
                groupableColumns.stream(),
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
            case ColumnNode node -> node.columnList().stream().anyMatch(GroupColumnLeafsToNodeBase::areConstraintsFine);
            case ColumnCollection collection ->
                    collection.columnList().stream().anyMatch(GroupColumnLeafsToNodeBase::areConstraintsFine);
            case ColumnLeaf ignore -> false;
        };
    }
}