package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.ColumnTransformation;
import org.jetbrains.annotations.NotNull;

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
    public List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        if (!(column instanceof ColumnNode node)) {
            throw new TransformationCouldNotBeExecutedException("Type of column wasn't ColumnNode!");
        }
        if (node.isNullable()) {
            return node.columnList().stream().map(co -> {
               if(co.isNullable()) {
                   return co;
               }
               return switch (co) {
                    case ColumnLeaf l -> l.withDataType(l.dataType().withIsNullable(true));
                    case ColumnNode n -> n.withIsNullable(true);
                    case ColumnCollection c -> c.withIsNullable(true);
               };
            }).toList();
        }
        return node.columnList();
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