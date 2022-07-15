package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.processing.transformations.linguistic.helpers.CharBase;
import de.mrobohm.processing.transformations.ColumnTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class AddTypoToColumnName implements ColumnTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        var newName = CharBase.introduceTypo(column.name(), random);
        var newColumn = switch (column) {
            case ColumnLeaf leaf -> leaf.withName(newName);
            case ColumnNode node -> node.withName(newName);
            case ColumnCollection col -> col.withName(newName);
        };
        return List.of(newColumn);
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream()
                .filter(column -> column.name().rawString().length() > 0)
                .toList();
    }
}