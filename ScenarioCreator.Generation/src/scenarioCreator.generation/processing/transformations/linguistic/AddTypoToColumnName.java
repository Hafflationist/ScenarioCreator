package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.CharBase;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class AddTypoToColumnName implements ColumnTransformation {

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
        final var newName = CharBase.introduceTypo(column.name(), random);
        final var newColumn = switch (column) {
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
                .filter(column -> column.name().rawString(LinguisticUtils::merge).length() > 0)
                .toList();
    }
}