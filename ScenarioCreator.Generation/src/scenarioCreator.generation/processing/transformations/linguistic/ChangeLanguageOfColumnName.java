package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class ChangeLanguageOfColumnName implements ColumnTransformation {

    private final Translation _translation;

    public ChangeLanguageOfColumnName(Translation translation) {
        _translation = translation;
    }

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
        if (!canBeTranslated(column)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        final var newNameOpt = _translation.translate(column.name(), random);
        if (newNameOpt.isEmpty()) {
            return List.of(column);
        }
        return switch (column) {
            case ColumnLeaf c -> List.of(c.withName(newNameOpt.get()));
            case ColumnNode c -> List.of(c.withName(newNameOpt.get()));
            case ColumnCollection c -> List.of(c.withName(newNameOpt.get()));
        };
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::canBeTranslated).toList();
    }

    private boolean canBeTranslated(Column column) {
        return _translation.canBeTranslated(column.name());
    }
}