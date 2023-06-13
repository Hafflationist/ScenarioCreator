package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;
import scenarioCreator.utils.Pair;

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
    public Pair<List<Column>, List<TupleGeneratingDependency>> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        if (!canBeTranslated(column)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        final var newNameOpt = _translation.translate(column.name(), random);
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        if (newNameOpt.isEmpty()) {
            return new Pair<>(List.of(column), tgdList);
        }
        return switch (column) {
            case ColumnLeaf c -> new Pair<>(List.of(c.withName(newNameOpt.get())), tgdList);
            case ColumnNode c -> new Pair<>(List.of(c.withName(newNameOpt.get())), tgdList);
            case ColumnCollection c -> new Pair<>(List.of(c.withName(newNameOpt.get())), tgdList);
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