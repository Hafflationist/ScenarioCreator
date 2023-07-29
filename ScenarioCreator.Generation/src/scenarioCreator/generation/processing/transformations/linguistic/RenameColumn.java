package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.column.nesting.ColumnNode;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.ColumnTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class RenameColumn implements ColumnTransformation {

    private static final TransformationCouldNotBeExecutedException TRANSFORMATION_EXCEPTION =
            new TransformationCouldNotBeExecutedException("Column does not have a valid name, which could be changed!");
    private final UnifiedLanguageCorpus _unifiedLanguageCorpus;

    public RenameColumn(UnifiedLanguageCorpus unifiedLanguageCorpus) {
        _unifiedLanguageCorpus = unifiedLanguageCorpus;
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
        if (!hasMeaningfulName(column)) {
            throw TRANSFORMATION_EXCEPTION;
        }
        final var newName = getNewName(column.name(), random);
        final List<TupleGeneratingDependency> tgdList = List.of(); // Namen werden nach dem Parsen der Instanzdaten eh vergessen
        final var newColumnList = switch (column) {
            case ColumnLeaf c -> List.of((Column) c.withName(newName));
            case ColumnNode c -> List.of((Column) c.withName(newName));
            default -> throw new IllegalStateException("Unexpected value: " + column);
        };
        return new Pair<>(newColumnList, tgdList);
    }

    @NotNull
    private StringPlus getNewName(StringPlus name, Random random) {
        final var newNameOptional = _unifiedLanguageCorpus.synonymizeRandomToken(name, random);
        if (newNameOptional.isEmpty()) {
            return new StringPlusNaked("Spalte" + random.nextInt(), Language.Technical);
        }
        return newNameOptional.get();
    }


    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::hasMeaningfulName).toList();
    }

    private boolean hasMeaningfulName(Column column) {
        return !(column instanceof ColumnCollection);
    }
}
