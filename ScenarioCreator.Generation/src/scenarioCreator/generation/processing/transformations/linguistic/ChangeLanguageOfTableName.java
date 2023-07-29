package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ChangeLanguageOfTableName implements TableTransformation {

    private final Translation _translation;

    public ChangeLanguageOfTableName(Translation translation) {
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
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        if (!canBeTranslated(table)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        final var tableSet = _translation.translate(table.name(), random)
                .map(newName -> SSet.of(table.withName(newName)))
                .orElse(SSet.of(table));
        final List<TupleGeneratingDependency> tgdList = List.of(); // Namen werden nach dem Parsen der Instanzdaten eh vergessen
        return new Pair<>(tableSet, tgdList);
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream().filter(this::canBeTranslated).collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean canBeTranslated(Table table) {
        return _translation.canBeTranslated(table.name());
    }
}
