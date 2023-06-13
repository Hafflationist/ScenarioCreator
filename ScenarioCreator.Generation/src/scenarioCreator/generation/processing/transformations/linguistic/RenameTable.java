package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Language;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.function.Function;

public class RenameTable implements TableTransformation {

    private final UnifiedLanguageCorpus _unifiedLanguageCorpus;

    public RenameTable(UnifiedLanguageCorpus unifiedLanguageCorpus) {
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
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var newName = getNewName(table.name(), random);
        final var newTableSet = SSet.of(table.withName(newName));
        final List<TupleGeneratingDependency> tgdList = List.of(); //TODO: tgds
        return new Pair<>(newTableSet, tgdList);
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
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet;
    }
}