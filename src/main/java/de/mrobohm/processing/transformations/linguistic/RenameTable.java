package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import de.mrobohm.utils.SSet;
import org.jetbrains.annotations.NotNull;

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
    public SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var newName = getNewName(table.name(), random);
        return SSet.of(table.withName(newName));
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