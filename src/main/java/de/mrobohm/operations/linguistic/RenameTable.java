package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Random;
import java.util.Set;

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
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet, Random random) {
        var newName = getNewName(table.name(), random);
        return Collections.singleton(table.withName(newName));
    }

    @NotNull
    private StringPlus getNewName(StringPlus name, Random random) {
        var newNameOptional = _unifiedLanguageCorpus.synonymizeRandomToken(name, random);
        if (newNameOptional.isEmpty()) {
            return new StringPlusNaked("Spalte" + random.nextInt(), Language.Technical);
        }
        return newNameOptional.get();
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet;
    }
}