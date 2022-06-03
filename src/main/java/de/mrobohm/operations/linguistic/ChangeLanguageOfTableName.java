package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.linguistic.helpers.Translation;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class ChangeLanguageOfTableName implements TableTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet, Random random) {
        if (!canBeTranslated(table)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        var newName = Translation.translate(table.name(), random);
        return Set.of(table.withName(newName));
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream().filter(this::canBeTranslated).collect(Collectors.toSet());
    }

    private boolean canBeTranslated(Table table) {
        return table.name().language() != Language.Technical;
    }
}