package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.Translation;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
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
    @NotNull
    public Set<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        if (!canBeTranslated(table)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        return _translation.translate(table.name(), random)
                .map(newName -> Set.of(table.withName(newName)))
                .orElse(Set.of(table));
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream().filter(this::canBeTranslated).collect(Collectors.toSet());
    }

    private boolean canBeTranslated(Table table) {
        return _translation.canBeTranslated(table.name());
    }
}