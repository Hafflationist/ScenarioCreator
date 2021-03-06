package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.Translation;
import de.mrobohm.utils.SSet;
import org.jetbrains.annotations.NotNull;

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
    public SortedSet<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        if (!canBeTranslated(table)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        return _translation.translate(table.name(), random)
                .map(newName -> SSet.of(table.withName(newName)))
                .orElse(SSet.of(table));
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