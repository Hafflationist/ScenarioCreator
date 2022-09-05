package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.linguistic.helpers.CharBase;
import de.mrobohm.utils.SSet;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AddTypoToTableName implements TableTransformation {

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
        final var newName = CharBase.introduceTypo(table.name(), random);
        return SSet.of(table.withName(newName));
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> t.name().rawString().length() > 0)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}