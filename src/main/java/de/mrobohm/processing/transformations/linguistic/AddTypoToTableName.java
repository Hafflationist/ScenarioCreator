package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.linguistic.helpers.CharBase;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
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
    public Set<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        var newName = CharBase.introduceTypo(table.name(), random);
        return Set.of(table.withName(newName));
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> t.name().rawString().length() > 0)
                .collect(Collectors.toSet());
    }
}