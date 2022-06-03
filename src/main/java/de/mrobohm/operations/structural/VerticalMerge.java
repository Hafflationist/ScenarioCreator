package de.mrobohm.operations.structural;

import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;

public class VerticalMerge implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet, Function<Integer, int[]> idGenerator, Random random) {
        // TODO: Implement me!
        throw new RuntimeException("Implement me!");
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        // TODO: Implement me!
        return Set.of();
    }
}