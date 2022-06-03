package de.mrobohm.operations.structural;

import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class RemoveTable implements TableTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet, Random random) {
        return new HashSet<>();
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet;
    }
}
