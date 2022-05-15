package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Random;
import java.util.Set;

public class RenameTable implements TableTransformation {

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet, Random random) {
        var newName = getNewName(table.name(), random);
        return Collections.singleton(table.withName(newName));
    }

    @NotNull
    private StringPlus getNewName(StringPlus name, Random random) {
        // TODO: Hier könnte WordNet oder GermaNet verwendet werden, um in den Synsets nach Synonamen zu schauen...
        // Erweitern ließe sich das Vorgehen mithilfe von Tokenisierung.
        // Solange dies noch nicht implmentiert ist, wird hier erstmal eine zufällige Zeichenkette gewählt:
        return new StringPlus("Spalte" + random.nextInt(), Language.Technical);
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet;
    }
}