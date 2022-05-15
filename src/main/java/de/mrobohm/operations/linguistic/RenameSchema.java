package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.operations.SchemaTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public class RenameSchema implements SchemaTransformation {

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        var newName = getNewName(schema.name(), random);
        return schema.withName(newName);
    }

    @NotNull
    private StringPlus getNewName(StringPlus name, Random random) {
        // TODO: Hier könnte WordNet oder GermaNet verwendet werden, um in den Synsets nach Synonamen zu schauen...
        // Erweitern ließe sich das Vorgehen mithilfe von Tokenisierung.
        // Solange dies noch nicht implmentiert ist, wird hier erstmal eine zufällige Zeichenkette gewählt:
        return new StringPlus("Spalte" + random.nextInt(), Language.Technical);
    }
}