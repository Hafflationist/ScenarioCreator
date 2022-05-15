package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;

public class RenameColumn implements ColumnTransformation {

    @Override
    @NotNull
    public List<Column> transform(Column column, Random random) {
        if (!hasMeaningfulName(column)) {
            throw new TransformationCouldNotBeExecutedException("Column does not have a valid name, which could be changed!");
        }
        var newName = getNewName(column.name(), random);
        return switch (column) {
            case ColumnLeaf c -> List.of(c.withName(newName));
            case ColumnNode c -> List.of(c.withName(newName));
            default -> throw new IllegalStateException("Unexpected value: " + column);
        };
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
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::hasMeaningfulName).toList();
    }

    private boolean hasMeaningfulName(Column column) {
        return !(column instanceof ColumnCollection);
    }
}