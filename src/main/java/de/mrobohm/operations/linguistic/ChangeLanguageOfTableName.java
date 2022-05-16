package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class ChangeLanguageOfTableName implements TableTransformation {

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