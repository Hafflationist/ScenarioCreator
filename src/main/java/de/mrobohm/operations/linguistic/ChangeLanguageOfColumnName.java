package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.operations.ColumnTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.linguistic.helpers.Translation;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;

public class ChangeLanguageOfColumnName implements ColumnTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Random random) {
        if (!canBeTranslated(column)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        var newName = Translation.translate(column.name(), random);
        return switch (column) {
            case ColumnLeaf c -> List.of(c.withName(newName));
            case ColumnNode c -> List.of(c.withName(newName));
            default -> throw new IllegalStateException("Unexpected value: " + column);
        };
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> columnList) {
        return columnList.stream().filter(this::canBeTranslated).toList();
    }

    private boolean canBeTranslated(Column column) {
        return switch (column) {
            case ColumnCollection ignore -> false;
            case ColumnNode node -> node.name().language() != Language.Technical;
            case ColumnLeaf leaf -> leaf.name().language() != Language.Technical;
        };
    }
}