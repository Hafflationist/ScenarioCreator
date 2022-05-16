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

public class ChangeLanguageOfColumn implements ColumnTransformation {

    @Override
    @NotNull
    public List<Column> transform(Column column, Random random) {
        if (!canBeTranslated(column)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        var newName = getNewStringPlus(column.name(), random);
        return switch (column) {
            case ColumnLeaf c -> List.of(c.withName(newName));
            case ColumnNode c -> List.of(c.withName(newName));
            default -> throw new IllegalStateException("Unexpected value: " + column);
        };
    }

    @NotNull
    private StringPlus getNewStringPlus(StringPlus name, Random random) {
        return switch (name.language()) {
            case English:
                var germanRawString = translate(name.rawString(), Language.German, random);
                // TODO: get translation
                yield new StringPlus(germanRawString, Language.German);

            case German:
                var englishRawString = translate(name.rawString(), Language.English, random);
                // TODO: get translation
                yield new StringPlus(englishRawString, Language.English);

            case Mixed:
                var newLanguage = (random.nextInt() % 2 == 0) ? Language.German : Language.English;
                var newRawString = name.rawString();
                // TODO: get translation
                yield new StringPlus(newRawString, newLanguage);

            default:
                throw new IllegalStateException("Unexpected value: " + name.language());
        };
    }

    @NotNull
    private String translate(String string, Language targetLanguage, Random random) {
        // TODO: Use a lib
        return string;
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