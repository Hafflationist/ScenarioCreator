package de.mrobohm.transformations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.transformations.ColumnTransformation;
import de.mrobohm.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class RenameColumn implements ColumnTransformation {

    private static final TransformationCouldNotBeExecutedException TRANSFORMATION_EXCEPTION =
            new TransformationCouldNotBeExecutedException("Column does not have a valid name, which could be changed!");
    private final UnifiedLanguageCorpus _unifiedLanguageCorpus;

    public RenameColumn(UnifiedLanguageCorpus unifiedLanguageCorpus) {
        _unifiedLanguageCorpus = unifiedLanguageCorpus;
    }

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public List<Column> transform(Column column, Function<Integer, Id[]> idGenerator, Random random) {
        if (!hasMeaningfulName(column)) {
            throw TRANSFORMATION_EXCEPTION;
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
        var newNameOptional = _unifiedLanguageCorpus.synonymizeRandomToken(name, random);
        if (newNameOptional.isEmpty()) {
            return new StringPlusNaked("Spalte" + random.nextInt(), Language.Technical);
        }
        return newNameOptional.get();
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