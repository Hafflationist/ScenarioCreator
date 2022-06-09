package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.operations.SchemaTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.linguistic.helpers.Translation;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public class ChangeLanguageOfSchemaName implements SchemaTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        if (schema.name().language() == Language.Technical) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        var newName = Translation.translate(schema.name(), random);
        return schema.withName(newName);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        var lang = schema.name().language();
        return !lang.equals(Language.Technical)
                && !lang.equals(Language.Mixed);
    }
}