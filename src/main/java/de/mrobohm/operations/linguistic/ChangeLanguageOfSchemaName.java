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
    @NotNull
    public Schema transform(Schema schema, Random random) {
        if (schema.name().language() == Language.Technical) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        var newName = Translation.translate(schema.name(), random);
        return schema.withName(newName);
    }
}