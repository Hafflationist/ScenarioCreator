package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Schema;
import de.mrobohm.operations.SchemaTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.linguistic.helpers.Translation;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public class ChangeLanguageOfSchemaName implements SchemaTransformation {

    private final Translation _translation;

    public ChangeLanguageOfSchemaName(Translation translation) {
        _translation = translation;
    }


    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        if (isExecutable(schema)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        return _translation.translate(schema.name(), random)
                .map(schema::withName)
                .orElse(schema);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return _translation.canBeTranslated(schema.name());
    }
}