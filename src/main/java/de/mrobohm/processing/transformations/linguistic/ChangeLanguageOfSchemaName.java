package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.Schema;
import de.mrobohm.processing.transformations.SchemaTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.Translation;
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