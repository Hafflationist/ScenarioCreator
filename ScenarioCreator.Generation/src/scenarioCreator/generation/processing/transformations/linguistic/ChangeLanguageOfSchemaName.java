package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;

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
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        if (!isExecutable(schema)) {
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