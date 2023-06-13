package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;
import scenarioCreator.utils.Pair;

import java.util.List;
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
    public Pair<Schema, List<TupleGeneratingDependency>> transform(Schema schema, Random random) {
        if (!isExecutable(schema)) {
            throw new TransformationCouldNotBeExecutedException("Name of column cannot be translated!");
        }
        final var newSchema = _translation.translate(schema.name(), random)
                .map(schema::withName)
                .orElse(schema);
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO: tgds
        return new Pair<>(newSchema, tgdList);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return _translation.canBeTranslated(schema.name());
    }
}