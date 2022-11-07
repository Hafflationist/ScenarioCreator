package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.CharBase;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;

import java.util.Random;

public class AddTypoToSchemaName implements SchemaTransformation {

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
        final var newName = CharBase.introduceTypo(schema.name(), random);
        return schema.withName(newName);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return schema.name().rawString(LinguisticUtils::merge).length() > 0;
    }
}