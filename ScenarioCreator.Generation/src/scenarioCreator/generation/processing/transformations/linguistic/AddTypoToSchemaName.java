package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.CharBase;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.Pair;

import java.util.List;
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
    public Pair<Schema, List<TupleGeneratingDependency>> transform(Schema schema, Random random) {
        final var newName = CharBase.introduceTypo(schema.name(), random);
        final var newSchema = schema.withName(newName);
        final List<TupleGeneratingDependency> tgdList = List.of(); // Namen werden nach dem Parsen der Instanzdaten eh vergessen
        return new Pair<>(newSchema, tgdList);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return schema.name().rawString(LinguisticUtils::merge).length() > 0;
    }
}
