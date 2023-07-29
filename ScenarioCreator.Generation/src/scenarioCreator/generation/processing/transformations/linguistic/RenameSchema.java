package scenarioCreator.generation.processing.transformations.linguistic;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.SchemaTransformation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Random;

public class RenameSchema implements SchemaTransformation {

    private final UnifiedLanguageCorpus _unifiedLanguageCorpus;

    public RenameSchema(UnifiedLanguageCorpus unifiedLanguageCorpus) {
        _unifiedLanguageCorpus = unifiedLanguageCorpus;
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
        final var newName = getNewName(schema.name(), random);
        final var newSchema = schema.withName(newName);
    final List<TupleGeneratingDependency> tgdList = List.of(); // Namen werden nach dem Parsen der Instanzdaten eh vergessen
        return new Pair<>(newSchema, tgdList);
    }

    @NotNull
    private StringPlus getNewName(StringPlus name, Random random) {
        final var newNameOptional = _unifiedLanguageCorpus.synonymizeRandomToken(name, random);
        if (newNameOptional.isEmpty()) {
            return new StringPlusNaked("Schema" + random.nextInt(), Language.Technical);
        }
        return newNameOptional.get();
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return !schema.name().language().equals(Language.Technical);
    }
}
