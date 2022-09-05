package de.mrobohm.processing.transformations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.processing.transformations.SchemaTransformation;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import org.jetbrains.annotations.NotNull;

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
    public Schema transform(Schema schema, Random random) {
        final var newName = getNewName(schema.name(), random);
        return schema.withName(newName);
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