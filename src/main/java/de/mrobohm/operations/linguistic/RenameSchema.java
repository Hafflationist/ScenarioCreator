package de.mrobohm.operations.linguistic;

import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.operations.SchemaTransformation;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public class RenameSchema implements SchemaTransformation {

    private final UnifiedLanguageCorpus _unifiedLanguageCorpus;

    public RenameSchema(UnifiedLanguageCorpus unifiedLanguageCorpus) {
        _unifiedLanguageCorpus = unifiedLanguageCorpus;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        var newName = getNewName(schema.name(), random);
        return schema.withName(newName);
    }

    @NotNull
    private StringPlus getNewName(StringPlus name, Random random) {
        var newNameOptional = _unifiedLanguageCorpus.synonymizeRandomToken(name, random);
        if (newNameOptional.isEmpty()) {
            return new StringPlusNaked("Schema" + random.nextInt(), Language.Technical);
        }
        return newNameOptional.get();
    }
}