package de.mrobohm.transformations.linguistic;

import de.mrobohm.data.Schema;
import de.mrobohm.transformations.SchemaTransformation;
import de.mrobohm.transformations.linguistic.helpers.CharBase;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public class AddTypoToSchemaName implements SchemaTransformation {

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        var newName = CharBase.introduceTypo(schema.name(), random);
        return schema.withName(newName);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return schema.name().rawString().length() > 0;
    }
}