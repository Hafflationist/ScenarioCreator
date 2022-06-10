package de.mrobohm.operations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.operations.SchemaTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public class HorizontalInheritanceToNullable implements SchemaTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        // TODO: Implement me!
        throw new RuntimeException("Implement me!");
    }

    @Override
    public boolean isExecutable(Schema schema) {
        // TODO: Implement me!
        throw new RuntimeException("Implement me!");
    }
}
