package de.mrobohm.operations;

import de.mrobohm.data.Schema;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public interface SchemaTransformation extends Transformation {

    @Contract(pure = true)
    boolean conservesFlatRelations();

    @Contract(pure = true)
    @NotNull
    Schema transform(Schema schema, Random random);

    @Contract(pure = true)
    boolean isExecutable(Schema schema);
}