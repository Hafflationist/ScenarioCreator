package de.mrobohm.operations;

import de.mrobohm.data.Schema;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface SchemaTransformation {

    @Contract(pure = true)
    @NotNull
    Schema transform(Schema schema);
}