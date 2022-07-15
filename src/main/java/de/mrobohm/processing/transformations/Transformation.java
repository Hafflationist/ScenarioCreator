package de.mrobohm.processing.transformations;

import org.jetbrains.annotations.Contract;

public sealed interface Transformation permits ColumnTransformation, TableTransformation, SchemaTransformation {

    @Contract(pure = true)
    boolean conservesFlatRelations();
}
