package de.mrobohm.operations;

public sealed interface Transformation permits ColumnTransformation, TableTransformation, SchemaTransformation {
}
