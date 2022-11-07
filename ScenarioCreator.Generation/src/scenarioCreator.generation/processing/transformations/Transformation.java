package scenarioCreator.generation.processing.transformations;

import org.jetbrains.annotations.Contract;

public sealed interface Transformation extends Comparable<Transformation>
        permits ColumnTransformation, TableTransformation, SchemaTransformation {

    @Contract(pure = true)
    boolean conservesFlatRelations();

    @Contract(pure = true)
    boolean breaksSemanticSaturation();

    @Override
    default int compareTo(Transformation trans) {
        return (this.getClass().getName() + this).compareTo(trans.getClass().getName() + trans);
    }
}
