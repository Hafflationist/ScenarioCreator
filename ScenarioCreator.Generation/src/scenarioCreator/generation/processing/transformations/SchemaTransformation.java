package scenarioCreator.generation.processing.transformations;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.Schema;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.utils.Pair;

import java.util.List;
import java.util.Random;

public non-sealed interface SchemaTransformation extends Transformation {

    @Contract(pure = true)
    @NotNull
    Pair<Schema, List<TupleGeneratingDependency>> transform(Schema schema, Random random);

    @Contract(pure = true)
    boolean isExecutable(Schema schema);
}