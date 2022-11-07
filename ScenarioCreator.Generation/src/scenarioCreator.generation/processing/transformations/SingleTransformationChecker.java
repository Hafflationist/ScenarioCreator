package scenarioCreator.generation.processing.transformations;


import org.jetbrains.annotations.Contract;
import scenarioCreator.data.Schema;

public final class SingleTransformationChecker {

    private SingleTransformationChecker() {
    }

    @Contract(pure = true)
    public static boolean checkTransformation(Schema schema, Transformation transformation) {
        return switch (transformation) {
            case ColumnTransformation ct -> schema.tableSet().parallelStream()
                    .map(t -> ct.getCandidates(t.columnList()).size() > 0)
                    .reduce((a, b) -> a || b)
                    .orElse(false);
            case TableTransformation tt -> tt.getCandidates(schema.tableSet()).size() > 0;
            case SchemaTransformation st -> st.isExecutable(schema);
        };
    }
}