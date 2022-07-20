package de.mrobohm.processing.transformations;


import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.processing.preprocessing.SemanticSaturation;
import de.mrobohm.processing.transformations.exceptions.NoColumnFoundException;
import de.mrobohm.processing.transformations.exceptions.NoTableFoundException;
import de.mrobohm.processing.transformations.structural.generator.IdentificationNumberGenerator;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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