package de.mrobohm.processing.transformations;


import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.processing.preprocessing.SemanticSaturation;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.processing.transformations.exceptions.NoColumnFoundException;
import de.mrobohm.processing.transformations.exceptions.NoTableFoundException;
import de.mrobohm.processing.transformations.structural.generator.IdentificationNumberGenerator;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SingleTransformationExecuter {

    @Nullable
    private final SemanticSaturation _semanticSaturation;

    public SingleTransformationExecuter(@Nullable SemanticSaturation semanticSaturation) {
        _semanticSaturation = semanticSaturation;
    }

    @Contract(pure = true)
    @NotNull
    public Schema executeTransformation(Schema schema, Transformation transformation, Random random)
            throws NoTableFoundException, NoColumnFoundException {
        if (!SingleTransformationChecker.checkTransformation(schema, transformation)) {
            throw new NoTableFoundException("SingleTransformationChecker.checkTransformation dais \"no!\"");
        }
        final var newSchema = switch (transformation) {
            case ColumnTransformation ct -> executeTransformationColumn(schema, ct, random);
            case TableTransformation tt -> executeTransformationTable(schema, tt, random);
            case SchemaTransformation st -> executeTransformationSchema(schema, st, random);
        };
        if (transformation.breaksSemanticSaturation() && _semanticSaturation != null) {
            return _semanticSaturation.saturateSemantically(newSchema);
        }
        return FunctionalDependencyManager.transClosure(newSchema);
    }


    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationSchema(
            Schema schema, SchemaTransformation transformation, Random random
    ) {
        final var newSchema = transformation.transform(schema, random);
        IntegrityChecker.assertValidSchema(newSchema);
        return newSchema;
    }


    @Contract(pure = true)
    @NotNull
    private Table chooseTable(SortedSet<Table> tableSet, Random random) throws NoTableFoundException {
        final var tableStream = tableSet.stream();
        return StreamExtensions.pickRandomOrThrow(tableStream, new NoTableFoundException(), random);
    }

    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationTable(Schema schema, Table targetTable, SortedSet<Table> newTableSet) {
        assert schema.tableSet().contains(targetTable);

        final var filteredTableStream = schema.tableSet()
                .stream()
                .filter(table -> !table.equals(targetTable));
        final var newTableStream = Stream.concat(filteredTableStream, newTableSet.stream());
        return schema.withTableSet(newTableStream.collect(Collectors.toCollection(TreeSet::new)));
    }


    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationTable(Schema schema, TableTransformation transformation, Random random)
            throws NoTableFoundException {
        final var targetTable = chooseTable(transformation.getCandidates(schema.tableSet()), random);
        Function<Integer, Id[]> idGenerator = n -> IdentificationNumberGenerator.generate(schema, n);
        final var newTableSet = transformation.transform(targetTable, idGenerator, random);
        final var newSchema = executeTransformationTable(schema, targetTable, newTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
        return newSchema;
    }


    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationColumn(Schema schema, ColumnTransformation transformation, Random random)
            throws NoColumnFoundException {
        assert schema != null;

        final var target = chooseColumn(schema, transformation::getCandidates, random);
        final var targetTable = target.first();
        final var targetColumn = target.second();
        Function<Integer, Id[]> idGenerator = n -> IdentificationNumberGenerator.generate(schema, n);
        final var newPartialColumnStream = transformation.transform(targetColumn, idGenerator, random).stream();

        final var oldColumnStream = targetTable.columnList().stream();
        final var newColumnList = StreamExtensions
                .replaceInStream(oldColumnStream, targetColumn, newPartialColumnStream)
                .toList();
        final var newTableSet = SSet.of(targetTable.withColumnList(newColumnList));
        final var newSchema = executeTransformationTable(schema, target.first(), newTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
        return newSchema;
    }


    @Contract(pure = true)
    @NotNull
    private Pair<Table, Column> chooseColumn(
            Schema schema, Function<List<Column>, List<Column>> getCandidates, Random random
    ) throws NoColumnFoundException {
        assert schema.tableSet().size() > 0;

        final var candidateStream = schema.tableSet()
                .stream()
                .flatMap(t -> getCandidates.apply(t.columnList()).stream().map(column -> new Pair<>(t, column)));
        return StreamExtensions
                .pickRandomOrThrow(candidateStream, new NoColumnFoundException(), random);
    }
}