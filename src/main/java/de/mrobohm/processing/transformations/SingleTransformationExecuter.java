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
        if(!SingleTransformationChecker.checkTransformation(schema, transformation)) {
            throw new NoTableFoundException("SingleTransformationChecker.checkTransformation dais \"no!\"");
        }
        var newSchema = switch (transformation) {
            case ColumnTransformation ct -> executeTransformationColumn(schema, ct, random);
            case TableTransformation tt -> executeTransformationTable(schema, tt, random);
            case SchemaTransformation st -> executeTransformationSchema(schema, st, random);
        };
        if (transformation.breaksSemanticSaturation() && _semanticSaturation != null) {
            return _semanticSaturation.saturateSemantically(newSchema);
        }
        return newSchema;
    }


    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationSchema(
            Schema schema, SchemaTransformation transformation, Random random
    ) {
        var newSchema = transformation.transform(schema, random);
        IntegrityChecker.assertValidSchema(newSchema);
        return newSchema;
    }


    @Contract(pure = true)
    @NotNull
    private Table chooseTable(Set<Table> tableSet, Random random) throws NoTableFoundException {
        var tableStream = tableSet.stream();
        return StreamExtensions.pickRandomOrThrow(tableStream, new NoTableFoundException(), random);
    }

    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationTable(Schema schema, Table targetTable, Set<Table> newTableSet) {
        assert schema.tableSet().contains(targetTable);

        var filteredTableStream = schema.tableSet()
                .stream()
                .filter(table -> !table.equals(targetTable));
        var newTableStream = Stream.concat(filteredTableStream, newTableSet.stream());
        return schema.withTables(newTableStream.collect(Collectors.toSet()));
    }


    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationTable(Schema schema, TableTransformation transformation, Random random)
            throws NoTableFoundException {
        var targetTable = chooseTable(transformation.getCandidates(schema.tableSet()), random);
        Function<Integer, Id[]> idGenerator = n -> IdentificationNumberGenerator.generate(schema, n);
        var newTableSet = transformation.transform(targetTable, idGenerator, random);
        var newSchema = executeTransformationTable(schema, targetTable, newTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
        return newSchema;
    }


    @Contract(pure = true)
    @NotNull
    private Schema executeTransformationColumn(Schema schema, ColumnTransformation transformation, Random random)
            throws NoTableFoundException, NoColumnFoundException {
        assert schema != null;

        var target = chooseColumn(schema, transformation::getCandidates, random);
        var targetTable = target.first();
        var targetColumn = target.second();
        Function<Integer, Id[]> idGenerator = n -> IdentificationNumberGenerator.generate(schema, n);
        var newPartialColumnStream = transformation.transform(targetColumn, idGenerator, random).stream();

        var oldColumnStream = targetTable.columnList().stream();
        var newColumnList = StreamExtensions
                .replaceInStream(oldColumnStream, targetColumn, newPartialColumnStream)
                .toList();
        var newTableSet = Collections.singleton(targetTable.withColumnList(newColumnList));
        var newSchema = executeTransformationTable(schema, target.first(), newTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
        return newSchema;
    }


    @Contract(pure = true)
    @NotNull
    private Pair<Table, Column> chooseColumn(
            Schema schema, Function<List<Column>, List<Column>> getCandidates, Random random
    ) throws NoColumnFoundException, NoTableFoundException {
        assert schema.tableSet().size() > 0;

        var candidateTableSet = schema.tableSet()
                .stream()
                .filter(table -> getCandidates.apply(table.columnList()).size() > 0)
                .collect(Collectors.toSet());

        var targetTable = chooseTable(candidateTableSet, random);
        var columnStream = targetTable.columnList().stream();
        var targetColumn = StreamExtensions.pickRandomOrThrow(columnStream, new NoColumnFoundException(), random);
        return new Pair<>(targetTable, targetColumn);
    }
}