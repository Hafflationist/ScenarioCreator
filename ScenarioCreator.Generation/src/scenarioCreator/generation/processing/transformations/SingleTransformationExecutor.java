package scenarioCreator.generation.processing.transformations;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.generation.processing.preprocessing.SemanticSaturation;
import scenarioCreator.generation.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import scenarioCreator.generation.processing.transformations.exceptions.NoColumnFoundException;
import scenarioCreator.generation.processing.transformations.exceptions.NoTableFoundException;
import scenarioCreator.generation.processing.transformations.structural.generator.IdentificationNumberGenerator;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SingleTransformationExecutor {

    @Nullable
    private final SemanticSaturation _semanticSaturation;

    public SingleTransformationExecutor(@Nullable SemanticSaturation semanticSaturation) {
        _semanticSaturation = semanticSaturation;
    }

    public Pair<Schema, List<TupleGeneratingDependency>> executeTransformation(Schema schema, Transformation transformation, Random random)
            throws NoTableFoundException, NoColumnFoundException {
        return executeTransformation(schema, transformation, random, true);
    }

    @Contract(pure = true)
    @NotNull
    public Pair<Schema, List<TupleGeneratingDependency>> executeTransformation(Schema schema, Transformation transformation, Random random, boolean debug)
            throws NoTableFoundException, NoColumnFoundException {
        if (!SingleTransformationChecker.checkTransformation(schema, transformation)) {
            throw new NoTableFoundException("SingleTransformationChecker.checkTransformation dais \"no!\"");
        }
        final var transformationName = transformation.getClass().getName();
        if (debug) {
            System.out.println("Executing " + transformationName.substring(54) + " (" + random.nextInt(1000) + ")");
        }
        final var newSchemaWithTgds = switch (transformation) {
            case ColumnTransformation ct -> executeTransformationColumn(schema, ct, random);
            case TableTransformation tt -> executeTransformationTable(schema, tt, random);
            case SchemaTransformation st -> executeTransformationSchema(schema, st, random);
        };
        final var newSchema = newSchemaWithTgds.first();
        if (transformation.breaksSemanticSaturation() && _semanticSaturation != null) {
            return new Pair<>(_semanticSaturation.saturateSemantically(newSchema), newSchemaWithTgds.second());
        }
        return new Pair<>(FunctionalDependencyManager.transClosure(newSchema), newSchemaWithTgds.second());
    }


    @Contract(pure = true)
    @NotNull
    private Pair<Schema, List<TupleGeneratingDependency>> executeTransformationSchema(
            Schema schema, SchemaTransformation transformation, Random random
    ) {
        final var newSchemaWithTgds = transformation.transform(schema, random);
        final var newSchema = newSchemaWithTgds.first();
        IntegrityChecker.assertValidSchema(newSchema);
        final var trivialTgdList = TgdHelper.calculateTrivialTgds(schema, newSchema);
        final var allTgdList = Stream
                .concat(trivialTgdList.stream(), newSchemaWithTgds.second().stream())
                .toList();
        return new Pair<>(newSchema, allTgdList);
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
    private Pair<Schema, List<TupleGeneratingDependency>> executeTransformationTable(Schema schema, TableTransformation transformation, Random random)
            throws NoTableFoundException {
        final var targetTable = chooseTable(transformation.getCandidates(schema.tableSet()), random);
        Function<Integer, Id[]> idGenerator = n -> IdentificationNumberGenerator.generate(schema, n);
        final var newTableSetWithTgds = transformation.transform(targetTable, idGenerator, random);
        final var newSchema = executeTransformationTable(schema, targetTable, newTableSetWithTgds.first());
        IntegrityChecker.assertValidSchema(newSchema);
        final var trivialTgdList = TgdHelper.calculateTrivialTgds(schema, newSchema);
        final var allTgdList = Stream
                .concat(trivialTgdList.stream(), newTableSetWithTgds.second().stream())
                .toList();
        return new Pair<>(newSchema, allTgdList);
    }


    @Contract(pure = true)
    @NotNull
    private Pair<Schema, List<TupleGeneratingDependency>> executeTransformationColumn(Schema schema, ColumnTransformation transformation, Random random)
            throws NoColumnFoundException {
        assert schema != null;

        final var target = chooseColumn(schema, transformation::getCandidates, random);
        final var targetTable = target.first();
        final var targetColumn = target.second();
        Function<Integer, Id[]> idGenerator = n -> IdentificationNumberGenerator.generate(schema, n);
        final var newPartialColumnListWithTgds = transformation.transform(targetColumn, idGenerator, random);

        final var oldColumnStream = targetTable.columnList().stream();
        final var newColumnList = StreamExtensions
                .replaceInStream(oldColumnStream, targetColumn, newPartialColumnListWithTgds.first().stream())
                .toList();
        final var newTableSet = SSet.of(targetTable.withColumnList(newColumnList));
        final var newSchema = executeTransformationTable(schema, target.first(), newTableSet);
        IntegrityChecker.assertValidSchema(newSchema);
        final var trivialTgdList = TgdHelper.calculateTrivialTgds(schema, newSchema);
        final var allTgdList = Stream
                .concat(trivialTgdList.stream(), newPartialColumnListWithTgds.second().stream())
                .toList();
        return new Pair<>(newSchema, allTgdList);
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