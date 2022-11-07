package scenarioCreator.generation;

import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.SemanticDomain;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.context.Encoding;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.data.column.context.UnitOfMeasure;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.dataset.Value;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RandomSchemaGenerator {

    private static Language pickRandomLanguage(Random random) {
        return switch (random.nextInt(0, 3)) {
            case 0 -> Language.German;
            case 1 -> Language.English;
            default -> Language.Mixed;
        };
    }

    private static Context generateRandomContext(Random random) {
        final var language = pickRandomLanguage(random);
        return new Context(new SemanticDomain(1), language);
    }

    private static Column generateRandomColumn(Random random, Function<Random, String> nameGenerator) {
        final var context = generateRandomContext(random);
        final var columnContext = new ColumnContext(
                context, Encoding.UTF, UnitOfMeasure.Pure, pickRandomLanguage(random), NumericalDistribution.getDefault()
        );
        return new ColumnLeaf(
                new IdSimple(random.nextInt(10000)),
                new StringPlusNaked("Spalte_" + nameGenerator.apply(random), pickRandomLanguage(random)),
                new DataType(DataTypeEnum.NVARCHAR, random.nextBoolean()),
                SSet.of(new Value("1"), new Value("2"), new Value("3")),
                columnContext,
                SSet.of()
        );
    }

    private static Table generateRandomTable(Random random, int maxColumns, Function<Random, String> nameGenerator) {
        final var context = generateRandomContext(random);
        return new Table(
                new IdSimple(random.nextInt(10000)),
                new StringPlusNaked("tabelle-" + nameGenerator.apply(random), pickRandomLanguage(random)),
                generateRandomList(
                        2,
                        maxColumns,
                        r -> RandomSchemaGenerator.generateRandomColumn(r, nameGenerator),
                        random
                ),
                context,
                SSet.of(),
                SSet.of()
        );
    }

    public static Schema generateRandomSchema(
            Random random, int maxTables, int maxColumn, Function<Random, String> nameGenerator
    ) {
        final var context = generateRandomContext(random);
        final var tableSet = generateRandomSet(1, maxTables, r -> generateRandomTable(r, maxColumn, nameGenerator), random);
        return new Schema(
                new IdSimple(random.nextInt(10000)),
                new StringPlusNaked(
                        "SCHEMA_" + nameGenerator.apply(random).toUpperCase(), pickRandomLanguage(random)
                ),
                context,
                tableSet
        );
    }

    private static <T> List<T> generateRandomList(int min, int max, Function<Random, T> elementGenerator, Random random) {
        final var size = random.nextLong(min, max + 1);
        return Stream.generate(() -> random).map(elementGenerator).limit(size).toList();
    }

    private static <T> SortedSet<T> generateRandomSet(int min, int max, Function<Random, T> elementGenerator, Random random) {
        final var size = random.nextLong(min, max + 1);
        return Stream.generate(() -> random).map(elementGenerator).limit(size).collect(Collectors.toCollection(TreeSet::new));
    }

}