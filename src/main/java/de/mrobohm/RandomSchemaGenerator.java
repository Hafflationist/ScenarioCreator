package de.mrobohm;

import de.mrobohm.data.*;
import de.mrobohm.data.column.*;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;

import java.util.*;
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
        var language = pickRandomLanguage(random);
        return new Context(new SemanticDomain(1), language);
    }

    private static Column generateRandomColumn(Random random, Function<Random, String> nameGenerator) {
        var context = generateRandomContext(random);
        var columnContext = new ColumnContext(context, Encoding.UTF, UnitOfMeasure.Pure, pickRandomLanguage(random));
        return new ColumnLeaf(
                new IdSimple(random.nextInt()),
                new StringPlusNaked("Spalte_" + nameGenerator.apply(random), pickRandomLanguage(random)),
                new DataType(DataTypeEnum.NVARCHAR, random.nextBoolean()),
                Set.of(new Value("1"), new Value("2"), new Value("3")),
                columnContext,
                new HashSet<>()
        );
    }

    private static Table generateRandomTable(Random random, int maxColumns, Function<Random, String> nameGenerator) {
        var context = generateRandomContext(random);
        return new Table(
                new IdSimple(random.nextInt()),
                new StringPlusNaked("tabelle-" + nameGenerator.apply(random), pickRandomLanguage(random)),
                generateRandomList(
                        2,
                        maxColumns,
                        r -> RandomSchemaGenerator.generateRandomColumn(r, nameGenerator),
                        random
                ),
                context,
                new HashSet<>()
        );
    }

    public static Schema generateRandomSchema(
            Random random, int maxTables, int maxColumn, Function<Random, String> nameGenerator
    ) {
        var context = generateRandomContext(random);
        var tableSet = generateRandomSet(1, maxTables, r -> generateRandomTable(r, maxColumn, nameGenerator), random);
        return new Schema(
                new IdSimple(random.nextInt()),
                new StringPlusNaked(
                        "SCHEMA_" + nameGenerator.apply(random).toUpperCase(), pickRandomLanguage(random)
                ),
                context,
                tableSet
        );
    }

    private static <T> List<T> generateRandomList(int min, int max, Function<Random, T> elementGenerator, Random random) {
        var size = random.nextLong(min, max + 1);
        return Stream.generate(() -> random).map(elementGenerator).limit(size).toList();
    }

    private static <T> Set<T> generateRandomSet(int min, int max, Function<Random, T> elementGenerator, Random random) {
        var size = random.nextLong(min, max + 1);
        return Stream.generate(() -> random).map(elementGenerator).limit(size).collect(Collectors.toSet());
    }

}