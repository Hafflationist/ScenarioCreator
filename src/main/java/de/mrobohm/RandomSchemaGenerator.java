package de.mrobohm;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.Encoding;
import de.mrobohm.data.column.UnitOfMeasure;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
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

    private static Column generateRandomColumn(Random random) {
        var context = generateRandomContext(random);
        var columnContext = new ColumnContext(context, Encoding.UTF, UnitOfMeasure.Pure, pickRandomLanguage(random));
        return new ColumnLeaf(
                random.nextInt(),
                new StringPlusNaked("Spalte" + random.nextInt(), pickRandomLanguage(random)),
                DataType.NVARCHAR,
                columnContext,
                new HashSet<>()
        );
    }

    private static Table generateRandomTable(Random random, int maxColumns) {
        var context = generateRandomContext(random);
        return new Table(random.nextInt(),
            new StringPlusNaked("Tabelle" + random.nextInt(), pickRandomLanguage(random)),
            generateRandomList(2, maxColumns, RandomSchemaGenerator::generateRandomColumn, random),
            context,
            new HashSet<>()
        );
    }

    public static Schema generateRandomSchema(Random random, int maxTables, int maxColumn) {
        var context = generateRandomContext(random);
        var tableSet = generateRandomSet(1, maxTables, r -> generateRandomTable(r, maxColumn), random);
        return new Schema(random.nextInt(), new StringPlusNaked("Schema" + random.nextInt(), pickRandomLanguage(random)), context, tableSet);
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