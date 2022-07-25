package de.mrobohm.processing.preprocessing;

import de.mrobohm.data.Entity;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SemanticSaturation {

    private final UnifiedLanguageCorpus _corpus;

    public SemanticSaturation(UnifiedLanguageCorpus corpus) {
        _corpus = corpus;
    }

    private static SortedSet<StringPlus> gatherAllNames(Schema schema) {
        var schemaName = schema.name();
        return Stream
                .concat(
                        Stream.of(schemaName),
                        schema.tableSet().stream().flatMap(SemanticSaturation::gatherAllNames))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static Stream<StringPlus> gatherAllNames(Table table) {
        var tableName = table.name();
        return Stream.concat(
                Stream.of(tableName),
                table.columnList().stream().flatMap(SemanticSaturation::gatherAllNames));
    }

    private static Stream<StringPlus> gatherAllNames(Column column) {
        return switch (column) {
            case ColumnLeaf leaf -> Stream.of(leaf.name());
            case ColumnCollection collection ->
                    collection.columnList().stream().flatMap(SemanticSaturation::gatherAllNames);
            case ColumnNode node -> Stream.concat(
                    Stream.of(node.name()),
                    node.columnList().stream().flatMap(SemanticSaturation::gatherAllNames));
        };
    }

    public Schema saturateSemantically(Schema schema) {
        var allNames = gatherAllNames(schema).stream()
                .flatMap(s -> LinguisticUtils.tokenize(s).stream())
                .collect(Collectors.toCollection(TreeSet::new));

        return Saturation.saturate(schema,
                s -> enrichName(s, allNames),
                t -> enrichName(t, allNames),
                c -> enrichName(c, allNames));
    }

    private <T extends Entity> T enrichName(T entity, SortedSet<String> context, Function<StringPlusSemantical, T> enricher) {
        return switch (entity.name()) {
            case StringPlusSemantical ignore -> entity;
            case StringPlusNaked oldName -> {
                var newName = StringPlusSemantical.of(
                        oldName,
                        s -> _corpus.estimateSynsetId(s, context)
                );
                yield enricher.apply(newName);
            }
        };
    }

    private Schema enrichName(Schema schema, SortedSet<String> context) {
        return enrichName(schema, context, schema::withName);
    }

    private Table enrichName(Table table, SortedSet<String> context) {
        return enrichName(table, context, table::withName);
    }

    private Column enrichName(Column column, SortedSet<String> context) {
        return enrichName(column, context, sps -> switch (column) {
            case ColumnLeaf leaf -> leaf.withName(sps);
            case ColumnCollection collection ->
                    collection.withColumnList(collection.columnList().stream().map(c -> enrichName(c, context)).toList());
            case ColumnNode node -> node
                    .withColumnList(node.columnList().stream().map(c -> enrichName(c, context)).toList())
                    .withName(sps);
        });
    }

    public StringPlusSemantical saturateSemantically(StringPlusNaked spn, SortedSet<String> context) {
        return StringPlusSemantical.of(
                spn,
                s -> _corpus.estimateSynsetId(s, context)
        );
    }
}