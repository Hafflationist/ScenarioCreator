package de.mrobohm.preprocessing;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SemanticSaturation {

    private final UnifiedLanguageCorpus _corpus;

    public SemanticSaturation(UnifiedLanguageCorpus corpus) {
        _corpus = corpus;
    }

    private static Set<StringPlus> gatherAllNames(Schema schema) {
        var schemaName = schema.name();
        return Stream
                .concat(
                        Stream.of(schemaName),
                        schema.tableSet().stream().flatMap(SemanticSaturation::gatherAllNames))
                .collect(Collectors.toSet());
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
                .collect(Collectors.toSet());

        return Saturation.saturate(schema,
                s -> enrichName(s, allNames),
                t -> enrichName(t, allNames),
                c -> enrichName(c, allNames));
    }

    public StringPlusSemantical saturateSemantically(StringPlusNaked spn, Set<String> context) {
        return StringPlusSemantical.of(
                spn,
                s -> _corpus.estimateSynsetId(s, context)
        );
    }

    private Schema enrichName(Schema schema, Set<String> context) {
        var oldName = schema.name();
        var newName = StringPlusSemantical.of(
                oldName,
                s -> _corpus.estimateSynsetId(s, context)
        );
        return schema.withName(newName);
    }

    private Table enrichName(Table table, Set<String> context) {
        var oldName = table.name();
        var newName = StringPlusSemantical.of(
                oldName,
                s -> _corpus.estimateSynsetId(s, context)
        );
        return table.withName(newName);
    }

    private Column enrichName(Column column, Set<String> context) {
        var oldName = column.name();
        var newName = StringPlusSemantical.of(
                oldName,
                s -> _corpus.estimateSynsetId(s, context)
        );
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withName(newName);
            case ColumnCollection collection ->
                    collection.withColumnList(collection.columnList().stream().map(c -> enrichName(c, context)).toList());
            case ColumnNode node -> node
                    .withColumnList(node.columnList().stream().map(c -> enrichName(c, context)).toList())
                    .withName(newName);
        };
    }
}