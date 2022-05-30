package de.mrobohm.preprocessing;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SemanticSaturation {

    private final UnifiedLanguageCorpus _corpus;

    public SemanticSaturation(UnifiedLanguageCorpus corpus) {
        _corpus = corpus;
    }

    public Schema saturateSemantically(Schema schema) {
        var allNames = gatherAllNames(schema);
        return Saturation.saturate(schema,
                s -> enrichName(s, allNames),
                t -> enrichName(t, allNames),
                c -> enrichName(c, allNames));
    }

    private static Set<String> gatherAllNames(Schema schema) {
        var schemaName = schema.name().rawString();
        return Stream
                .concat(
                        Stream.of(schemaName),
                        schema.tableSet().stream().flatMap(SemanticSaturation::gatherAllNames))
                .collect(Collectors.toSet());
    }

    private static Stream<String> gatherAllNames(Table table) {
        var tableName = table.name().rawString();
        return Stream.concat(
                Stream.of(tableName),
                table.columnList().stream().flatMap(SemanticSaturation::gatherAllNames));
    }

    private static Stream<String> gatherAllNames(Column column) {
        return switch (column) {
            case ColumnLeaf leaf ->
                    Stream.of(leaf.name().rawString());
            case ColumnCollection collection ->
                    collection.columnList().stream().flatMap(SemanticSaturation::gatherAllNames);
            case ColumnNode node ->
                    Stream.concat(
                        Stream.of(node.name().rawString()),
                        node.columnList().stream().flatMap(SemanticSaturation::gatherAllNames));
        };
    }

    private Schema enrichName(Schema schema, Set<String> context) {
        var oldName = schema.name();
        var estimateSynsetIdSet = _corpus.estimateSynsetId(oldName, context);
        var newName = new StringPlusSemantical(oldName, estimateSynsetIdSet);
        return schema.withName(newName);
    }

    private Table enrichName(Table table, Set<String> context) {
        var oldName = table.name();
        var estimateSynsetIdSet = _corpus.estimateSynsetId(oldName, context);
        var newName = new StringPlusSemantical(oldName, estimateSynsetIdSet);
        return table.withName(newName);
    }

    private Column enrichName(Column column, Set<String> context) {
        var oldName = column.name();
        var estimateSynsetIdSet = _corpus.estimateSynsetId(oldName, context);
        var newName = new StringPlusSemantical(oldName, estimateSynsetIdSet);
        return switch (column) {
            case ColumnLeaf leaf -> leaf.withName(newName);
            case ColumnCollection collection -> collection.withColumnList(collection.columnList().stream().map(c -> enrichName(c, context)).toList());
            case ColumnNode node -> node
                    .withColumnList(node.columnList().stream().map(c -> enrichName(c, context)).toList())
                    .withName(newName);
        };
    }
}