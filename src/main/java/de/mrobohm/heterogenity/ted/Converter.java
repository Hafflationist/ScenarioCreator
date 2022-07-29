package de.mrobohm.heterogenity.ted;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.table.Table;

import java.io.IOException;
import java.util.stream.Collectors;

final class Converter {
    private Converter() {
    }

    static TedTree schemaToTedTree(Schema schema) throws IOException {
        var preorderSchema = schemaToPreorderNotation(schema);
        return new TedTree(preorderSchema);
    }

    private static String idToLabel(Id id) {
        return switch(id) {
            case IdSimple ids -> "i" + ids.number();
            case IdPart idp -> "q" + idToLabel(idp.predecessorId()) + "p" + idp.extensionNumber();
            case IdMerge idm -> idToLabel(idm.predecessorId1()) + idToLabel(idm.predecessorId2());
        };
    }

    private static String schemaToPreorderNotation(Schema schema) {
        var schemaLabel = idToLabel(schema.id());
        var childLabelList = schema.tableSet().stream()
                .map(Converter::tableToPreorderNotation)
                .collect(Collectors.joining(" "));
        var childPart = schema.tableSet().isEmpty() ? "" : "(" + childLabelList + ")";
        return schemaLabel + childPart;
    }

    private static String tableToPreorderNotation(Table table) {
        var tableLabel = idToLabel(table.id());
        var childLabelList = table.columnList().stream()
                .map(Converter::columnToPreorderNotation)
                .collect(Collectors.joining(" "));
        var childPart = table.columnList().isEmpty() ? "" : "(" + childLabelList + ")";
        return tableLabel + childPart;
    }

    private static String columnToPreorderNotation(Column column) {
        return switch (column) {
            case ColumnLeaf leaf -> idToLabel(leaf.id());
            case ColumnNode node -> {
                var columnLabel = idToLabel(node.id());
                var childLabelList = node.columnList().stream()
                        .map(Converter::columnToPreorderNotation)
                        .collect(Collectors.joining(" "));
                var childPart = node.columnList().isEmpty() ? "" : "(" + childLabelList + ")";
                yield columnLabel + childPart;
            }
            case ColumnCollection col -> {
                var columnLabel = idToLabel(col.id());
                var childLabelList = col.columnList().stream()
                        .map(Converter::columnToPreorderNotation)
                        .collect(Collectors.joining(" "));
                var childPart = col.columnList().isEmpty() ? "" : "(" + childLabelList + ")";
                yield columnLabel + childPart;
            }
        };
    }


}
