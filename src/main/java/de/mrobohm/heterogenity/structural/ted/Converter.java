package de.mrobohm.heterogenity.structural.ted;

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
        final var preorderSchema = schemaToPreorderNotation(schema);
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
        final var schemaLabel = idToLabel(schema.id());
        final var childLabelList = schema.tableSet().stream()
                .map(Converter::tableToPreorderNotation)
                .collect(Collectors.joining(" "));
        final var childPart = schema.tableSet().isEmpty() ? "" : "(" + childLabelList + ")";
        return schemaLabel + childPart;
    }

    private static String tableToPreorderNotation(Table table) {
        final var tableLabel = idToLabel(table.id());
        final var childLabelList = table.columnList().stream()
                .map(Converter::columnToPreorderNotation)
                .collect(Collectors.joining(" "));
        final var childPart = table.columnList().isEmpty() ? "" : "(" + childLabelList + ")";
        return tableLabel + childPart;
    }

    private static String columnToPreorderNotation(Column column) {
        return switch (column) {
            case ColumnLeaf leaf -> idToLabel(leaf.id());
            case ColumnNode node -> {
                final var columnLabel = idToLabel(node.id());
                final var childLabelList = node.columnList().stream()
                        .map(Converter::columnToPreorderNotation)
                        .collect(Collectors.joining(" "));
                final var childPart = node.columnList().isEmpty() ? "" : "(" + childLabelList + ")";
                yield columnLabel + childPart;
            }
            case ColumnCollection col -> {
                final var columnLabel = idToLabel(col.id());
                final var childLabelList = col.columnList().stream()
                        .map(Converter::columnToPreorderNotation)
                        .collect(Collectors.joining(" "));
                final var childPart = col.columnList().isEmpty() ? "" : "(" + childLabelList + ")";
                yield columnLabel + childPart;
            }
        };
    }


}
