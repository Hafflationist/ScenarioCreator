package de.mrobohm.heterogenity;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.StreamExtensions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StructuralDistanceMeasure {
    private StructuralDistanceMeasure() {
    }
    // TODO: Man muss den Vergleich zwischen arbiträren Schemata S1 und S2 ermöglichen.
    // Grötenteils gilt (S1 - Root) + (S2 - Root) = S1 - S2
    // Das Ziel müsste es sein nach der Kürzung des Id-Baums, analoge Operationen zu streichen, da sie sich ausgleichen.

    public static double calculateDistanceToRootRelative(Schema root, Schema schema) {
        var distanceAbsolute = calculateDistanceToRootAbsolute(root, schema);
        var rootSize = IdentificationNumberCalculator.getAllIds(root, true).count();
        var schemaSize = IdentificationNumberCalculator.getAllIds(schema, true).count();
        return (2.0 * distanceAbsolute) / (double)(rootSize + schemaSize);
    }

    public static int calculateDistanceToRootAbsolute(Schema root, Schema schema) {
        var rootIdSet = IdentificationNumberCalculator
                .getAllIds(root, true)
                .collect(Collectors.toSet());

        var isRealRoot = rootIdSet.stream().allMatch(id -> id instanceof IdSimple);
        assert isRealRoot : "root schema must contain only IdSimple!";

        var rootSimpleIdSet = IdentificationNumberCalculator
                .extractIdSimple(rootIdSet.stream())
                .map(IdSimple::number)
                .collect(Collectors.toSet());

        var schemaSimpleIdSet = IdentificationNumberCalculator
                .extractIdSimple(IdentificationNumberCalculator.getAllIds(schema, true))
                .map(IdSimple::number)
                .collect(Collectors.toSet());

        var dropCount = (int) rootSimpleIdSet.stream().filter(idNum -> !schemaSimpleIdSet.contains(idNum)).count();
        var createCount = (int) schemaSimpleIdSet.stream().filter(idNum -> !rootSimpleIdSet.contains(idNum)).count();

        var schemaPathMap = getIdPathSet(schema);
        var modificationCount = schemaPathMap.values().stream().mapToInt(StructuralDistanceMeasure::diffPath).sum();

        return modificationCount + dropCount + createCount;
    }

    public static Map<Id, List<Id>> getIdPathSet(Schema schema) {
        var rootId = schema.id();
        return IdentificationNumberCalculator
                .getAllIds(schema, false)
                .collect(Collectors.toMap(id -> id,
                        id -> {
                            var tablePath = schema.tableSet().stream().flatMap(t -> {
                                if (t.id().equals(id)) {
                                    return Stream.of(t.id()); // Entity found! Id path finished!
                                }
                                var path = getIdPath(t.columnList(), id).toList();
                                if (path.isEmpty()) {
                                    return Stream.of();
                                }
                                return Stream.concat(Stream.of(t.id()), path.stream());
                            }).toList();
                            if (tablePath.isEmpty()) {
                                return List.of();
                            }
                            return Stream.concat(Stream.of(rootId), tablePath.stream()).toList();
                        }));
    }

    private static Stream<Id> getIdPath(List<Column> columnList, Id id) {
        return getIdPathAcc(columnList, id, Stream.of());
    }

    private static Stream<Id> getIdPathAcc(List<Column> columnList, Id id, Stream<Id> acc) {
        return columnList.stream()
                .flatMap(column -> {
                    var newPath = StreamExtensions.prepend(acc, column.id());
                    if (column.id().equals(id)) {
                        return newPath;
                    }
                    return switch (column) {
                        case ColumnLeaf ignore -> Stream.of();
                        case ColumnNode node -> getIdPathAcc(node.columnList(), id, newPath);
                        case ColumnCollection col -> getIdPathAcc(col.columnList(), id, newPath);
                    };
                });
    }

    private static int diffPath(List<Id> path) {
        var pathShortened = path.stream().map(StructuralDistanceMeasure::shorten).toList();
        return pathShortened.stream().mapToInt(StructuralDistanceMeasure::diffId).sum();
    }

    private static int diffId(Id id) {
        // Eine ID ist nichts anderes als ein Baum.
        // Der Abstand zur Wurzel ist gegeben durch die Anzahl der Knoten des Id-Baums
        // Dies müsste die Anzahl der Operationen darstellen, die zur aktuellen Situation geführt haben.
        return switch (id) {
            case IdSimple ignore -> 0;
            case IdPart idp -> 1 + diffId(idp.predecessorId());
            case IdMerge idm -> 1 + diffId(idm.predecessorId1()) + diffId(idm.predecessorId2());
        };
    }

    private static Id shorten(Id id) {
        // TODO: Id kürzen/normalisieren
        return switch (id) {
            case IdSimple ids -> ids;   // unironisch
            case IdPart idp -> idp;
            case IdMerge idm -> idm;

        };
    }
}
