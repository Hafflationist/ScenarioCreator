package de.mrobohm.heterogenity.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StructuralDistanceMeasureElementary {
    private StructuralDistanceMeasureElementary() {
    }
    // TODO: Man muss den Vergleich zwischen arbiträren Schemata S1 und S2 ermöglichen.
    // Grötenteils gilt (S1 - Root) + (S2 - Root) = S1 - S2
    // Das Ziel müsste es sein nach der Kürzung des Id-Baums, analoge Operationen zu streichen, da sie sich ausgleichen.

    public static double calculateDistanceToRootRelative(Schema root, Schema schema) {
        final var distanceAbsolute = calculateDistanceToRootAbsolute(root, schema);
        final var rootSize = IdentificationNumberCalculator.getAllIds(root, true).count();
        final var schemaSize = IdentificationNumberCalculator.getAllIds(schema, true).count();
        return (2.0 * distanceAbsolute) / (double) (rootSize + schemaSize);
    }

    public static int calculateDistanceToRootAbsolute(Schema root, Schema schema) {
        final var rootIdSet = IdentificationNumberCalculator
                .getAllIds(root, false)
                .collect(Collectors.toCollection(TreeSet::new));

        final var isRealRoot = rootIdSet.stream().allMatch(id -> id instanceof IdSimple);
        assert isRealRoot : "root schema must contain only IdSimple!";

        final var rootSimpleIdSet = IdentificationNumberCalculator
                .extractIdSimple(rootIdSet.stream())
                .map(IdSimple::number)
                .collect(Collectors.toCollection(TreeSet::new));

        final var schemaSimpleIdSet = IdentificationNumberCalculator
                .extractIdSimple(IdentificationNumberCalculator.getAllIds(schema, false))
                .map(IdSimple::number)
                .collect(Collectors.toCollection(TreeSet::new));

        final var dropCount = (int) rootSimpleIdSet.stream().filter(idNum -> !schemaSimpleIdSet.contains(idNum)).count();
        final var createCount = (int) schemaSimpleIdSet.stream().filter(idNum -> !rootSimpleIdSet.contains(idNum)).count();

        final var schemaPathMap = getIdPathSet(schema);
        final var rootPathMap = getIdPathSet(root);
        final var entityModificationCount = diffEntity(schemaPathMap.keySet());
        final var entityNestingChanges = schemaPathMap.keySet().stream()
                .filter(rootPathMap.keySet()::contains)
                .map(id -> new Pair<>(schemaPathMap.get(id), rootPathMap.get(id)))
                .mapToInt(StructuralDistanceMeasureElementary::diffEntityNestingChanges)
                .sum();
        final var modificationCount = entityModificationCount + entityNestingChanges;

        return modificationCount + dropCount + createCount;
    }

    private static Map<Id, List<Id>> getIdPathSet(Schema schema) {
        final var rootId = schema.id();
        return IdentificationNumberCalculator
                .getAllIds(schema, false)
                .collect(Collectors.toMap(id -> id,
                        id -> {
                            final var tablePath = schema.tableSet().stream().flatMap(t -> {
                                if (t.id().equals(id)) {
                                    return Stream.of(t.id()); // Entity found! Id path finished!
                                }
                                final var path = getIdPath(t.columnList(), id).toList();
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
        return getIdPathAcc(columnList, id, List.of());
    }

    private static Stream<Id> getIdPathAcc(List<Column> columnList, Id id, List<Id> acc) {
        return columnList.stream()
                .flatMap(column -> {
                    final var newPath = StreamExtensions.prepend(acc.stream(), column.id()).toList();
                    if (column.id().equals(id)) {
                        return newPath.stream();
                    }
                    return switch (column) {
                        case ColumnLeaf ignore -> Stream.of();
                        case ColumnNode node -> getIdPathAcc(node.columnList(), id, newPath);
                        case ColumnCollection col -> getIdPathAcc(col.columnList(), id, newPath);
                    };
                });
    }

    private static int diffEntity(Set<Id> idSet) {
        return idSet.stream()
                .map(StructuralDistanceMeasureElementary::shorten)
                .mapToInt(StructuralDistanceMeasureElementary::diffId).sum();
    }

    private static int diffEntityNestingChanges(Pair<List<Id>, List<Id>> combinedPathPair) {
        final var schemaPath = combinedPathPair.first();
        final var rootPath = combinedPathPair.second();
        if (schemaPath.size() != rootPath.size()) {
            // change detected!
            return 1;
        }
        if (schemaPath.size() <= 2) {
            // schema and leaf id are irrelevant
            return 0;
        }
        final var schemaPathTrimmed = schemaPath.subList(1, schemaPath.size() - 2);
        final var rootPathTrimmed = rootPath.subList(1, rootPath.size() - 2);
        return schemaPathTrimmed.equals(rootPathTrimmed) ? 0 : 1;
    }

    private static int diffId(Id id) {
        // Eine ID ist nichts anderes als ein Baum.
        // Der Abstand zur Wurzel ist gegeben durch die Anzahl der Knoten des Id-Baums
        // Dies müsste die Anzahl der Operationen darstellen, die zur aktuellen Situation geführt haben.
        return switch (id) {
            case IdSimple ignore -> 0;
            case IdPart idp -> (idp.extensionNumber() == 0 ? 0 : 1) + diffId(idp.predecessorId());
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
