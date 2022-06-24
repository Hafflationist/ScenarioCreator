package de.mrobohm.operations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.*;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.SchemaTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.structural.base.IngestionBase;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HorizontalInheritanceToNullable implements SchemaTransformation {


    private final int _primaryKeyCountThreshold;

    private final double _jaccardThreshold;

    public HorizontalInheritanceToNullable(int primaryKeyCountThreshold, double jaccardThreshold) {
        _primaryKeyCountThreshold = primaryKeyCountThreshold;
        _jaccardThreshold = jaccardThreshold;
    }

    public HorizontalInheritanceToNullable(double jaccardThreshold) {
        _primaryKeyCountThreshold = 2;  // just trust every confirmed non-surrogate primary key correspondence
        _jaccardThreshold = jaccardThreshold;
    }


    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given schema did not include horizontal inheritance");
        if (!isExecutable(schema)) {
            throw exception;
        }

        var ip = findDerivingTable(schema.tableSet(), random);
        var derivationIntegrationResult = integrateDerivation(ip);
        var oldTableStream = Stream.of(ip.base(), ip.derivation());

        var newTableSet = StreamExtensions
                .replaceInStream(schema.tableSet().stream(), oldTableStream, derivationIntegrationResult.newTable())
                .collect(Collectors.toSet());

        return purgeColumnIds(schema.withTables(newTableSet), derivationIntegrationResult.removedColumnIdSet());
    }

    private InheritancePair findDerivingTable(Set<Table> tableSet, Random random) {
        var ipStream = tableSet.stream()
                .flatMap(t1 -> tableSet.stream().map(t2 -> new Pair<>(t1, t2)))
                .map(pair -> new InheritancePair(pair.first(), pair.second()))
                .filter(this::isDeriving);
        var ipOptional = StreamExtensions.tryPickRandom(ipStream, random);
        assert ipOptional.isPresent() : "This should be handled before!";
        return ipOptional.get();
    }

    private DerivationIntegrationResult integrateDerivation(InheritancePair ip) {
        var columnStreamShouldAdd = StreamExtensions
                .partition(
                        ip.derivation().columnList().stream(),
                        column -> ip.base().columnList().stream().noneMatch(columnB -> equalsColumns(column, columnB))
                );

        var additionalColumnStream = columnStreamShouldAdd.yes()
                .map(column -> switch (column) {
                    case ColumnLeaf leaf -> leaf.withDataType(leaf.dataType().withIsNullable(true));
                    case ColumnNode node -> node.withIsNullable(true);
                    case ColumnCollection col -> col.withIsNullable(true);
                });
        var newColumnList = Stream.concat(ip.base().columnList().stream(), additionalColumnStream).toList();
        var newTable = ip.base().withColumnList(newColumnList);
        var removedColumnIdSet = columnStreamShouldAdd.no().map(Column::id).collect(Collectors.toSet());
        return new DerivationIntegrationResult(newTable, removedColumnIdSet);
    }

    private Schema purgeColumnIds(Schema schema, Set<Id> cidSet) {
        var newTableSet = schema.tableSet().stream().map(t -> {
            var newColumnList = t.columnList().stream().map(column -> {
                var newConstraintSet = column.constraintSet().stream().filter(c -> switch (c) {
                    case ColumnConstraintForeignKey ccfk -> !cidSet.contains(ccfk.foreignColumnId());
                    case ColumnConstraintForeignKeyInverse ccfki -> !cidSet.contains(ccfki.foreignColumnId());
                    default -> true;
                }).collect(Collectors.toSet());
                if (column.constraintSet().equals(newConstraintSet)) {
                    return column;
                }
                return switch (column) {
                    case ColumnLeaf leaf -> leaf.withConstraintSet(newConstraintSet);
                    case ColumnNode node -> node.withConstraintSet(newConstraintSet);
                    case ColumnCollection col -> col.withConstraintSet(newConstraintSet);
                };
            }).toList();
            if (t.columnList().equals(newColumnList)) {
                return t;
            }
            return t.withColumnList(newColumnList);
        }).collect(Collectors.toSet());
        return schema.withTables(newTableSet);
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return schema.tableSet().stream().anyMatch(t1 -> schema.tableSet().stream()
                .anyMatch(t2 -> isDeriving(new InheritancePair(t1, t2))));
    }

    private boolean isDeriving(InheritancePair ip) {
        var relationshipCount = IngestionBase.getRelationshipCount(ip.base(), ip.derivation());
        if (relationshipCount > 0 || ip.base().equals(ip.derivation())) {
            return false;
        }

        var basePartition = StreamExtensions
                .partition(ip.base().columnList().stream(), column -> column.constraintSet().stream()
                        .anyMatch(c -> c instanceof ColumnConstraintPrimaryKey));

        var derivingPartition = StreamExtensions
                .partition(ip.derivation().columnList().stream(), column -> column.constraintSet().stream()
                        .anyMatch(c -> c instanceof ColumnConstraintPrimaryKey));

        // Check whether both primary key column lists are equal
        var basePrimaryKeyColumnList = basePartition.yes().toList();
        var derivingPrimaryKeyColumnList = derivingPartition.yes().toList();
        if (!equalsColumnList(basePrimaryKeyColumnList, derivingPrimaryKeyColumnList)) {
            return false;
        } else if (basePrimaryKeyColumnList.size() >= _primaryKeyCountThreshold) {
            return true;
        }

        // Check whether there are enough equal columns
        var jaccardIndex = jaccard(basePartition.no().toList(), derivingPartition.no().toList());
        return jaccardIndex >= _jaccardThreshold && isSubset(ip.derivation().columnList(), ip.base().columnList());
    }

    private double jaccard(List<Column> columnListA, List<Column> columnListB) {
        var intersection = columnListA.stream()
                .distinct()
                .filter(c1 -> columnListB.stream().anyMatch(c2 -> equalsColumns(c1, c2)))
                .count();
        var union = Stream
                .concat(
                        columnListA.stream(),
                        columnListB.stream().filter(c2 -> columnListA.stream().noneMatch(c1 -> equalsColumns(c1, c2))))
                .count();
        return (double) intersection / union;
    }

    private boolean isSubset(List<Column> superset, List<Column> subset) {
        return subset.stream().allMatch(sub -> superset.stream().anyMatch(sup -> equalsColumns(sub, sup)));
    }

    private boolean equalsColumnList(List<Column> columnListA, List<Column> columnListB) {
        return StreamExtensions
                .zip(columnListA.stream(), columnListB.stream(), this::equalsColumns)
                .allMatch(x -> x);
    }

    private boolean equalsColumns(Column c, Column d) {
        if (c instanceof ColumnLeaf cLeaf && d instanceof ColumnLeaf dLeaf) {
            return c.name().equals(d.name())
                    && cLeaf.dataType().equals(dLeaf.dataType())
                    && equalsConstraintSets(c.constraintSet(), d.constraintSet());
        }
        if (c instanceof ColumnNode cNode && d instanceof ColumnNode dNode) {
            return c.name().equals(d.name())
                    && c.isNullable() == d.isNullable()
                    && equalsConstraintSets(c.constraintSet(), d.constraintSet())
                    && equalsColumnList(cNode.columnList(), dNode.columnList());
        }
        if (c instanceof ColumnCollection cCol && d instanceof ColumnCollection dCol) {
            return c.name().equals(d.name())
                    && c.isNullable() == d.isNullable()
                    && equalsConstraintSets(c.constraintSet(), d.constraintSet())
                    && equalsColumnList(cCol.columnList(), dCol.columnList());
        }
        return false;
    }

    private boolean isSubsetConstraintSets(Set<ColumnConstraint> constraintSetSuper, Set<ColumnConstraint> constraintSetSub) {
        return constraintSetSub.stream().allMatch(ca -> constraintSetSuper.stream().anyMatch(cb -> switch (ca) {
            case ColumnConstraintPrimaryKey ignore -> cb instanceof ColumnConstraintPrimaryKey;
            case ColumnConstraintUnique ignore -> cb instanceof ColumnConstraintUnique;
            case ColumnConstraintLocalPredicate cclpa ->
                    cb instanceof ColumnConstraintLocalPredicate cclpb && cclpa.equals(cclpb);
            case ColumnConstraintForeignKey ccfka -> cb instanceof ColumnConstraintForeignKey ccfkb
                    && ccfka.foreignColumnId().equals(ccfkb.foreignColumnId());
            case ColumnConstraintForeignKeyInverse ccfkia -> cb instanceof ColumnConstraintForeignKeyInverse ccfkib
                    && ccfkia.foreignColumnId().equals(ccfkib.foreignColumnId());

        }));
    }

    private boolean equalsConstraintSets(Set<ColumnConstraint> constraintSetA, Set<ColumnConstraint> constraintSetB) {
        return isSubsetConstraintSets(constraintSetA, constraintSetB)
                && isSubsetConstraintSets(constraintSetB, constraintSetA);
    }

    private record DerivationIntegrationResult(Table newTable, Set<Id> removedColumnIdSet) {
    }

    private record InheritancePair(Table derivation, Table base) {
    }
}