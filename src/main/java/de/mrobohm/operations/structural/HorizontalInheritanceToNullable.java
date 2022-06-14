package de.mrobohm.operations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
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
        var newTable = integrateDerivation(ip);

        var newTableSet = StreamExtensions
                .replaceInStream(schema.tableSet().stream(), Stream.of(ip.base(), ip.derivation()), newTable)
                .collect(Collectors.toSet());
        return schema.withTables(newTableSet);
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

    private Table integrateDerivation(InheritancePair ip) {
        var additionalColumnStream = ip.derivation().columnList().stream()
                .filter(column -> !ip.base().columnList().contains(column))
                .map(column -> switch (column) {
                    case ColumnLeaf leaf -> leaf.withDataType(leaf.dataType().withIsNullable(true));
                    case ColumnNode node -> node.withIsNullable(true);
                    case ColumnCollection col -> col.withIsNullable(true);
                });
        var newColumnList = Stream.concat(ip.base().columnList().stream(), additionalColumnStream).toList();
        return ip.base().withColumnList(newColumnList);
    }


    @Override
    public boolean isExecutable(Schema schema) {
        return schema.tableSet().stream().anyMatch(t1 -> schema.tableSet().stream()
                .anyMatch(t2 -> isDeriving(new InheritancePair(t1, t2))));
    }

    private boolean isDeriving(InheritancePair ip) {
        var relationshipCount = IngestionBase.getRelationshipCount(ip.base(), ip.derivation());
        if(relationshipCount > 0)
        {
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
        if (!basePrimaryKeyColumnList.equals(derivingPrimaryKeyColumnList)) {
            return false;
        } else if (basePrimaryKeyColumnList.size() >= _primaryKeyCountThreshold) {
            return true;
        }

        // Check whether there are enough equal columns
        var jaccardIndex = jaccard(basePartition.no().toList(), derivingPartition.no().toList());
        return jaccardIndex >= _jaccardThreshold;
    }

    private double jaccard(List<Column> columnListA, List<Column> columnListB) {
        var intersection = columnListA.stream()
                .distinct()
                .filter(c1 -> columnListB.stream().anyMatch(c1::equals))
                .count();
        var union = Stream
                .concat(columnListA.stream(), columnListB.stream())
                .distinct()
                .count();
        return (double)intersection / union;
    }

    private record InheritancePair(Table derivation, Table base) {}
}