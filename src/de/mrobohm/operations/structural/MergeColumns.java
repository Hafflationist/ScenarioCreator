package de.mrobohm.operations.structural;

import de.mrobohm.data.DataType;
import de.mrobohm.data.column.constraint.*;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record MergeColumns(boolean keepForeignKeyIntegrity) implements TableTransformation {

    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet) {
        var pair = getMergeableColumns(table, otherTableSet);
        var newColumn = generateNewColumn(pair.first(), pair.second(), table, otherTableSet);
        var filteredOldColumnStream = table.columnList().stream()
                .filter(c -> !c.equals(pair.first()))
                .filter(c -> !c.equals(pair.second()));
        var newColumnList = Stream.concat(filteredOldColumnStream, Stream.of(newColumn)).toList();
        return Collections.singleton(table.withColumnList(newColumnList));
    }

    private Pair<ColumnLeaf, ColumnLeaf> getMergeableColumns(Table table, Set<Table> otherTableSet) {
        var referencedColumnIdSet = getReferencedColumnIdSet(otherTableSet);
        var validColumnStream = getValidColumns(table.columnList(), referencedColumnIdSet).stream();
        var exception = new TransformationCouldNotBeExecutedException("2 columns could not be found! This exception is an indicator of bad checking. This should be stopped by <getCandidates>!");
        var twoColumns = StreamExtensions.pickRandomOrThrowMultiple(validColumnStream, 2, exception);
        var twoColumnsList = twoColumns.toList();
        var firstColumn = twoColumnsList.get(0);
        var secondColumn = twoColumnsList.get(1);
        return new Pair<>(firstColumn, secondColumn);
    }

    private Column generateNewColumn(ColumnLeaf columnA, ColumnLeaf columnB, Table table, Set<Table> otherTableSet) {
        // TODO this method can be improved dramatically!
        var newId = StreamExtensions.getColumnId(otherTableSet);
        var newName = columnA.name() + "_and_" + columnB;
        var newColumn = new ColumnLeaf(newId, newName, DataType.NVARCHAR, null, new HashSet<>());
        return newColumn;
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        var referencedColumnIdSet = getReferencedColumnIdSet(tableSet);
        return tableSet
                .stream()
                .filter(t -> checkTable(t, referencedColumnIdSet))
                .collect(Collectors.toSet());
    }

    private boolean checkTable(Table table, Set<Integer> referencedColumnIdSet) {
        if (table.columnList().size() < 3) return false;

        var validColumnsCount = getValidColumns(table.columnList(), referencedColumnIdSet).size();
        return validColumnsCount >= 2;
    }

    private Set<ColumnLeaf> getValidColumns(List<Column> columnList, Set<Integer> referencedColumnIdSet) {
        return columnList.stream()
                .filter(c -> !referencedColumnIdSet.contains(c.id()) || !keepForeignKeyIntegrity)
                .filter(c -> c instanceof ColumnLeaf)
                .map(c -> (ColumnLeaf) c)
                .filter(c -> checkConstraintSet(c.constraintSet()))
                .collect(Collectors.toSet());
    }

    private Set<Integer> getReferencedColumnIdSet(Set<Table> tableSet) {
        return tableSet.stream()
                .flatMap(t -> t.columnList().stream())
                .flatMap(c -> c.constraintSet().stream())
                .filter(c -> c instanceof ColumnConstraintForeignKey)
                .map(c -> ((ColumnConstraintForeignKey) c).foreignColumnId())
                .collect(Collectors.toSet());
    }

    private boolean checkConstraintSet(Set<ColumnConstraint> constraints) {
        var hasBadConstraints = constraints.stream()
                .anyMatch(this::isObstacle);
        return !hasBadConstraints;
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    private boolean isObstacle(ColumnConstraint constraint) {
        return switch (constraint) {
            case ColumnConstraintForeignKey ignored2 -> keepForeignKeyIntegrity;
            case ColumnConstraintLocalPredicate ignored1 -> false;
            case ColumnConstraintPrimaryKey ignored -> true;
            case ColumnConstraintUnique ignored -> false;
        };
    }
}
