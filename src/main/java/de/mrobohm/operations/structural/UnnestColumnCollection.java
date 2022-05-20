package de.mrobohm.operations.structural;

import de.mrobohm.data.Context;
import de.mrobohm.data.DataType;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.Encoding;
import de.mrobohm.data.column.UnitOfMeasure;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.TableTransformation;
import de.mrobohm.operations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.operations.linguistic.LinguisticUtils;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnnestColumnCollection implements TableTransformation {
    @Override
    @NotNull
    public Set<Table> transform(Table table, Set<Table> otherTableSet, Random random) {
        var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a collection as column!");

        var columnCollectionStream = table.columnList().stream()
                .filter(c -> c instanceof ColumnCollection);
        var column = StreamExtensions.pickRandomOrThrow(columnCollectionStream, exception, random);
        assert column instanceof ColumnCollection : "BUG!";

        var newIdArray = IdentificationNumberGenerator.generate(otherTableSet, 3);
        var newIds = new NewIds(newIdArray[0], newIdArray[1], newIdArray[2]);
        var newTable = createNewTable((ColumnCollection) column, newIds);
        var modifiedTable = createModifiedTable(table, (ColumnCollection) column, newIds);
        return Set.of(newTable, modifiedTable);
    }

    private Table createModifiedTable(Table oldTable, ColumnCollection oldColumnCollection, NewIds newIds) {
        var reducedColumnList = oldTable.columnList().stream().filter(c -> c != oldColumnCollection);
        var newForeignKeyColumn = createNewForeignKeyColumn(newIds, oldColumnCollection);
        var newColumnList = Stream.concat(reducedColumnList, Stream.of(newForeignKeyColumn)).toList();
        return oldTable.withColumnList(newColumnList);
    }

    private ColumnLeaf createNewForeignKeyColumn(NewIds newIds, ColumnCollection columnCollection) {
        var newConstraintSet = Set.of(
                (ColumnConstraint) new ColumnConstraintForeignKey(newIds.targetColumn, Set.of()));
        return createNewIdColumn(newIds.sourceColumn, columnCollection, newConstraintSet);
    }

    private Table createNewTable(ColumnCollection columnCollection, NewIds newIds) {
        var primaryKeyColumn = createNewPrimaryKeyColumn(newIds, columnCollection);
        var newColumnList = Stream.concat(Stream.of(primaryKeyColumn), columnCollection.columnList().stream()).toList();
        var newContext = Context.getDefault();
        return new Table(newIds.targetTable, columnCollection.name(), newColumnList, newContext, Set.of());
    }

    private ColumnLeaf createNewPrimaryKeyColumn(NewIds newIds, ColumnCollection columnCollection) {
        var newConstraintSet = Set.of(
                new ColumnConstraintPrimaryKey(),
                new ColumnConstraintForeignKeyInverse(newIds.sourceColumn(), Set.of()));
        return createNewIdColumn(newIds.targetColumn, columnCollection, newConstraintSet);
    }


    private ColumnLeaf createNewIdColumn(int columnId, ColumnCollection columnCollection, Set<ColumnConstraint> newConstraintSet) {
        var nc = columnCollection.name().guessNamingConvention();
        var newNameRawString = LinguisticUtils.merge(nc, columnCollection.name().rawString(), "id");
        var newName = new StringPlus(newNameRawString, columnCollection.name().language());
        var newColumnContext = new ColumnContext(Context.getDefault(), Encoding.UTF, UnitOfMeasure.None, Language.Technical);
        return new ColumnLeaf(columnId, newName, DataType.INT64, newColumnContext, newConstraintSet);
    }

    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream().filter(this::hasColumnCollection).collect(Collectors.toSet());
    }

    private boolean hasColumnCollection(Table table) {
        return table.columnList().stream().anyMatch(c -> c instanceof ColumnCollection);
    }

    private record NewIds(int targetTable, int targetColumn, int sourceColumn) {
    }
}