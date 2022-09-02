package de.mrobohm.processing.transformations.structural.base;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.*;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.context.Encoding;
import de.mrobohm.data.column.context.UnitOfMeasure;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.MergeOrSplitType;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.SSet;

import java.util.List;
import java.util.SortedSet;
import java.util.stream.Stream;

public final class NewTableBase {
    private NewTableBase() {
    }

    public static Table createModifiedTable(Table oldTable, Column oldColumn, NewIds newIds, boolean oneToOne) {
        return createModifiedTable(oldTable, oldColumn.name(), List.of(oldColumn), newIds, oneToOne);
    }

    public static Table createModifiedTable(Table oldTable, StringPlus otherTablesName,
                                            List<Column> extractedColumnList, NewIds newIds, boolean oneToOne) {
        var reducedColumnStream = oldTable.columnList().stream().filter(c -> !extractedColumnList.contains(c));
        var newForeignKeyColumn = createNewForeignKeyColumn(newIds, otherTablesName, oneToOne);
        var newColumnList = Stream.concat(reducedColumnStream, Stream.of(newForeignKeyColumn)).toList();

        var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                oldTable.functionalDependencySet(), newColumnList
        );
        return oldTable
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFunctionalDependencySet)
                .withId(new IdPart(oldTable.id(), 0, MergeOrSplitType.And));
    }

    private static ColumnLeaf createNewForeignKeyColumn(NewTableBase.NewIds newIds, StringPlus tableName, boolean oneToOne) {
        var constraintSetOneToOne = SSet.of(
                new ColumnConstraintForeignKey(newIds.targetColumn, SSet.of()),
                (ColumnConstraint) new ColumnConstraintForeignKeyInverse(newIds.targetColumn, SSet.of()));
        var constraintSetOneToMany = SSet.of(
                (ColumnConstraint) new ColumnConstraintForeignKey(newIds.targetColumn, SSet.of()));
        var newConstraintSet = oneToOne ? constraintSetOneToOne : constraintSetOneToMany;
        return createNewIdColumn(newIds.sourceColumn, tableName, newConstraintSet);
    }

    public static Table createNewTable(Table oldTable, StringPlus tableName, List<Column> columnList,
                                       NewIds newIds, boolean oneToOne) {
        var primaryKeyColumn = createNewPrimaryKeyColumn(newIds, tableName, oneToOne);
        var newColumnList = Stream.concat(Stream.of(primaryKeyColumn), columnList.stream()).toList();
        var newContext = Context.getDefault();
        var newId = new IdPart(oldTable.id(), 1, MergeOrSplitType.And);
        var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                oldTable.functionalDependencySet(), columnList
        );
        return new Table(newId, tableName, newColumnList, newContext, SSet.of(), newFunctionalDependencySet);
    }

    private static ColumnLeaf createNewPrimaryKeyColumn(NewIds newIds, StringPlus tableName, boolean oneToOne) {
        var constraintSetOneToOne = SSet.of(
                new ColumnConstraintPrimaryKey(newIds.constraintGroupId()),
                new ColumnConstraintForeignKey(newIds.sourceColumn(), SSet.of()),
                new ColumnConstraintForeignKeyInverse(newIds.sourceColumn(), SSet.of()));
        var constraintSetOneToMany = SSet.of(
                new ColumnConstraintPrimaryKey(newIds.constraintGroupId()),
                new ColumnConstraintForeignKeyInverse(newIds.sourceColumn(), SSet.of()));
        var newConstraintSet = oneToOne ? constraintSetOneToOne : constraintSetOneToMany;
        return createNewIdColumn(newIds.targetColumn, tableName, newConstraintSet);
    }

    public static ColumnLeaf createNewIdColumn(Id columnId,
                                               StringPlus tableName,
                                               SortedSet<ColumnConstraint> newConstraintSet) {
        var nc = tableName.guessNamingConvention();
        var newNameRawString = LinguisticUtils.merge(nc, tableName.rawString(), "id");
        var newName = new StringPlusNaked(newNameRawString, tableName.language());
        var newColumnContext = new ColumnContext(Context.getDefault(), Encoding.UTF, UnitOfMeasure.None, Language.Technical);
        var newDataType = new DataType(DataTypeEnum.INT64, false);
        return new ColumnLeaf(columnId, newName, newDataType, SSet.of(), newColumnContext, newConstraintSet);
    }

    public record NewIds(Id targetColumn, Id sourceColumn, Id constraintGroupId) {
    }
}