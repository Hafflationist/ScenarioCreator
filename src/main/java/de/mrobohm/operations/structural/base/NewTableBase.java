package de.mrobohm.operations.structural.base;

import de.mrobohm.data.Context;
import de.mrobohm.data.DataType;
import de.mrobohm.data.DataTypeEnum;
import de.mrobohm.data.Language;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.Encoding;
import de.mrobohm.data.column.UnitOfMeasure;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.operations.linguistic.helpers.LinguisticUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public final class NewTableBase {
    private NewTableBase() {
    }

    public static Table createModifiedTable(Table oldTable, Column oldColumn, NewIds newIds, boolean oneToOne) {
        return createModifiedTable(oldTable, oldColumn.name(), List.of(oldColumn), newIds, oneToOne);
    }

    public static Table createModifiedTable(Table oldTable, StringPlus otherTablesName,
                                            List<Column> oldColumnList, NewIds newIds, boolean oneToOne) {
        var reducedColumnStream = oldTable.columnList().stream().filter(c -> !oldColumnList.contains(c));
        var newForeignKeyColumn = createNewForeignKeyColumn(newIds, otherTablesName, oneToOne);
        var newColumnList = Stream.concat(reducedColumnStream, Stream.of(newForeignKeyColumn)).toList();
        return oldTable.withColumnList(newColumnList);
    }

    private static ColumnLeaf createNewForeignKeyColumn(NewTableBase.NewIds newIds, StringPlus tableName, boolean oneToOne) {
        var constraintSetOneToOne = Set.of(
                new ColumnConstraintForeignKey(newIds.targetColumn, Set.of()),
                (ColumnConstraint) new ColumnConstraintForeignKeyInverse(newIds.targetColumn, Set.of()));
        var constraintSetOneToMany = Set.of(
                (ColumnConstraint) new ColumnConstraintForeignKey(newIds.targetColumn, Set.of()));
        var newConstraintSet = oneToOne ? constraintSetOneToOne : constraintSetOneToMany;
        return createNewIdColumn(newIds.sourceColumn, tableName, newConstraintSet);
    }

    public static Table createNewTable(StringPlus tableName, List<Column> columnList, NewIds newIds, boolean oneToOne) {
        var primaryKeyColumn = createNewPrimaryKeyColumn(newIds, tableName, oneToOne);
        var newColumnList = Stream.concat(Stream.of(primaryKeyColumn), columnList.stream()).toList();
        var newContext = Context.getDefault();
        return new Table(newIds.targetTable, tableName, newColumnList, newContext, Set.of());
    }

    private static ColumnLeaf createNewPrimaryKeyColumn(NewIds newIds, StringPlus tableName, boolean oneToOne) {
        var constraintSetOneToOne = Set.of(
                new ColumnConstraintPrimaryKey(newIds.constraintGroupId()),
                new ColumnConstraintForeignKey(newIds.sourceColumn(), Set.of()),
                new ColumnConstraintForeignKeyInverse(newIds.sourceColumn(), Set.of()));
        var constraintSetOneToMany = Set.of(
                new ColumnConstraintPrimaryKey(newIds.constraintGroupId()),
                new ColumnConstraintForeignKeyInverse(newIds.sourceColumn(), Set.of()));
        var newConstraintSet = oneToOne ? constraintSetOneToOne : constraintSetOneToMany;
        return createNewIdColumn(newIds.targetColumn, tableName, newConstraintSet);
    }

    public static ColumnLeaf createNewIdColumn(Id columnId,
                                                StringPlus tableName,
                                                Set<ColumnConstraint> newConstraintSet) {
        var nc = tableName.guessNamingConvention();
        var newNameRawString = LinguisticUtils.merge(nc, tableName.rawString(), "id");
        var newName = new StringPlusNaked(newNameRawString, tableName.language());
        var newColumnContext = new ColumnContext(Context.getDefault(), Encoding.UTF, UnitOfMeasure.None, Language.Technical);
        var newDataType = new DataType(DataTypeEnum.INT64, false);
        return new ColumnLeaf(columnId, newName, newDataType, newColumnContext, newConstraintSet);
    }

    public record NewIds(Id targetTable, Id targetColumn, Id sourceColumn, Id constraintGroupId) {
    }
}