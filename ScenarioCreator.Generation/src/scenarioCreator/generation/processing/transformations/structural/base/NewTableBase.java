package scenarioCreator.generation.processing.transformations.structural.base;

import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.context.Encoding;
import scenarioCreator.data.column.context.NumericalDistribution;
import scenarioCreator.data.column.context.UnitOfMeasure;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdPart;
import scenarioCreator.data.identification.MergeOrSplitType;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.SSet;

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
        final var reducedColumnStream = oldTable.columnList().stream().filter(c -> !extractedColumnList.contains(c));
        final var newForeignKeyColumn = createNewForeignKeyColumn(newIds, otherTablesName, oneToOne);
        final var newColumnList = Stream.concat(reducedColumnStream, Stream.of(newForeignKeyColumn)).toList();

        final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                oldTable.functionalDependencySet(), newColumnList
        );
        return oldTable
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFunctionalDependencySet)
                .withId(new IdPart(oldTable.id(), 0, MergeOrSplitType.And));
    }

    private static ColumnLeaf createNewForeignKeyColumn(NewIds newIds, StringPlus tableName, boolean oneToOne) {
        final var constraintSetOneToOne = SSet.of(
                new ColumnConstraintForeignKey(newIds.targetColumn),
                (ColumnConstraint) new ColumnConstraintForeignKeyInverse(newIds.targetColumn));
        final var constraintSetOneToMany = SSet.of(
                (ColumnConstraint) new ColumnConstraintForeignKey(newIds.targetColumn));
        final var newConstraintSet = oneToOne ? constraintSetOneToOne : constraintSetOneToMany;
        return createNewIdColumn(newIds.sourceColumn, tableName, newConstraintSet);
    }

    public static Table createNewTable(Table oldTable, StringPlus tableName, List<Column> columnList,
                                       NewIds newIds, boolean oneToOne) {
        final var primaryKeyColumn = createNewPrimaryKeyColumn(newIds, tableName, oneToOne);
        final var newColumnList = Stream.concat(Stream.of(primaryKeyColumn), columnList.stream()).toList();
        final var newContext = Context.getDefault();
        final var newId = new IdPart(oldTable.id(), 1, MergeOrSplitType.And);
        final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                oldTable.functionalDependencySet(), columnList
        );
        return new Table(newId, tableName, newColumnList, newContext, SSet.of(), newFunctionalDependencySet);
    }

    private static ColumnLeaf createNewPrimaryKeyColumn(NewIds newIds, StringPlus tableName, boolean oneToOne) {
        final var constraintSetOneToOne = SSet.of(
                new ColumnConstraintPrimaryKey(newIds.constraintGroupId()),
                new ColumnConstraintForeignKey(newIds.sourceColumn()),
                new ColumnConstraintForeignKeyInverse(newIds.sourceColumn()));
        final var constraintSetOneToMany = SSet.of(
                new ColumnConstraintPrimaryKey(newIds.constraintGroupId()),
                new ColumnConstraintForeignKeyInverse(newIds.sourceColumn()));
        final var newConstraintSet = oneToOne ? constraintSetOneToOne : constraintSetOneToMany;
        return createNewIdColumn(newIds.targetColumn, tableName, newConstraintSet);
    }

    public static ColumnLeaf createNewIdColumn(Id columnId,
                                               StringPlus tableName,
                                               SortedSet<ColumnConstraint> newConstraintSet) {
        final var nc = tableName.guessNamingConvention(LinguisticUtils::merge);
        final var newNameRawString = LinguisticUtils.merge(nc, tableName.rawString(LinguisticUtils::merge), "id");
        final var newName = new StringPlusNaked(newNameRawString, tableName.language());
        final var newColumnContext = new ColumnContext(
                Context.getDefault(), Encoding.UTF, UnitOfMeasure.None, Language.Technical, NumericalDistribution.getDefault()
        );
        final var newDataType = new DataType(DataTypeEnum.INT64, false);
        return new ColumnLeaf(columnId, newName, newDataType, SSet.of(), newColumnContext, newConstraintSet);
    }

    public record NewIds(Id targetColumn, Id sourceColumn, Id constraintGroupId) {
    }
}