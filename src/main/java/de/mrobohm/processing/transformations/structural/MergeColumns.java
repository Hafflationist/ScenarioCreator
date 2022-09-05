package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.*;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.dataset.Value;
import de.mrobohm.data.identification.IdMerge;
import de.mrobohm.data.identification.MergeOrSplitType;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.transformations.SchemaTransformation;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class MergeColumns implements SchemaTransformation {
    private final boolean keepForeignKeyIntegrity;

    public MergeColumns(boolean keepForeignKeyIntegrity) {
        this.keepForeignKeyIntegrity = keepForeignKeyIntegrity;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    public boolean conservesFlatRelations() {
        return true;
    }

    @Override
    @NotNull
    public Schema transform(Schema schema, Random random) {
        final var exception = new TransformationCouldNotBeExecutedException("2 columns could not be found! This exception is an indicator of bad checking. This should be stopped by <isExecutable>!");
        final var validTableStream = schema.tableSet().stream().filter(this::checkTable);
        final var table = StreamExtensions.pickRandomOrThrow(validTableStream, exception, random);
        final var pair = getMergeableColumns(table, random);
        final var newColumn = generateNewColumn(pair.first(), pair.second(), random);
        final var filteredOldColumnStream = getNewColumnStream(table, pair);

        final var newColumnList = StreamExtensions.prepend(filteredOldColumnStream, newColumn).toList();
        final var newFunctionalDependencySet = FunctionalDependencyManager.getValidFdSet(
                table.functionalDependencySet(), newColumnList
        );
        final var newTableSet = StreamExtensions
                .replaceInStream(
                        schema.tableSet().stream(),
                        table,
                        table
                                .withColumnList(newColumnList)
                                .withFunctionalDependencySet(newFunctionalDependencySet)
                )
                .map(t -> {
                    final var ncl = getNewColumnStream(t, pair).toList();
                    if (ncl.equals(t.columnList())) {
                        return t;
                    }
                    return t.withColumnList(ncl);
                })
                .collect(Collectors.toCollection(TreeSet::new));
        return schema.withTableSet(newTableSet);
    }

    private Stream<Column> getNewColumnStream(Table table, Pair<ColumnLeaf, ColumnLeaf> mergedColumns) {
        return table.columnList().stream()
                .filter(column -> !column.equals(mergedColumns.first()))
                .filter(column -> !column.equals(mergedColumns.second()))
                .map(column -> {
                    final var newConstraintSet = column.constraintSet().stream()
                            .filter(c -> switch (c) {
                                case ColumnConstraintForeignKey ccfk ->
                                        !ccfk.foreignColumnId().equals(mergedColumns.first().id())
                                                && !ccfk.foreignColumnId().equals(mergedColumns.second().id());
                                case ColumnConstraintForeignKeyInverse ccfki ->
                                        !ccfki.foreignColumnId().equals(mergedColumns.first().id())
                                                && !ccfki.foreignColumnId().equals(mergedColumns.second().id());
                                default -> true;
                            }).collect(Collectors.toCollection(TreeSet::new));
                    if (newConstraintSet.equals(column.constraintSet())) {
                        return column;
                    }
                    return switch (column) {
                        case ColumnLeaf leaf -> leaf.withConstraintSet(newConstraintSet);
                        case ColumnNode node -> node.withConstraintSet(newConstraintSet);
                        case ColumnCollection col -> col.withConstraintSet(newConstraintSet);
                    };
                });
    }


    private Pair<ColumnLeaf, ColumnLeaf> getMergeableColumns(Table table, Random random) {
        final var validColumnStream = getValidColumns(table.columnList());
        final var exception = new TransformationCouldNotBeExecutedException("2 columns could not be found! This exception is an indicator of bad checking. This should be stopped by <isExecutable>!");
        final var twoColumns = StreamExtensions.pickRandomOrThrowMultiple(
                validColumnStream.stream(), 2, exception, random
        );
        final var twoColumnsList = twoColumns.toList();
        final var firstColumn = twoColumnsList.get(0);
        final var secondColumn = twoColumnsList.get(1);
        return new Pair<>(firstColumn, secondColumn);
    }

    private Column generateNewColumn(ColumnLeaf columnA, ColumnLeaf columnB, Random random) {
        // TODO this method can be improved dramatically!
        // Sobald die Kontexteigenschaft eine Bedeutung bekommt, müsste diese auch verschmolzen werden.
        final var newId = new IdMerge(columnA.id(), columnB.id(), MergeOrSplitType.And);
        final var newName = LinguisticUtils.merge(columnA.name(), columnB.name(), random);
        final var newDataType = new DataType(DataTypeEnum.NVARCHAR, random.nextBoolean());
        final var newValueSet = columnA.valueSet().stream()
                .flatMap(valueA -> columnB.valueSet().stream()
                        .map(valueB -> valueA + "|" + valueB)
                        .map(Value::new))
                .collect(Collectors.toCollection(TreeSet::new));
        // numerical distributions can be ignored, because their information cannot be bundled
        final var newContext = ColumnContext.getDefault();
        return new ColumnLeaf(newId, newName, newDataType, newValueSet, newContext, SSet.of());
    }

    @Override
    public boolean isExecutable(Schema schema) {
        final var tableSet = schema.tableSet();
        return tableSet.stream()
                .anyMatch(this::checkTable);
    }

    private boolean checkTable(Table table) {
        if (table.columnList().size() < 2) return false;

        final var validColumnsCount = getValidColumns(table.columnList()).size();
        return validColumnsCount >= 2;
    }

    private SortedSet<ColumnLeaf> getValidColumns(List<Column> columnList) {
        return columnList.stream()
                .filter(c -> c instanceof ColumnLeaf)
                .map(c -> (ColumnLeaf) c)
                .filter(c -> checkConstraintSet(c.constraintSet()))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean checkConstraintSet(SortedSet<ColumnConstraint> constraints) {
        return constraints.stream().noneMatch(this::isObstacle);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    private boolean isObstacle(ColumnConstraint constraint) {
        return switch (constraint) {
            case ColumnConstraintForeignKey ignored -> keepForeignKeyIntegrity;
            case ColumnConstraintForeignKeyInverse ignored -> keepForeignKeyIntegrity;
            case ColumnConstraintPrimaryKey ignored -> true;
            case ColumnConstraintUnique ignored -> false;
            case ColumnConstraintCheckNumerical ignored -> false;
        };
    }
}