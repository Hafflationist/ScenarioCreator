package de.mrobohm.operations.structural;

import de.mrobohm.data.*;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.integrity.IntegrityChecker;
import de.mrobohm.utils.StreamExtensions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

class NullableToHorizontalInheritanceTest {

    @Test
    void transform() {
    }

    @Test
    void transformWithPrimaryKey() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var primaryKeyColumn1 = new ColumnLeaf(-1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(5)));
        var primaryKeyColumn2 = new ColumnLeaf(0, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintPrimaryKey(5)));
        var invalidColumn0 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(3, Set.of())));
        var invalidColumn1 = new ColumnLeaf(2, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(3, Set.of())));
        var invalidColumn2 = new ColumnLeaf(3, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(1, Set.of()),
                        new ColumnConstraintForeignKeyInverse(2, Set.of())));
        var validColumn = new ColumnLeaf(4, name, dataType.withIsNullable(true), ColumnContext.getDefault(), Set.of());

        var invalidTable1 = new Table(10, name, List.of(invalidColumn1), Context.getDefault(), Set.of());
        var invalidTable2 = new Table(11, name, List.of(invalidColumn0, invalidColumn2), Context.getDefault(), Set.of());
        var targetTable = new Table(14, name,
                List.of(primaryKeyColumn1, primaryKeyColumn2, validColumn), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable1, invalidTable2, targetTable);
        IntegrityChecker.assertValidSchema(new Schema(-100, name, Context.getDefault(), tableSet));
        var idGenerator = StructuralTestingUtils.getIdGenerator(100);
        var transformation = new NullableToHorizontalInheritance();

        // --- Act
        var newTableSet = transformation.transform(targetTable, tableSet, idGenerator, new Random());

        // --- Assert
        Assertions.assertEquals(2, newTableSet.size());
        var newTableList = newTableSet.stream().toList();
        var newTable1 = newTableList.get(0);
        var newTable2 = newTableList.get(1);
        Assertions.assertEquals(targetTable.columnList().size(), Math.max(newTable1.columnList().size(), newTable2.columnList().size()));
        Assertions.assertEquals(targetTable.columnList().size() - 1, Math.min(newTable1.columnList().size(), newTable2.columnList().size()));
        Assertions.assertNotEquals(targetTable, newTable1);
        Assertions.assertNotEquals(targetTable, newTable2);
        var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toSet());
        IntegrityChecker.assertValidSchema(new Schema(-100, name, Context.getDefault(), fullNewTableSet));
    }

    @Test
    void getCandidates() {
        // --- Arrange
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var invalidColumn1 = new ColumnLeaf(1, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKeyInverse(6, Set.of())));
        var invalidColumn2 = new ColumnLeaf(2, name, dataType, ColumnContext.getDefault(),
                Set.of(new ColumnConstraintForeignKey(7, Set.of())));
        var validColumn1 = new ColumnLeaf(
                4, name, dataType.withIsNullable(true), ColumnContext.getDefault(), Set.of()
        );
        var validColumn2 = new ColumnLeaf(
                5, name, dataType.withIsNullable(true), ColumnContext.getDefault(), Set.of()
        );

        var invalidTable1 = new Table(10, name, List.of(invalidColumn1), Context.getDefault(), Set.of());
        var invalidTable2 = new Table(11, name, List.of(invalidColumn1, invalidColumn2), Context.getDefault(), Set.of());
        var validTable = new Table(14, name, List.of(validColumn1, validColumn2), Context.getDefault(), Set.of());
        var tableSet = Set.of(invalidTable1, invalidTable2, validTable);
        var transformation = new NullableToHorizontalInheritance();

        // --- Act
        var candidates = transformation.getCandidates(tableSet);

        // --- Assert
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertTrue(candidates.contains(validTable));
        Assertions.assertFalse(candidates.contains(invalidTable1));
        Assertions.assertFalse(candidates.contains(invalidTable2));
    }
}