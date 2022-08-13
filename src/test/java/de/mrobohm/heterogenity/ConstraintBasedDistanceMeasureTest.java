package de.mrobohm.heterogenity;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.ColumnContext;
import de.mrobohm.data.column.DataType;
import de.mrobohm.data.column.DataTypeEnum;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintPrimaryKey;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.processing.integrity.IntegrityChecker;
import de.mrobohm.processing.transformations.constraintBased.FunctionalDependencyRemover;
import de.mrobohm.processing.transformations.structural.NullableToHorizontalInheritance;
import de.mrobohm.processing.transformations.structural.StructuralTestingUtils;
import de.mrobohm.utils.SSet;
import de.mrobohm.utils.StreamExtensions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

class ConstraintBasedDistanceMeasureTest {

    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    })
    void calculateDistanceToRootAbsolute(int seed) {
        // --- Arrange
        var random = new Random(seed);
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column4 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column5 = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column6 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column7 = new ColumnLeaf(new IdSimple(7), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column8 = new ColumnLeaf(new IdSimple(8), name, dataType, ColumnContext.getDefault(), SSet.of());
        var column9 = new ColumnLeaf(new IdSimple(9), name, dataType, ColumnContext.getDefault(), SSet.of());

        var table = StructuralTestingUtils.createTable(
                101,
                List.of(column1, column2, column3, column4, column5, column6, column7, column8, column9),
                random
        );
        var tableSet = SSet.of(table);
        var schema = new Schema(new IdSimple(1001), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var idGenerator = StructuralTestingUtils.getIdGenerator(600);
        var transformation = new FunctionalDependencyRemover();
        var newTableSet = transformation.transform(table, idGenerator, random);
        var newSchema = new Schema(new IdSimple(1001), name, Context.getDefault(), newTableSet);

        // --- Act
        var constraintBasedDistance = ConstraintBasedDistanceMeasure.calculateDistanceAbsolute(schema, newSchema);

        // --- Assert
        System.out.println(constraintBasedDistance);
    }


    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    })
    void calculateDistanceToRootAbsolute2(int seed) {
        // --- Arrange
        var random = new Random(seed);
        var name = new StringPlusNaked("Spalte", Language.Mixed);
        var dataType = new DataType(DataTypeEnum.INT32, false);
        var primaryKeyColumn1 = new ColumnLeaf(new IdSimple(31), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        var primaryKeyColumn2 = new ColumnLeaf(new IdSimple(32), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        var invalidColumn0 = new ColumnLeaf(new IdSimple(21), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22), SSet.of())));
        var invalidColumn1 = new ColumnLeaf(new IdSimple(11), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22), SSet.of())));
        var invalidColumn2 = new ColumnLeaf(new IdSimple(22), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(invalidColumn0.id(), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(invalidColumn1.id(), SSet.of())));
        var validColumn = new ColumnLeaf(new IdSimple(33), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());
        var neutralColumn1 = new ColumnLeaf(new IdSimple(34), name, dataType, ColumnContext.getDefault(), SSet.of());
        var neutralColumn2 = new ColumnLeaf(new IdSimple(35), name, dataType, ColumnContext.getDefault(), SSet.of());
        var neutralColumn3 = new ColumnLeaf(new IdSimple(36), name, dataType, ColumnContext.getDefault(), SSet.of());

        var invalidTable1 = StructuralTestingUtils.createTable(
                101, List.of(invalidColumn1), random
        );
        var invalidTable2 = StructuralTestingUtils.createTable(
                102, List.of(invalidColumn0, invalidColumn2), random
        );
        var targetTable = StructuralTestingUtils.createTable(
                103,
                List.of(primaryKeyColumn1, primaryKeyColumn2, validColumn, neutralColumn1, neutralColumn2, neutralColumn3),
                random
        );
        var tableSet = SSet.of(invalidTable1, invalidTable2, targetTable);
        var schema = new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        var idGenerator = StructuralTestingUtils.getIdGenerator(1200);
        var transformation = new NullableToHorizontalInheritance();
        var newTableSet = transformation.transform(targetTable, idGenerator, new Random());
        var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toCollection(TreeSet::new));
        var newSchema = new Schema(new IdSimple(-100), name, Context.getDefault(), fullNewTableSet);
        IntegrityChecker.assertValidSchema(newSchema);

        // --- Act
        var constraintBasedDistance = ConstraintBasedDistanceMeasure.calculateDistanceAbsolute(schema, newSchema);

        // --- Assert
        System.out.println(constraintBasedDistance);
    }
}