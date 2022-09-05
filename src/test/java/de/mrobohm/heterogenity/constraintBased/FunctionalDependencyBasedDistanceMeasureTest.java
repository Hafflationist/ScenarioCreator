package de.mrobohm.heterogenity.constraintBased;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.context.ColumnContext;
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

class FunctionalDependencyBasedDistanceMeasureTest {

    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    })
    void calculateDistanceToRootAbsolute(int seed) {
        // --- Arrange
        final var random = new Random(seed);
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var column1 = new ColumnLeaf(new IdSimple(1), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column2 = new ColumnLeaf(new IdSimple(2), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column3 = new ColumnLeaf(new IdSimple(3), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column4 = new ColumnLeaf(new IdSimple(4), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column5 = new ColumnLeaf(new IdSimple(5), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column6 = new ColumnLeaf(new IdSimple(6), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column7 = new ColumnLeaf(new IdSimple(7), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column8 = new ColumnLeaf(new IdSimple(8), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var column9 = new ColumnLeaf(new IdSimple(9), name, dataType, ColumnContext.getDefault(), SSet.of());

        final var table = StructuralTestingUtils.createTable(
                101,
                List.of(column1, column2, column3, column4, column5, column6, column7, column8, column9),
                random
        );
        final var tableSet = SSet.of(table);
        final var schema = new Schema(new IdSimple(1001), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var idGenerator = StructuralTestingUtils.getIdGenerator(600);
        final var transformation = new FunctionalDependencyRemover();
        final var newTableSet = transformation.transform(table, idGenerator, random);
        final var newSchema = new Schema(new IdSimple(1001), name, Context.getDefault(), newTableSet);

        // --- Act
        final var constraintBasedDistance = FunctionalDependencyBasedDistanceMeasure.calculateDistanceAbsolute(schema, newSchema);

        // --- Assert
        System.out.println(constraintBasedDistance);
    }


    @ParameterizedTest
    @ValueSource(ints = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    })
    void calculateDistanceToRootAbsolute2(int seed) {
        // --- Arrange
        final var random = new Random(seed);
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var primaryKeyColumn1 = new ColumnLeaf(new IdSimple(31), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        final var primaryKeyColumn2 = new ColumnLeaf(new IdSimple(32), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(5))));
        final var invalidColumn0 = new ColumnLeaf(new IdSimple(21), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22), SSet.of())));
        final var invalidColumn1 = new ColumnLeaf(new IdSimple(11), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKey(new IdSimple(22), SSet.of())));
        final var invalidColumn2 = new ColumnLeaf(new IdSimple(22), name, dataType, ColumnContext.getDefault(),
                SSet.of(new ColumnConstraintForeignKeyInverse(invalidColumn0.id(), SSet.of()),
                        new ColumnConstraintForeignKeyInverse(invalidColumn1.id(), SSet.of())));
        final var validColumn = new ColumnLeaf(new IdSimple(33), name, dataType.withIsNullable(true),
                ColumnContext.getDefault(), SSet.of());
        final var neutralColumn1 = new ColumnLeaf(new IdSimple(34), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn2 = new ColumnLeaf(new IdSimple(35), name, dataType, ColumnContext.getDefault(), SSet.of());
        final var neutralColumn3 = new ColumnLeaf(new IdSimple(36), name, dataType, ColumnContext.getDefault(), SSet.of());

        final var invalidTable1 = StructuralTestingUtils.createTable(
                101, List.of(invalidColumn1), random
        );
        final var invalidTable2 = StructuralTestingUtils.createTable(
                102, List.of(invalidColumn0, invalidColumn2), random
        );
        final var targetTable = StructuralTestingUtils.createTable(
                103,
                List.of(primaryKeyColumn1, primaryKeyColumn2, validColumn, neutralColumn1, neutralColumn2, neutralColumn3),
                random
        );
        final var tableSet = SSet.of(invalidTable1, invalidTable2, targetTable);
        final var schema = new Schema(new IdSimple(-100), name, Context.getDefault(), tableSet);
        IntegrityChecker.assertValidSchema(schema);
        final var idGenerator = StructuralTestingUtils.getIdGenerator(1200);
        final var transformation = new NullableToHorizontalInheritance();
        final var newTableSet = transformation.transform(targetTable, idGenerator, new Random());
        final var fullNewTableSet = StreamExtensions
                .replaceInStream(tableSet.stream(), targetTable, newTableSet.stream())
                .collect(Collectors.toCollection(TreeSet::new));
        final var newSchema = new Schema(new IdSimple(-100), name, Context.getDefault(), fullNewTableSet);
        IntegrityChecker.assertValidSchema(newSchema);

        // --- Act
        final var constraintBasedDistance = FunctionalDependencyBasedDistanceMeasure.calculateDistanceAbsolute(schema, newSchema);

        // --- Assert
        System.out.println(constraintBasedDistance);
    }
}