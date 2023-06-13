package scenarioCreator.generation.processing.transformations.constraintBased;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.data.Language;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.column.constraint.ColumnConstraintCheckRegex;
import scenarioCreator.data.column.constraint.regexy.*;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.generation.processing.transformations.structural.StructuralTestingUtils;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;

class RegularCheckModifierTest {

    @Test
    void transform() {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var cccr = new ColumnConstraintCheckRegex(new RegularSum(
                new RegularConcatenation(new RegularTerminal('a'), new RegularTerminal('b')),
                new RegularKleene(new RegularTerminal('c'))
        ));
        final var column = new ColumnLeaf(
                new IdSimple(1), name, dataType, SSet.of(), ColumnContext.getDefault(), SSet.of(cccr)
        );
        final var idGenerator = StructuralTestingUtils.getIdGenerator(2);
        final var transformation = new RegularCheckModifier();

        // --- Act
        final var newColumnList = transformation.transform(column, idGenerator, new Random()).first();

        // --- Assert
        Assertions.assertEquals(1, newColumnList.size());
        final var newColumn = newColumnList.get(0);
        Assertions.assertEquals(column.id(), newColumn.id());
        final var newCccrOpt = newColumn.constraintSet().stream()
                .filter(c -> c instanceof ColumnConstraintCheckRegex)
                .findFirst();
        assert newCccrOpt.isPresent();
        System.out.println(newCccrOpt.get());
        Assertions.assertNotEquals(cccr, newCccrOpt.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void getCandidates(boolean withCccr) {
        // --- Arrange
        final var name = new StringPlusNaked("Spalte", Language.Mixed);
        final var dataType = new DataType(DataTypeEnum.INT32, false);
        final var cccr = new ColumnConstraintCheckRegex(RegularExpression.acceptsEverything());
        final var constraintSet = (SortedSet<ColumnConstraint>) (withCccr ? SSet.of(cccr) : SSet.of());
        final var column = new ColumnLeaf(
                new IdSimple(1), name, dataType, SSet.of(), ColumnContext.getDefault(), constraintSet
        );
        final var transformation = new RegularCheckModifier();

        // --- Act
        final var candidateList = transformation.getCandidates(List.of(column));

        // --- Assert
        if (withCccr) {

            Assertions.assertEquals(1, candidateList.size());
            final var newColumn = candidateList.get(0);
            Assertions.assertEquals(column.id(), newColumn.id());
        } else {
            Assertions.assertTrue(candidateList.isEmpty());
        }
    }
}