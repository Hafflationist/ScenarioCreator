package de.mrobohm.heterogenity.constraintBased.numerical;

import de.mrobohm.data.column.constraint.numerical.*;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CheckExpressionEvaluationTest {

    @Test
    void evaluateEmptyConjunction() {
        // --- Arrange
        var checkExpression = new CheckConjunction(SSet.of());

        // --- Act
        var isValid = CheckExpressionEvaluation.evaluate(checkExpression, 5.0);

        // --- Assert
        Assertions.assertTrue(isValid);
    }

    @Test
    void evaluateEmptyDisjunction() {
        // --- Arrange
        var checkExpression = new CheckDisjunction(SSet.of());

        // --- Act
        var isValid = CheckExpressionEvaluation.evaluate(checkExpression, 5.0);

        // --- Assert
        Assertions.assertFalse(isValid);
    }

    @Test
    void evaluateWithFullComplexity() {
        // --- Arrange
        var checkExpression = new CheckDisjunction(SSet.of(
                new CheckConjunction(SSet.of(
                        new CheckPrimitive(ComparisonType.GreaterThan, 6.0),
                        new CheckPrimitive(ComparisonType.LowerThan, 8.0)
                )),
                new CheckConjunction(SSet.of(
                        new CheckPrimitive(ComparisonType.GreaterEquals, 2.0),
                        new CheckPrimitive(ComparisonType.LowerEquals, 4.0)
                ))
        ));

        // --- Act
        var isValid1 = CheckExpressionEvaluation.evaluate(checkExpression, 1.0);
        var isValid2 = CheckExpressionEvaluation.evaluate(checkExpression, 2.0);
        var isValid3 = CheckExpressionEvaluation.evaluate(checkExpression, 3.0);
        var isValid4 = CheckExpressionEvaluation.evaluate(checkExpression, 4.0);
        var isValid5 = CheckExpressionEvaluation.evaluate(checkExpression, 5.0);
        var isValid6 = CheckExpressionEvaluation.evaluate(checkExpression, 6.0);
        var isValid7 = CheckExpressionEvaluation.evaluate(checkExpression, 7.0);
        var isValid8 = CheckExpressionEvaluation.evaluate(checkExpression, 8.0);
        var isValid9 = CheckExpressionEvaluation.evaluate(checkExpression, 9.0);

        // --- Assert
        Assertions.assertFalse(isValid1);
        Assertions.assertTrue(isValid2);
        Assertions.assertTrue(isValid3);
        Assertions.assertTrue(isValid4);
        Assertions.assertFalse(isValid5);
        Assertions.assertFalse(isValid6);
        Assertions.assertTrue(isValid7);
        Assertions.assertFalse(isValid8);
        Assertions.assertFalse(isValid9);
    }

    @Test
    void evaluateDefaultSetIsConjunction() {
        // --- Arrange
        var checkExpressionSet = SSet.<CheckExpression>of(
                        new CheckPrimitive(ComparisonType.GreaterEquals, 6.0),
                        new CheckPrimitive(ComparisonType.LowerEquals, 7.0)
                );

        // --- Act
        var isValid5 = CheckExpressionEvaluation.evaluate(checkExpressionSet, 5.0);
        var isValid6 = CheckExpressionEvaluation.evaluate(checkExpressionSet, 6.0);
        var isValid7 = CheckExpressionEvaluation.evaluate(checkExpressionSet, 7.0);
        var isValid8 = CheckExpressionEvaluation.evaluate(checkExpressionSet, 8.0);

        // --- Assert
        Assertions.assertFalse(isValid5);
        Assertions.assertTrue(isValid6);
        Assertions.assertTrue(isValid7);
        Assertions.assertFalse(isValid8);
    }
}