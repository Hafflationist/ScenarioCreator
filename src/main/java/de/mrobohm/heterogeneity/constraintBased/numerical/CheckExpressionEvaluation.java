package de.mrobohm.heterogeneity.constraintBased.numerical;

import de.mrobohm.data.column.constraint.numerical.CheckConjunction;
import de.mrobohm.data.column.constraint.numerical.CheckDisjunction;
import de.mrobohm.data.column.constraint.numerical.CheckExpression;
import de.mrobohm.data.column.constraint.numerical.CheckPrimitive;

import java.util.SortedSet;

public class CheckExpressionEvaluation {
    public static boolean evaluate(SortedSet<CheckExpression> checkExpression, double value) {
        return evaluateConjunction(new CheckConjunction(checkExpression), value);
    }

    public static boolean evaluate(CheckExpression checkExpression, double value) {
        return switch (checkExpression) {
            case CheckPrimitive cp -> evaluatePrimitive(cp, value);
            case CheckConjunction cc -> evaluateConjunction(cc, value);
            case CheckDisjunction cd -> evaluateDisjunction(cd, value);
        };
    }

    private static boolean evaluatePrimitive(CheckPrimitive checkPrimitive, double value) {
        return switch (checkPrimitive.comparisonType()) {
            case LowerThan -> value < checkPrimitive.value();
            case LowerEquals -> value <= checkPrimitive.value();
            case GreaterEquals -> value >= checkPrimitive.value();
            case GreaterThan -> value > checkPrimitive.value();
        };
    }

    private static boolean evaluateConjunction(CheckConjunction checkConjunction, double value) {
        return checkConjunction.conjunctiveCheckExpressionSet().stream()
                .map(ce -> CheckExpressionEvaluation.evaluate(ce, value))
                .reduce((a, b) -> a && b)
                .orElse(true);
    }

    private static boolean evaluateDisjunction(CheckDisjunction checkDisjunction, double value) {
        return checkDisjunction.disjunctiveCheckExpressionSet().stream()
                .map(ce -> CheckExpressionEvaluation.evaluate(ce, value))
                .reduce((a, b) -> a || b)
                .orElse(false);
    }
}