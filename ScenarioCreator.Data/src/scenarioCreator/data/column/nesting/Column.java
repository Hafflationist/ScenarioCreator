package scenarioCreator.data.column.nesting;

import scenarioCreator.data.Entity;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;

import java.util.SortedSet;

public sealed interface Column extends Entity permits ColumnCollection, ColumnLeaf, ColumnNode {
    Id id();

    StringPlus name();

    default <T extends ColumnConstraint> boolean containsConstraint(Class<T> constraintType){
        return constraintSet().stream().anyMatch(c -> c.getClass().equals(constraintType));
    }

    SortedSet<ColumnConstraint> constraintSet();

    boolean isNullable();
}