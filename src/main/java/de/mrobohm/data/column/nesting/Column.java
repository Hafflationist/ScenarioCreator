package de.mrobohm.data.column.nesting;

import de.mrobohm.data.Entity;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;
import org.jetbrains.annotations.NotNull;

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