package de.mrobohm.data.column.nesting;

import de.mrobohm.data.Entity;
import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;

import java.util.Set;

public sealed interface Column extends Entity permits ColumnCollection, ColumnLeaf, ColumnNode {
    Id id();

    StringPlus name();

    Set<ColumnConstraint> constraintSet();

    boolean isNullable();
}