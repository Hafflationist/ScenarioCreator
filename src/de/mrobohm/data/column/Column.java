package de.mrobohm.data.column;

import de.mrobohm.data.DataType;

import java.util.List;

public record Column(int id,
                     String name,
                     DataType dataType,
                     ColumnContext context,
                     List<ColumnConstraint> constraintList) {
}
