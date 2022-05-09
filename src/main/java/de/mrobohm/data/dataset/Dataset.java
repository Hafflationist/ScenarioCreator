package de.mrobohm.data.dataset;

import de.mrobohm.data.column.nesting.ColumnLeaf;

import java.util.Map;

public record Dataset(Map<ColumnLeaf, Value> values) {

}
