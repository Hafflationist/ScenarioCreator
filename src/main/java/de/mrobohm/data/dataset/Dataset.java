package de.mrobohm.data.dataset;

import de.mrobohm.data.column.nesting.ColumnLeaf;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public record Dataset(Map<ColumnLeaf, Value> values) implements Comparable<Dataset> {

    @Override
    public int compareTo(@NotNull Dataset ds) {
        return this.toString().compareTo(ds.toString());
    }
}
