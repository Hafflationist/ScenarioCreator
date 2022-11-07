package scenarioCreator.data.dataset;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.ColumnLeaf;

import java.util.Map;

public record Dataset(Map<ColumnLeaf, Value> values) implements Comparable<Dataset> {

    @Override
    public int compareTo(@NotNull Dataset ds) {
        return this.toString().compareTo(ds.toString());
    }
}
