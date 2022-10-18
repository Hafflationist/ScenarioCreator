package de.mrobohm.processing.tree;

import de.mrobohm.data.Schema;
import de.mrobohm.heterogenity.Distance;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public record SchemaWithAdditionalData(Schema schema, List<Distance> distanceList) implements Comparable<SchemaWithAdditionalData> {
    @Override
    public int compareTo(@NotNull SchemaWithAdditionalData o) {
        return schema.compareTo(o.schema);
    }
}
