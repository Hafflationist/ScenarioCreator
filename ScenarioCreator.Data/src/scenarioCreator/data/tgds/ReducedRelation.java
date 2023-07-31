package scenarioCreator.data.tgds;

import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.table.Table;

import java.util.List;

public record ReducedRelation(Id relationId, StringPlus name, List<Column> columnList) {
    public static ReducedRelation fromTable(Table table) {
        final var columnList = table.columnList();
        final var relationId = table.id();
        return new ReducedRelation(relationId, table.name(), columnList);
    }
}
