package scenarioCreator.data.tgds;

import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;

import java.util.List;

public record ReducedRelation(Id relationId, List<Id> columnIdList) {
    public static ReducedRelation fromTable(Table table) {
        final var columnIdList = table.columnList().stream().map(Column::id).toList();
        final var relationId = table.id();
        return new ReducedRelation(relationId, columnIdList);
    }
}
