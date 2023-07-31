package scenarioCreator.data.table;

import scenarioCreator.data.column.nesting.Column;

import java.util.List;
import java.util.Map;

public record InstancesOfTable(Table table, List<Map<Column, String>> entries) {}
