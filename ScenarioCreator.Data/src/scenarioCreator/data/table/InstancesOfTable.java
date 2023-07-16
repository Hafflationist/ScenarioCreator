package scenarioCreator.data.table;

import scenarioCreator.data.identification.Id;

import java.util.List;
import java.util.Map;

public record InstancesOfTable(Table table, List<Map<Id, String>> entries) {}
