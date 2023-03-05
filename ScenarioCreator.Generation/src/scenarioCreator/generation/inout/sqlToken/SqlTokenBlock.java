package scenarioCreator.generation.inout.sqlToken;

import java.util.List;

public record SqlTokenBlock(List<SqlTokenMaybeWithBlock> block) implements SqlTokenMaybeWithBlock {
}