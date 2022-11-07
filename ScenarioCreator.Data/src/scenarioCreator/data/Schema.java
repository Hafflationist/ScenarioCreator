package scenarioCreator.data;

import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.SortedSet;

public record Schema(Id id,
                     StringPlus name,
                     Context context,
                     SortedSet<Table> tableSet) implements Entity, Serializable {

    @Contract(pure = true)
    @NotNull
    public Schema withId(Id newId) {
        return new Schema(newId, name, context, tableSet);
    }

    @Contract(pure = true)
    @NotNull
    public Schema withName(StringPlus newName) {
        return new Schema(id, newName, context, tableSet);
    }

    @Contract(pure = true)
    @NotNull
    public Schema withContext(Context newContext) {
        return new Schema(id, name, newContext, tableSet);
    }

    @Contract(pure = true)
    @NotNull
    public Schema withTableSet(SortedSet<Table> newTableSet) {
        return new Schema(id, name, context, newTableSet);
    }

}