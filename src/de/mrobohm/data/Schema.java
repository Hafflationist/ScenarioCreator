package de.mrobohm.data;

import de.mrobohm.data.table.Table;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public record Schema(int id,
                     String name,
                     Context context,
                     Set<Table> tableSet) {

    @Contract(pure = true)
    @NotNull
    public Schema withId(int newId) {
        return new Schema(newId, name, context, tableSet);
    }

    @Contract(pure = true)
    @NotNull
    public Schema withName(String newName) {
        return new Schema(id, newName, context, tableSet);
    }

    @Contract(pure = true)
    @NotNull
    public Schema withContext(Context newContext) {
        return new Schema(id, name, newContext, tableSet);
    }

    @Contract(pure = true)
    @NotNull
    public Schema withTables(Set<Table> newTableSet) {
        return new Schema(id, name, context, newTableSet);
    }

}

