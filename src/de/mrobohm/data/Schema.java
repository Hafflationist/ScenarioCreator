package de.mrobohm.data;

import de.mrobohm.data.table.Table;

import java.util.List;
import java.util.Set;

public record Schema(int id,
                     String name,
                     Context context,
                     Set<Table> tableSet) {

    public Schema withId(int newId){
        return new Schema(newId, name, context, tableSet);
    }

    public Schema withName(String newName) {
        return new Schema(id, newName, context, tableSet);
    }

    public Schema withContext(Context newContext) {
        return new Schema(id, name, newContext, tableSet);
    }

    public Schema withTables(Set<Table> newTableSet) {
        return new Schema(id, name, context, newTableSet);
    }

}

