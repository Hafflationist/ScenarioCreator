package de.mrobohm.data;

import de.mrobohm.data.table.Table;

import java.util.List;

public record Schema(int id,
                     String name,
                     Context context,
                     List<Table> tables) {}

