package de.mrobohm.operations.structural;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.operations.ColumnTransformation;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class RemoveColumn implements ColumnTransformation {
    @Override
    @NotNull
    public List<Column> transform(Column column) {
        return new ArrayList<>();
    }

    @Override
    @NotNull
    public List<Column> getCandidates(List<Column> tableSet) {
        if (tableSet.size() == 1) {
            return new ArrayList<>();
        }
        return tableSet;
    }
}