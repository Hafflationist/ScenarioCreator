package de.mrobohm.data;

import de.mrobohm.data.identification.Id;
import de.mrobohm.data.primitives.StringPlus;

public interface Entity {
    StringPlus name();
    Id id();
}
