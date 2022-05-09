package de.mrobohm.data.column;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;

public record ColumnContext(Context context, Encoding encoding, UnitOfMeasure unitOfMeasure, Language language) {
    // TODO: hier müsste man genauer schauen, wie die Wissensbasis aufgebaut sein wird.
}
