package de.mrobohm.data.column;

import de.mrobohm.data.Context;

public record ColumnContext(Context context, Encoding encoding, UnitOfMeasure unitOfMeasure) {
    // TODO: hier müsste man genauer schauen, wie die Wissensbasis aufgebaut sein wird.
}
