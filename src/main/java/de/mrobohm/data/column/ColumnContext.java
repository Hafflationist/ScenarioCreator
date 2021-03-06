package de.mrobohm.data.column;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public record ColumnContext(Context context, Encoding encoding, UnitOfMeasure unitOfMeasure, Language language)
        implements Comparable<ColumnContext> {

    @Contract(pure = true)
    @NotNull
    public ColumnContext witchContext(Context newContext) {
        return new ColumnContext(newContext, encoding, unitOfMeasure, language);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext withEncoding(Encoding newEncoding) {
        return new ColumnContext(context, newEncoding, unitOfMeasure, language);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext withUnitOfMeasure(UnitOfMeasure newUnitOfMeasure) {
        return new ColumnContext(context, encoding, newUnitOfMeasure, language);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext withLanguage(Language newLanguage) {
        return new ColumnContext(context, encoding, unitOfMeasure, newLanguage);
    }

    public static ColumnContext getDefault() {
        return new ColumnContext(Context.getDefault(), Encoding.UTF, UnitOfMeasure.Pure, Language.Technical);
    }

    @Override
    public int compareTo(@NotNull ColumnContext context) {
        return this.toString().compareTo(context.toString());
    }
}
