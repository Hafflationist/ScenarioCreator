package de.mrobohm.data.column.context;

import de.mrobohm.data.Context;
import de.mrobohm.data.Language;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public record ColumnContext(Context context, Encoding encoding, UnitOfMeasure unitOfMeasure,
                            Language language, NumericalDistribution numericalDistribution)
        implements Comparable<ColumnContext> {

    public static ColumnContext getDefault() {
        return new ColumnContext(
                Context.getDefault(), Encoding.UTF, UnitOfMeasure.Pure,
                Language.Technical, NumericalDistribution.getDefault()
        );
    }

    public static ColumnContext getDefaultWithNd(NumericalDistribution nd) {
        return new ColumnContext(
                Context.getDefault(), Encoding.UTF, UnitOfMeasure.Pure,
                Language.Technical, nd
        );
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext witchContext(Context newContext) {
        return new ColumnContext(newContext, encoding, unitOfMeasure, language, numericalDistribution);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext withEncoding(Encoding newEncoding) {
        return new ColumnContext(context, newEncoding, unitOfMeasure, language, numericalDistribution);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext withUnitOfMeasure(UnitOfMeasure newUnitOfMeasure) {
        return new ColumnContext(context, encoding, newUnitOfMeasure, language, numericalDistribution);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext withLanguage(Language newLanguage) {
        return new ColumnContext(context, encoding, unitOfMeasure, newLanguage, numericalDistribution);
    }

    @Contract(pure = true)
    @NotNull
    public ColumnContext withNumericalDistribution(NumericalDistribution newNumericalDistribution) {
        return new ColumnContext(context, encoding, unitOfMeasure, language, newNumericalDistribution);
    }

    @Override
    public int compareTo(@NotNull ColumnContext context) {
        return this.toString().compareTo(context.toString());
    }
}