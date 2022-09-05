package de.mrobohm.heterogenity.structural.ted;

import de.mrobohm.data.Schema;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;

import java.io.IOException;

public final class Ted {
    private Ted() {
    }

    public static double calculateDistanceRelative(Schema schema1, Schema schema2) throws IOException {
        final var distanceAbsolute = calculateDistanceAbsolute(schema1, schema2);
        final var rootSize = IdentificationNumberCalculator.getAllIds(schema1, true).count();
        final var schemaSize = IdentificationNumberCalculator.getAllIds(schema2, true).count();
        return (2.0 * distanceAbsolute) / (double) (rootSize + schemaSize);
    }

    public static int calculateDistanceAbsolute(Schema schema1, Schema schema2) throws IOException {
        final var tedTree1 = Converter.schemaToTedTree(schema1);
        final var tedTree2 = Converter.schemaToTedTree(schema2);
        try {
            return TedTree.ZhangShasha(tedTree1, tedTree2);
        } catch (ArrayIndexOutOfBoundsException ex) {
            // https://i.kym-cdn.com/entries/icons/original/000/030/952/goofy.jpg
            return calculateDistanceAbsolute(schema1, schema2);
        }
    }
}
