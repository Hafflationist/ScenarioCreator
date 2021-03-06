package de.mrobohm.heterogenity.ted;

import de.mrobohm.data.Schema;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;

import java.io.IOException;

public final class Ted {
    private Ted() {
    }

    public static double calculateDistanceRelative(Schema schema1, Schema schema2) throws IOException {
        var distanceAbsolute = calculateDistanceAbsolute(schema1, schema2);
        var rootSize = IdentificationNumberCalculator.getAllIds(schema1, true).count();
        var schemaSize = IdentificationNumberCalculator.getAllIds(schema2, true).count();
        return (2.0 * distanceAbsolute) / (double) (rootSize + schemaSize);
    }

    public static int calculateDistanceAbsolute(Schema schema1, Schema schema2) throws IOException {
        var tedTree1 = Converter.schemaToTedTree(schema1);
        var tedTree2 = Converter.schemaToTedTree(schema2);
        return TedTree.ZhangShasha(tedTree1, tedTree2);
    }
}
