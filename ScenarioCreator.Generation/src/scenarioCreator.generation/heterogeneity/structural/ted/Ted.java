package scenarioCreator.generation.heterogeneity.structural.ted;

import scenarioCreator.data.Schema;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;

import java.io.IOException;

public final class Ted {
    private Ted() {
    }

    public static double calculateDistanceRelative(Schema schema1, Schema schema2) throws IOException {
        final var distanceAbsolute = calculateDistanceAbsolute(schema1, schema2);
        final var schemaSize1 = IdentificationNumberCalculator.getAllIds(schema1, true).count();
        final var schemaSize2 = IdentificationNumberCalculator.getAllIds(schema2, true).count();
        return (2.0 * distanceAbsolute) / (double) (schemaSize1 + schemaSize2);
    }

    public static int calculateDistanceAbsolute(Schema schema1, Schema schema2) throws IOException {
        return calculateDistanceAbsoluteInner(schema1, schema2, 0);
    }

    public static int calculateDistanceAbsoluteInner(Schema schema1, Schema schema2, int num) throws IOException {
        IntegrityChecker.assertValidSchema(schema1);
        IntegrityChecker.assertValidSchema(schema2);
        final var tedTree1 = Converter.schemaToTedTree(schema1);
        final var tedTree2 = Converter.schemaToTedTree(schema2);
        try {
            return TedTree.ZhangShasha(tedTree1, tedTree2);
        } catch (ArrayIndexOutOfBoundsException ex) {
            if (num == 12) {
                System.out.println("REEE!");
                return calculateDistanceAbsoluteInner(schema1, schema2, num + 1);
            } else if (num > 12) {
                return 1;
            }

            // https://i.kym-cdn.com/entries/icons/original/000/030/952/goofy.jpg
            return calculateDistanceAbsoluteInner(schema1, schema2, num + 1);
        }
    }
}
