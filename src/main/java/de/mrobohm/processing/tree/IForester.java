package de.mrobohm.processing.tree;

import de.mrobohm.data.Schema;

import java.util.Random;
import java.util.SortedSet;

public interface IForester {

    SchemaWithAdditionalData createNext(
            SchemaWithAdditionalData rootSchema,
            TreeGenerationDefinition tgd,
            SortedSet<Schema> oldSchemaSet,
            Random random
    );

    @FunctionalInterface
    interface Injection {
        IForester get(
                DistanceDefinition validDefinition,
                DistanceDefinition targetDefinition
        );
    }
}
