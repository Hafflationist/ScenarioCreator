package de.mrobohm.processing.tree;

public record TreeGenerationDefinition(
        boolean keepForeignKeyIntegrity,
        boolean shouldConserveAllRecords,
        boolean shouldStayNormalized,
        boolean conservesFlatRelations
) {
}