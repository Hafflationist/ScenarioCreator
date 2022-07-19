package de.mrobohm.processing.tree;

public record TreeTargetDefinition(
        boolean keepForeignKeyIntegrity,
        boolean shouldConserveAllRecords,
        boolean shouldStayNormalized,
        boolean conservesFlatRelations
) {
}