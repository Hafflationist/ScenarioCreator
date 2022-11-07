package scenarioCreator.generation.processing.tree;

public record TreeGenerationDefinition(
        boolean keepForeignKeyIntegrity,
        boolean shouldConserveAllRecords,
        boolean shouldStayNormalized,
        boolean conservesFlatRelations
) {
}