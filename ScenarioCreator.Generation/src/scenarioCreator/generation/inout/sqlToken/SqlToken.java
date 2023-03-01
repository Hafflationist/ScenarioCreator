package scenarioCreator.generation.inout.sqlToken;

public sealed interface SqlToken permits
        TokenComma, TokenCommentStart, TokenCommentEnd,
        TokenCreate, TokenIgnore, TokenIdentifier,
        TokenNot, TokenDataType, TokenTable {
}