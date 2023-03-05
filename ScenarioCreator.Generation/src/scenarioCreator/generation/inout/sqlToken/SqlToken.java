package scenarioCreator.generation.inout.sqlToken;

public sealed interface SqlToken extends SqlTokenMaybeWithBlock permits
        TokenBraceStart, TokenBraceEnd,
        TokenComma, TokenCommentStart, TokenCommentEnd,
        TokenCreate, TokenIgnore, TokenIdentifier,
        TokenNot, TokenReferences, TokenDataType,
        TokenTable, TokenSemicolon, TokenPrimary {
}