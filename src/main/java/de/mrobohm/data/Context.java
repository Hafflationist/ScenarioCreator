package de.mrobohm.data;

public record Context(SemanticDomain semanticDomain, Language language) {

    public static Context getDefault() {
        return new Context(new SemanticDomain(-1), Language.Technical);
    }
}
