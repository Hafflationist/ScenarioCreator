package de.mrobohm.data.primitives.synset;

import de.mrobohm.data.Language;

public record GermanSynset(int id) implements GlobalSynset {
    @Override
    public Language language() {
        return Language.German;
    }
}
