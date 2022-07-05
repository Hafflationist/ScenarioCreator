package de.mrobohm.data.primitives.synset;

import de.mrobohm.data.Language;

public record EnglishSynset(int offset, PartOfSpeech partOfSpeech) implements GlobalSynset {
    @Override
    public Language language() {
        return Language.English;
    }
}
