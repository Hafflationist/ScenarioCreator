package scenarioCreator.data.primitives.synset;

import scenarioCreator.data.Language;

public record EnglishSynset(int offset, PartOfSpeech partOfSpeech) implements GlobalSynset {
    @Override
    public Language language() {
        return Language.English;
    }
}
