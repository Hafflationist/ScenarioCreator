package scenarioCreator.data.primitives.synset;

import scenarioCreator.data.Language;

public record GermanSynset(int id) implements GlobalSynset {
    @Override
    public Language language() {
        return Language.German;
    }
}
