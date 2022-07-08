package de.mrobohm.operations.linguistic.helpers;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;

public class Translation {

    private final UnifiedLanguageCorpus _corpus;
    public Translation(UnifiedLanguageCorpus corpus) {
        _corpus = corpus;
    }

    @NotNull
    public StringPlus translate(StringPlus name, Random random) {
        return switch(name) {
            case StringPlusNaked spn -> translateNaked(spn, random);
            case StringPlusSemantical sps -> {
                // TODO: Implement me!
                throw new RuntimeException("implement me!");
            }
        };
    }

    @NotNull
    private StringPlus translateNaked(StringPlusNaked name, Random random) {
        return switch (name.language()) {
            case English:
                var germanRawString = translate(name.rawString(), Language.German, random);
                // TODO: get translation
                yield new StringPlusNaked(germanRawString, Language.German);

            case German:
                var englishRawString = translate(name.rawString(), Language.English, random);
                // TODO: get translation
                yield new StringPlusNaked(englishRawString, Language.English);

            case Mixed:
                var newLanguage = (random.nextInt() % 2 == 0) ? Language.German : Language.English;
                var newRawString = name.rawString();
                // TODO: get translation
                yield new StringPlusNaked(newRawString, newLanguage);

            default:
                throw new IllegalStateException("Unexpected value: " + name.language());
        };
    }

    @NotNull
    public String translate(String string, Language targetLanguage, Random random) {
        // TODO: Use a lib
        return string;
    }

    public boolean canBeTranslated(StringPlus stringPlus) {
        return switch (stringPlus) {
            case StringPlusNaked spn -> ! Set.of(Language.Mixed, Language.Technical).contains(spn.language());
            case StringPlusSemantical sps -> sps.language() != Language.Technical;
        };
    }
}