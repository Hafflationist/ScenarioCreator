package de.mrobohm.operations.linguistic.helpers;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import org.jetbrains.annotations.NotNull;

import java.util.Random;

public final class Translation {

    private Translation() {
    }

    @NotNull
    public static StringPlus translate(StringPlus name, Random random) {
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
    public static String translate(String string, Language targetLanguage, Random random) {
        // TODO: Use a lib
        return string;
    }
}