package de.mrobohm.operations.linguistic.helpers;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.StringPlusSemantical;
import de.mrobohm.data.primitives.StringPlusSemanticalSegment;
import de.mrobohm.data.primitives.synset.GlobalSynset;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

public class Translation {

    private final UnifiedLanguageCorpus _corpus;

    public Translation(UnifiedLanguageCorpus corpus) {
        _corpus = corpus;
    }

    private Language chooseDifferentLanguage(Language exception, Random random) {
        var rte = new RuntimeException("Fatal error! Not enough languages defined!");
        var newLanguageStream = Arrays.stream(Language.values())
                .filter(lang -> !Set.of(Language.Technical, Language.Mixed, exception).contains(lang));
        var newLanguage = StreamExtensions.pickRandomOrThrow(newLanguageStream, rte, random);
        assert !newLanguage.equals(exception) : "Local bug found!";
        return newLanguage;
    }

    @NotNull
    private Optional<StringPlusSemanticalSegment> translate(StringPlusSemanticalSegment segment, Random random) {
        if (segment.gssSet().isEmpty()) {
            return Optional.empty();
        }
        var rte = new RuntimeException("Should not happen.");
        var chosenGss = StreamExtensions.pickRandomOrThrow(segment.gssSet().stream(), rte, random);
        var targetLanguage = chooseDifferentLanguage(chosenGss.language(), random);
        var translationPossibilitySet = _corpus.translate(segment, targetLanguage);
        return StreamExtensions.tryPickRandom(translationPossibilitySet.stream(), random);
    }

    @NotNull
    public StringPlus translate(StringPlus name, Random random) {
        return switch (name) {
            case StringPlusNaked spn -> translateNaked(spn, random);
            case StringPlusSemantical sps -> {
                var rte = new RuntimeException("StringPlus without segments are invalid");
                var validSegmentStream = sps.segmentList().stream()
                        .filter(segment -> segment.gssSet().size() > 0
                                && !segment.gssSet().stream()
                                .map(GlobalSynset::language)
                                .allMatch(Set.of(Language.Mixed, Language.Technical)::contains));
                var chosenSegment = StreamExtensions.pickRandomOrThrow(
                        validSegmentStream, rte, random
                );
                var newSegmentOpt = translate(chosenSegment, random);
                if (newSegmentOpt.isEmpty()) {
                    throw new RuntimeException("StringPlus without valid segments! This should be prevented by <canBeTranslated>");
                }
                var newSegmentList = StreamExtensions.replaceInStream(
                        sps.segmentList().stream(), chosenSegment, newSegmentOpt.get()
                ).toList();
                yield sps.withSegmentList(newSegmentList);
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
            case StringPlusNaked spn -> !Set.of(Language.Mixed, Language.Technical).contains(spn.language());
            case StringPlusSemantical sps -> sps.language() != Language.Technical;
        };
    }
}