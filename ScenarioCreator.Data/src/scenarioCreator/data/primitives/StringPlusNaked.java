package scenarioCreator.data.primitives;

import scenarioCreator.data.Language;

import java.util.function.BiFunction;


public record StringPlusNaked(String rawString, Language language) implements StringPlus {

    @Override
    public String rawString(BiFunction<NamingConvention, String[], String> merge) {
        return rawString;
    }

    public StringPlusNaked withRawString(String newRawString) {
        return new StringPlusNaked(newRawString, language);
    }
}