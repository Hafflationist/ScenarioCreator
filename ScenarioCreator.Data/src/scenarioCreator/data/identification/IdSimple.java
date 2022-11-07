package scenarioCreator.data.identification;

public record IdSimple(int number) implements Id {
    @Override
    public String toString() {
        return Integer.toString(number());
    }
}
