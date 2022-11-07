package scenarioCreator.data.identification;

public sealed interface Id extends Comparable<Id> permits IdSimple, IdMerge, IdPart {
    @Override
    default int compareTo(Id id){
        return this.toString().compareTo(id.toString());
    }
}
