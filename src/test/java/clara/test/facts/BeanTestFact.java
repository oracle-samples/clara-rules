package clara.test.facts;

/**
 * A Java Pojo for the express purpose of testing the behavior of compilation and execution of rules.
 *
 * This class should not be included in the released artifact, if it did make it into a released artifact it should be
 * ignored and not consumed as it may be moved/removed without warning to consumers.
 */
public class BeanTestFact {
    private String[] locations;
    private String[] roadConditions;

    public BeanTestFact(String[] locations) {
        this.locations = locations;
    }

    // Standard and Indexed property accessors
    public void setLocations(String[] locations) {
        this.locations = locations;
    }
    public String[] getLocations() {
        return locations;
    }
    public void setLocations(int pos, String location) {
        locations[pos] = location;
    }
    public String getLocations(int pos){
        return locations[pos];
    }

    // Partial Indexed property accessor, ie. no standard accessor
    // See https://github.com/cerner/clara-rules/issues/446 for more details
    public void setRoadConditions(int pos, String condition) {
        roadConditions[pos] = condition;
    }
    public String getRoadConditions(int pos){
        return roadConditions[pos];
    }
}
