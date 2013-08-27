package clara.rules;

import java.util.List;
import java.util.Map;

/**
 * An immutable working memory of Clara rules.
 */
public interface WorkingMemory {

    /**
     * Returns a new working memory with the given facts inserted
     * @param facts facts to insert into the working memory
     * @return a new working memory with the facts inserted
     */
    public WorkingMemory insert(Iterable<Object> facts);

    /**
     * Returns a new working memory with the given facs retracted
     * @param facts facts to retract from the working memory
     * @return a new working memory with the facts retracted
     */
    public WorkingMemory retract(Iterable<Object> facts);

    /**
     * Fires any pending rules in the working memory, and returns a new
     * working memory with the rules in a fired state.
     *
     * @return a new working memory with the rules in a fired state.
     */
    public WorkingMemory fireRules();

    /**
     * Runs the query by the given name against the working memory and returns the matching
     * results. Query names are structured as "namespace/name"
     *
     * @param queryName the name of the query to perform, formatted as "namespace/name".
     * @param arguments query arguments
     * @return a list of query results
     */
    public Iterable<QueryResult> query(String queryName, Map<String,Object> arguments);
}
