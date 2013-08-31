package clara.rules;

import java.util.List;

/**
 * Result of a Clara query. This is typically returned in
 * a list of results from the working memory.
 */
public interface QueryResult {

    /**
     * Returns the object matching the field specified in the query.
     */
    public Object getResult(String fieldName);
}
