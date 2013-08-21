package clara.rules;

import java.util.List;

/**
 * Result of a Clara query.
 */
public interface QueryResult {

    public Object getResult(String fieldName);
}
