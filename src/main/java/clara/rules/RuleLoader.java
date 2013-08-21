package clara.rules;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

/**
 * Clara rule loader.
 */
public class RuleLoader {

    /**
     * Function to make a new Clara session.
     */
    static final IFn makeSession;

    static {

        Var require = RT.var("clojure.core", "require");

        require.invoke(Symbol.intern("clara.rules.java"));

        makeSession = RT.var("clara.rules.java", "mk-java-session");
    }

    /**
     * Returns a new working memory with rules loaded from the given namespaces.
     *
     * @param namespaces namespaces from which to load rules
     * @return an empty working memory with rules from the given namespaces.
     */
    public static WorkingMemory loadRules(String... namespaces) {

        return (WorkingMemory) makeSession.invoke(namespaces);
    }
}
