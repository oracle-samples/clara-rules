/**
 * The Java API for working Clara rules. It contains three simple pieces:
 *
 * <ul>
 *     <li>The {@link clara.rules.RuleLoader RuleLoader}, responsible for loading rules into a new working memory</li>
 *     <li>The {@link clara.rules.WorkingMemory WorkingMemory}, an immutable instance of a rule session.</li>
 *     <li>The {@link clara.rules.QueryResult QueryResult}, a container of query results.</li>
 * </ul>
 *
 * <p>
 * Note this API does not have a separate "knowledge base" class like those of other rules engines. Instead,
 * the user can simply create and reuse a single, empty WorkingMemory object for multiple rule instances -- optionally
 * sticking the initial empty working memory in a static variable. This type of pattern is efficient and possible
 * since the WorkingMemory is immutable, creating a new instance that shares internal state when changes occur.
 * </p>
 *
 * See the <a href="https://github.com/cerner/clara-examples/blob/main/src/main/java/clara/examples/java/ExampleMain.java">Clara Examples</a>
 * project for an example of this in action.
 */
package clara.rules;
