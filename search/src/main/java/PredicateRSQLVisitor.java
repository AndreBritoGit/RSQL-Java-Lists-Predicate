import static java.util.Arrays.stream;

import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import cz.jirutka.rsql.parser.ast.LogicalNode;
import cz.jirutka.rsql.parser.ast.Node;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLOperators;
import cz.jirutka.rsql.parser.ast.RSQLVisitor;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * Implementation of RSQLVisitor so it may create predicates over a type to mach a query parameter.
 * <p>
 * Mostly copied from https://github.com/RutledgePaulV/q-builders the class
 * https://github.com/RutledgePaulV/q-builders/blob/develop/src/main/java/com/github/rutledgepaulv/qbuilders/visitors/PredicateVisitor.java
 * <p>
 * but was adapted to RSQL nomenclature and thus RSQL can be applyed to Java Lists and Streams
 *
 * @param <T> Type to apply the predicate test to
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PredicateRSQLVisitor<T> implements RSQLVisitor<Predicate<T>, Void> {

  private static final String INCOMPATIBLE_TYPES_PROVIDED = "Incompatible types provided.";

  @Override
  public Predicate<T> visit(AndNode node, Void param) {
    return t -> node.getChildren().stream().map(this::visitAny)
        .allMatch(p -> ((Predicate<T>) p).test(t));
  }

  @Override
  public Predicate<T> visit(OrNode node, Void param) {
    return t -> node.getChildren().stream().map(this::visitAny)
        .anyMatch(p -> ((Predicate<T>) p).test(t));
  }

  @Override
  public Predicate<T> visit(ComparisonNode node, Void param) {
    ComparisonOperator operator = node.getOperator();

    if (RSQLOperators.EQUAL.equals(operator)) {
      return single(node, this::equality);
    } else if (RSQLOperators.NOT_EQUAL.equals(operator)) {
      return single(node, this::inequality);
    } else if (RSQLOperators.GREATER_THAN.equals(operator)) {
      return single(node, this::greaterThan);
    } else if (RSQLOperators.LESS_THAN.equals(operator)) {
      return single(node, this::lessThan);
    } else if (RSQLOperators.GREATER_THAN_OR_EQUAL.equals(operator)) {
      return single(node, this::greaterThanOrEqualTo);
    } else if (RSQLOperators.LESS_THAN_OR_EQUAL.equals(operator)) {
      return single(node, this::lessThanOrEqualTo);
    } else if (RSQLOperators.IN.equals(operator)) {
      return multi(node, this::in);
    } else if (RSQLOperators.NOT_IN.equals(operator)) {
      return multi(node, this::nin);
    }

    throw new UnsupportedOperationException(
        "This visitor does not support the operator " + operator + ".");
  }


  protected boolean regex(Object actual, Object query) {
    Predicate<String> test;

    if (actual == null) {
      return false;
    }

    if (query instanceof String) {
      String queryRegex = (String) query;
      queryRegex = queryRegex.replace("*", ".*").toLowerCase(); // so we can use *findMe* in query.
      // Regex become .*findMe.*
      test = Pattern.compile(queryRegex).asPredicate();
    } else {
      return false;
    }

    if (actual.getClass().isArray()) {
      String[] values = (String[]) actual;
      return Arrays.stream(values).map(String::toLowerCase).anyMatch(test);
    } else if (Collection.class.isAssignableFrom(actual.getClass())) {
      Collection<String> values = (Collection<String>) actual;
      return values.stream().map(String::toLowerCase).anyMatch(test);
    } else if (actual instanceof String) {
      return test.test(((String) actual).toLowerCase());
    } else if (actual.getClass().isEnum()) {
      return test.test(((Enum) actual).name().toLowerCase());
    }

    return false;
  }

  protected boolean equality(Object actual, Object query) {
    if (actual != null && actual.getClass().isArray()) {
      Object[] values = (Object[]) actual;
      return stream(values).anyMatch(query::equals);
    } else if (actual != null && Collection.class.isAssignableFrom(actual.getClass())) {
      Collection<?> values = (Collection<?>) actual;
      return values.stream().anyMatch(query::equals);
    } else if (actual instanceof Number) {
      return isNumber(query)
          && ((Number) actual).doubleValue() == Double.parseDouble(query.toString());
    } else if (actual instanceof Boolean) {
      return (Boolean) actual == Boolean.getBoolean(query.toString());
    } else {
      return regex(actual, query);
    }
  }

  private boolean isNumber(Object query) {
    try {
      Double.parseDouble(query.toString());
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  protected boolean inequality(Object actual, Object query) {
    if (actual != null && actual.getClass().isArray()) {
      Object[] values = (Object[]) actual;
      return stream(values).noneMatch(query::equals);
    } else if (actual != null && Collection.class.isAssignableFrom(actual.getClass())) {
      Collection<?> values = (Collection<?>) actual;
      return values.stream().noneMatch(query::equals);
    } else if (actual instanceof Number) {
      return isNumber(query)
          && ((Number) actual).doubleValue() != Double.parseDouble(query.toString());
    } else if (actual instanceof Boolean) {
      return (Boolean) actual != Boolean.getBoolean(query.toString());
    } else {
      return actual != null && !regex(actual, query);
    }
  }

  protected boolean nin(Object actual, Collection<?> queries) {
    if (actual != null && actual.getClass().isArray()) {
      Object[] values = (Object[]) actual;
      return stream(values).noneMatch(queries::contains);
    } else if (actual != null && Collection.class.isAssignableFrom(actual.getClass())) {
      Collection<?> values = (Collection<?>) actual;
      return values.stream().noneMatch(queries::contains);
    } else {
      return !queries.contains(actual);
    }
  }

  protected boolean in(Object actual, Collection<?> queries) {
    if (actual != null && actual.getClass().isArray()) {
      Object[] values = (Object[]) actual;
      return stream(values).anyMatch(queries::contains);
    } else if (actual != null && Collection.class.isAssignableFrom(actual.getClass())) {
      Collection<?> values = (Collection<?>) actual;
      return values.stream().anyMatch(queries::contains);
    } else {
      return queries.contains(actual);
    }
  }

  protected boolean greaterThan(Object actual, Object query) {
    if (actual == null) {
      return false;
    }
    if (actual instanceof Number && isNumber(query)) {
      return ((Number) actual).doubleValue() > Double.parseDouble(query.toString());
    } else if (query instanceof String && actual instanceof String) {
      return ((String) actual).compareTo((String) query) > 0;
    } else {
      throw new UnsupportedOperationException(INCOMPATIBLE_TYPES_PROVIDED);
    }
  }

  protected boolean greaterThanOrEqualTo(Object actual, Object query) {
    if (actual == null) {
      return false;
    }
    if (actual instanceof Number && isNumber(query)) {
      return ((Number) actual).doubleValue() >= Double.parseDouble(query.toString());
    } else if (query instanceof String && actual instanceof String) {
      return ((String) actual).compareTo((String) query) >= 0;
    } else {
      throw new UnsupportedOperationException(INCOMPATIBLE_TYPES_PROVIDED);
    }
  }

  protected boolean lessThan(Object actual, Object query) {
    if (actual == null) {
      return false;
    }
    if (actual instanceof Number && isNumber(query)) {
      return ((Number) actual).doubleValue() < Double.parseDouble(query.toString());
    } else if (query instanceof String && actual instanceof String) {
      return ((String) actual).compareTo((String) query) < 0;
    } else {
      throw new UnsupportedOperationException(INCOMPATIBLE_TYPES_PROVIDED);
    }
  }

  protected boolean lessThanOrEqualTo(Object actual, Object query) {
    if (actual == null) {
      return false;
    }
    if (actual instanceof Number && isNumber(query)) {
      return ((Number) actual).doubleValue() <= Double.parseDouble(query.toString());
    } else if (query instanceof String && actual instanceof String) {
      return ((String) actual).compareTo((String) query) <= 0;
    } else {
      throw new UnsupportedOperationException(INCOMPATIBLE_TYPES_PROVIDED);
    }
  }



  private Predicate<T> single(ComparisonNode node, BiPredicate<Object, Object> func) {
    return t -> resolveSingleField(t, node.getSelector(), node, func);
  }

  private Predicate<T> multi(ComparisonNode node, BiPredicate<Object, Collection<?>> func) {
    return t -> resolveMultiField(t, node.getSelector(), node, func);
  }

  private boolean resolveSingleField(Object root, String field, ComparisonNode node,
      BiPredicate<Object, Object> func) {
    if (root == null || node.getSelector() == null) {
      return func.test(null, node.getArguments().iterator().next());
    } else {
      String[] splitField = field.split("\\.", 2);
      Object currentField = getFieldValueFromString(root, splitField[0]);
      if (splitField.length == 1) {
        return func.test(currentField, node.getArguments().iterator().next());
      } else {
        return recurseSingle(currentField, splitField[1], node, func);
      }
    }
  }

  private boolean recurseSingle(Object root, String field, ComparisonNode node,
      BiPredicate<Object, Object> func) {

    if (root != null && root.getClass().isArray()) {
      return Arrays.stream((Object[]) root).anyMatch(t -> resolveSingleField(t, field, node, func));
    }

    if (root instanceof Collection) {
      return ((Collection<Object>) root).stream()
          .anyMatch(t -> resolveSingleField(t, field, node, func));
    }

    return resolveSingleField(root, field, node, func);
  }

  private boolean resolveMultiField(Object root, String field, ComparisonNode node,
      BiPredicate<Object, Collection<?>> func) {
    if (root == null || node.getSelector() == null) {
      return func.test(null, node.getArguments());
    } else {
      String[] splitField = field.split("\\.", 2);
      Object currentField = getFieldValueFromString(root, splitField[0]);
      if (splitField.length == 1) {
        return func.test(currentField, node.getArguments());
      } else {
        return recurseMulti(currentField, splitField[1], node, func);
      }
    }
  }

  private boolean recurseMulti(Object root, String field, ComparisonNode node,
      BiPredicate<Object, Collection<?>> func) {

    if (root != null && root.getClass().isArray()) {
      return Arrays.stream((Object[]) root).anyMatch(t -> resolveMultiField(t, field, node, func));
    }

    if (root instanceof Collection) {
      return ((Collection<Object>) root).stream()
          .anyMatch(t -> resolveMultiField(t, field, node, func));
    }

    return resolveMultiField(root, field, node, func);
  }

  private Object getFieldValueFromString(Object o, String s) {
    if (o == null) {
      return null;
    }
    try {
      return FieldUtils.readField(o, s, true);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      return null;
    }
  }

  private T visitAny(Node node) {
    // skip straight to the children if it's a logical node with one member
    if (node instanceof LogicalNode) {
      LogicalNode logical = (LogicalNode) node;
      if (logical.getChildren().size() == 1) {
        return visitAny(logical.getChildren().get(0));
      }
    }

    if (node instanceof AndNode) {
      return (T) visit((AndNode) node, null);
    } else if (node instanceof OrNode) {
      return (T) visit((OrNode) node, null);
    } else {
      return (T) visit((ComparisonNode) node, null);
    }
  }


}
