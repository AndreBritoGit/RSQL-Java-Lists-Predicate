import com.koerber.lion.core.design.domain.exceptions.LionBaseException;
import com.koerber.lion.core.design.domain.models.LionBaseEntity;
import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Interface for applying filters to a List o <T> This Uses the RSQL
 * 
 * @param <T> Type To be filtered
 */
public interface SearchBaseService<T extends BaseEntity> {
  RSQLParser parser = new RSQLParser();

  /**
   * provides search to a service
   * 
   * @param filters to query elements
   * @return filtered list
   * @throws BaseException error
   */
  List<T> search(String filters) throws BaseException;

  /**
   * Applies filters to the given list
   *
   * @param filters to apply
   * @param results list to apply filters to
   * @return filtered list
   * @throws Exception something wrong
   */
  default List<T> search(String filters, List<T> results) throws BaseException {
    try {
      Node node = parser.parse(filters);
      PredicateRSQLVisitor<T> visitor = new PredicateRSQLVisitor<>();

      Predicate<T> queryFilters = node.accept(visitor);

      return results.stream().filter(queryFilters)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new BaseException(e);
    }

  }

}
