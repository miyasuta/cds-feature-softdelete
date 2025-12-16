package io.github.miyasuta.util;

import com.sap.cds.ql.cqn.CqnComparisonPredicate;
import com.sap.cds.ql.cqn.CqnConnectivePredicate;
import com.sap.cds.ql.cqn.CqnPredicate;
import com.sap.cds.ql.cqn.CqnSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes CQN queries to determine query characteristics and extract filter values.
 */
public class QueryAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(QueryAnalyzer.class);
    private static final String FIELD_IS_DELETED = "isDeleted";
    private static final String FIELD_IS_DELETED_DISPLAY = "isDeletedDisplay";

    /**
     * Checks if a query is a by-key access (direct access using primary keys).
     * Navigation paths (e.g., Orders(...)/items) should return false.
     */
    public static boolean isByKeyAccess(CqnSelect select) {
        // Check if there's a filter on the root segment (potential by-key access)
        if (!select.ref().rootSegment().filter().isPresent()) {
            return false;
        }

        // Check if this is a navigation path (ref has more than one segment)
        int segmentCount = select.ref().segments().size();
        if (segmentCount > 1) {
            logger.debug("Navigation path detected (ref segments: {}), not by-key access", segmentCount);
            return false;
        }

        return true;
    }

    /**
     * Checks if this is a navigation path (e.g., Orders(ID=...,IsActiveEntity=true)/items).
     * Navigation paths have multiple segments in the ref.
     */
    public static boolean isNavigationPath(CqnSelect select) {
        int segmentCount = select.ref().segments().size();
        if (segmentCount > 1) {
            // Check if the first segment has a filter (parent key access)
            boolean hasParentFilter = select.ref().rootSegment().filter().isPresent();
            if (hasParentFilter) {
                logger.debug("Navigation path detected: {} segments with parent filter", segmentCount);
                return true;
            }
        }
        return false;
    }

    /**
     * Extracts the isDeleted value from the WHERE clause if present.
     * Returns null if isDeleted is not specified, true/false if specified.
     */
    public static Boolean getIsDeletedValueFromWhere(CqnSelect select) {
        if (!select.where().isPresent()) {
            return null;
        }
        return extractIsDeletedValue(select.where().get());
    }

    /**
     * Recursively extracts the isDeleted value from a predicate.
     * Also checks for isDeletedDisplay field (ISSUE-009).
     */
    public static Boolean extractIsDeletedValue(CqnPredicate predicate) {
        if (predicate instanceof CqnComparisonPredicate) {
            CqnComparisonPredicate comparison = (CqnComparisonPredicate) predicate;

            if (comparison.left().isRef()) {
                String refName = comparison.left().asRef().lastSegment();
                // Check for both isDeleted and isDeletedDisplay fields (ISSUE-009)
                if ((FIELD_IS_DELETED.equals(refName) || FIELD_IS_DELETED_DISPLAY.equals(refName))
                    && comparison.right().isLiteral()) {
                    Object value = comparison.right().asLiteral().value();
                    if (value instanceof Boolean) {
                        return (Boolean) value;
                    }
                }
            }
        } else if (predicate instanceof CqnConnectivePredicate) {
            CqnConnectivePredicate connective = (CqnConnectivePredicate) predicate;

            for (CqnPredicate p : connective.predicates()) {
                Boolean value = extractIsDeletedValue(p);
                if (value != null) {
                    return value;
                }
            }
        }
        return null;
    }

    /**
     * Checks if the query is specifically for draft records (IsActiveEntity=false).
     * Returns true if the query contains IsActiveEntity=false in the WHERE clause or ref.
     */
    public static boolean isQueryingDraftRecords(CqnSelect select) {
        // Check WHERE clause
        if (select.where().isPresent() && extractIsActiveEntityValue(select.where().get())) {
            return true;
        }

        // Check if the ref contains IsActiveEntity=false (for navigation queries)
        try {
            if (select.ref() != null && select.ref().rootSegment() != null) {
                var filter = select.ref().rootSegment().filter();
                if (filter.isPresent()) {
                    return extractIsActiveEntityValue(filter.get());
                }
            }
        } catch (Exception e) {
            logger.debug("Error checking IsActiveEntity in ref", e);
        }

        return false;
    }

    /**
     * Recursively extracts the IsActiveEntity value from a predicate.
     * Returns true if IsActiveEntity=false is found, false otherwise.
     */
    public static boolean extractIsActiveEntityValue(CqnPredicate predicate) {
        if (predicate instanceof CqnComparisonPredicate) {
            CqnComparisonPredicate comparison = (CqnComparisonPredicate) predicate;

            if (comparison.left().isRef()) {
                String refName = comparison.left().asRef().lastSegment();
                if ("IsActiveEntity".equals(refName) && comparison.right().isLiteral()) {
                    Object value = comparison.right().asLiteral().value();
                    // Return true only if IsActiveEntity=false (querying draft records)
                    return Boolean.FALSE.equals(value);
                }
            }
        } else if (predicate instanceof CqnConnectivePredicate) {
            CqnConnectivePredicate connective = (CqnConnectivePredicate) predicate;

            for (CqnPredicate p : connective.predicates()) {
                if (extractIsActiveEntityValue(p)) {
                    return true;
                }
            }
        }
        return false;
    }
}
