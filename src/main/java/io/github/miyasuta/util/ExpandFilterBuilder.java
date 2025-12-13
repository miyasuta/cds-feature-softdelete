package io.github.miyasuta.util;

import com.sap.cds.Result;
import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Predicate;
import com.sap.cds.ql.Select;
import com.sap.cds.ql.cqn.*;
import com.sap.cds.reflect.CdsEntity;
import com.sap.cds.reflect.CdsModel;
import com.sap.cds.services.cds.CdsReadEventContext;
import com.sap.cds.services.persistence.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Builds filters for expand items in CQN queries.
 * Handles propagation of isDeleted values to nested associations.
 */
public class ExpandFilterBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ExpandFilterBuilder.class);
    private static final String FIELD_IS_DELETED = "isDeleted";

    /**
     * Adds isDeleted filter to expand items if the target entity is soft-delete enabled.
     */
    public static CqnSelectListItem addFilterToExpandItem(CqnSelectListItem item, CdsModel model, CdsEntity parentEntity, Boolean expandIsDeletedValue) {
        if (!item.isExpand()) {
            return item;
        }

        CqnExpand expand = item.asExpand();
        String associationName = expand.ref().lastSegment();

        // Find the target entity through the association in the parent entity
        CdsEntity targetEntity = EntityMetadataHelper.findTargetEntity(parentEntity, associationName);

        // Process nested items recursively (with the new target entity as parent)
        final CdsEntity finalTargetEntity = targetEntity;
        List<CqnSelectListItem> nestedItems = expand.items().stream()
            .map(nestedItem -> addFilterToExpandItem(nestedItem, model, finalTargetEntity, expandIsDeletedValue))
            .collect(Collectors.toList());

        // Determine if we need to add the soft delete filter
        boolean needsSoftDeleteFilter = targetEntity != null && EntityMetadataHelper.isSoftDeleteEnabled(targetEntity);

        // If no changes needed, return original item
        if (!needsSoftDeleteFilter && nestedItems.equals(expand.items())) {
            return item;
        }

        if (needsSoftDeleteFilter) {
            logger.debug("Adding soft delete filter to expand: {} with isDeleted={}", associationName, expandIsDeletedValue);
        }

        // Build the filter
        CqnPredicate existingFilter = expand.ref().targetSegment().filter().orElse(null);
        Predicate newFilter = existingFilter != null ? (Predicate) existingFilter : null;

        if (needsSoftDeleteFilter && expandIsDeletedValue != null) {
            Predicate softDeleteFilter = CQL.get(FIELD_IS_DELETED).eq(expandIsDeletedValue);
            newFilter = newFilter != null ? CQL.and(newFilter, softDeleteFilter) : softDeleteFilter;
        }

        // Create new expand using CQL.to().expand()
        var toExpand = CQL.to(associationName);
        if (newFilter != null) {
            toExpand = toExpand.filter(newFilter);
        }

        return nestedItems.isEmpty()
            ? toExpand.expand()
            : toExpand.expand(nestedItems.toArray(new CqnSelectListItem[0]));
    }

    /**
     * Gets the isDeleted value from the parent entity for by-key access.
     */
    public static Boolean getParentIsDeletedValue(CdsReadEventContext context, CqnSelect select, CdsEntity entity) {
        try {
            // Extract keys from the select statement
            CdsModel model = context.getModel();
            CqnAnalyzer analyzer = CqnAnalyzer.create(model);
            AnalysisResult analysisResult = analyzer.analyze(select.ref());
            Map<String, Object> keys = new HashMap<>(analysisResult.rootKeys());
            EntityMetadataHelper.removeDraftKeys(keys);

            if (keys.isEmpty()) {
                return false;
            }

            String dbEntityName = EntityMetadataHelper.getDbEntityName(entity);

            // Query the parent's isDeleted value
            CqnSelect parentQuery = Select.from(dbEntityName)
                .columns(CQL.get(FIELD_IS_DELETED))
                .matching(keys);

            PersistenceService db = context.getServiceCatalog()
                .getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);
            Result result = db.run(parentQuery);

            if (result.rowCount() > 0) {
                Object isDeletedObj = result.single().get(FIELD_IS_DELETED);
                if (isDeletedObj instanceof Boolean) {
                    return (Boolean) isDeletedObj;
                }
            }

            return false;
        } catch (Exception e) {
            logger.warn("Failed to get parent isDeleted value, defaulting to false", e);
            return false;
        }
    }

    /**
     * Gets the isDeleted value from the parent entity in a navigation path.
     * For queries like Orders(ID=...,IsActiveEntity=true)/items, this queries the Order's isDeleted value.
     */
    public static Boolean getParentIsDeletedValueFromNavigation(CdsReadEventContext context, CqnSelect select, CdsModel model) {
        try {
            // Get the parent segment (first segment in the navigation path)
            var rootSegment = select.ref().rootSegment();
            String parentEntityName = rootSegment.id();

            // Get the parent entity
            CdsEntity parentEntity = model.getEntity(parentEntityName);
            if (parentEntity == null || !EntityMetadataHelper.isSoftDeleteEnabled(parentEntity)) {
                return false;
            }

            // Extract parent keys from the root segment filter
            if (!rootSegment.filter().isPresent()) {
                return false;
            }

            // Use CqnAnalyzer to extract keys from the parent segment
            CqnAnalyzer analyzer = CqnAnalyzer.create(model);
            AnalysisResult analysisResult = analyzer.analyze(select.ref());
            Map<String, Object> allKeys = new HashMap<>(analysisResult.rootKeys());

            // Filter to get only parent entity keys
            Map<String, Object> parentKeys = EntityMetadataHelper.filterKeysForEntity(allKeys, parentEntity);
            EntityMetadataHelper.removeDraftKeys(parentKeys);

            if (parentKeys.isEmpty()) {
                return false;
            }

            // Get the database entity name for the parent
            String dbEntityName = EntityMetadataHelper.getDbEntityName(parentEntity);

            // Query the parent's isDeleted value
            CqnSelect parentQuery = Select.from(dbEntityName)
                .columns(CQL.get(FIELD_IS_DELETED))
                .matching(parentKeys);

            PersistenceService db = context.getServiceCatalog()
                .getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);
            Result result = db.run(parentQuery);

            if (result.rowCount() > 0) {
                Object isDeletedObj = result.single().get(FIELD_IS_DELETED);
                if (isDeletedObj instanceof Boolean) {
                    return (Boolean) isDeletedObj;
                }
            }

            return false;
        } catch (Exception e) {
            logger.warn("Failed to get parent isDeleted value from navigation, defaulting to false", e);
            return false;
        }
    }
}
