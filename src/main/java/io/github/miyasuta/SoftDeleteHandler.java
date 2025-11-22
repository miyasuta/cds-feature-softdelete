package io.github.miyasuta;

import com.sap.cds.Result;
import com.sap.cds.ResultBuilder;
import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Predicate;
import com.sap.cds.ql.Select;
import com.sap.cds.ql.Update;
import com.sap.cds.ql.cqn.CqnExpand;
import com.sap.cds.ql.cqn.CqnPredicate;
import com.sap.cds.ql.cqn.CqnSelect;
import com.sap.cds.ql.cqn.CqnSelectListItem;
import com.sap.cds.ql.cqn.CqnUpdate;
import com.sap.cds.ql.cqn.Modifier;
import com.sap.cds.reflect.CdsAssociationType;
import com.sap.cds.reflect.CdsElement;
import com.sap.cds.reflect.CdsEntity;
import com.sap.cds.reflect.CdsModel;
import com.sap.cds.ql.cqn.AnalysisResult;
import com.sap.cds.ql.cqn.CqnAnalyzer;
import com.sap.cds.services.cds.ApplicationService;
import com.sap.cds.services.cds.CdsDeleteEventContext;
import com.sap.cds.services.cds.CdsReadEventContext;
import com.sap.cds.services.cds.CqnService;
import com.sap.cds.services.persistence.PersistenceService;
import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.Before;
import com.sap.cds.services.handler.annotations.HandlerOrder;
import com.sap.cds.services.handler.annotations.On;
import com.sap.cds.services.handler.annotations.ServiceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@ServiceName(value = "*", type = ApplicationService.class)
public class SoftDeleteHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(SoftDeleteHandler.class);
    private static final String ANNOTATION_SOFTDELETE_ENABLED = "@softdelete.enabled";
    private static final String FIELD_IS_DELETED = "isDeleted";
    private static final String FIELD_DELETED_AT = "deletedAt";
    private static final String FIELD_DELETED_BY = "deletedBy";

    /**
     * Intercepts DELETE operations and converts them to UPDATE operations that set soft delete fields.
     * Also cascades soft delete to composition children.
     */
    @On(event = CqnService.EVENT_DELETE)
    @HandlerOrder(HandlerOrder.EARLY)
    public void onDelete(CdsDeleteEventContext context) {
        CdsModel model = context.getModel();
        CdsEntity targetEntity = model.getEntity(context.getTarget().getQualifiedName());

        logger.info("onDelete called for entity: {}", context.getTarget().getQualifiedName());

        if (!isSoftDeleteEnabled(targetEntity)) {
            // Let the default DELETE handler proceed
            logger.info("Entity not soft-delete enabled, proceeding with normal DELETE");
            context.proceed();
            return;
        }

        logger.info("Intercepting DELETE for soft-delete enabled entity: {}", targetEntity.getQualifiedName());

        // Prepare soft delete data
        Instant now = Instant.now();
        String userName = context.getUserInfo().getName();
        if (userName == null || userName.isEmpty()) {
            userName = "system";
        }

        Map<String, Object> deletionData = new HashMap<>();
        deletionData.put(FIELD_IS_DELETED, true);
        deletionData.put(FIELD_DELETED_AT, now);
        deletionData.put(FIELD_DELETED_BY, userName);

        // Extract keys from the DELETE CQN using CqnAnalyzer
        CqnAnalyzer analyzer = CqnAnalyzer.create(model);
        AnalysisResult analysisResult = analyzer.analyze(context.getCqn());
        Map<String, Object> keys = analysisResult.rootKeys();

        logger.info("Extracted keys for soft delete: {}", keys);

        // Get the underlying database entity name from the service entity
        String dbEntityName = targetEntity.getQualifiedName();
        // If this is a projection, get the source entity
        if (targetEntity.query().isPresent()) {
            String source = targetEntity.query().get().ref().firstSegment();
            if (source != null) {
                dbEntityName = source;
            }
        }
        logger.info("Database entity name: {}", dbEntityName);

        // Convert DELETE to UPDATE using extracted keys
        CqnUpdate update = Update.entity(dbEntityName)
            .data(deletionData)
            .matching(keys);

        logger.info("Executing soft delete UPDATE: {}", update);

        // Use PersistenceService to run the update directly on the database
        PersistenceService db = context.getServiceCatalog().getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);
        Result result = db.run(update);
        logger.info("Soft delete UPDATE executed, affected rows: {}", result.rowCount());

        // Cascade soft delete to composition children
        softDeleteCompositionChildren(context, targetEntity, context.getCqn().where().orElse(null), deletionData);

        // Mark the event as completed and set result with affected row count (HTTP 204)
        long rowCount = result.rowCount();
        context.setResult(ResultBuilder.deletedRows((int) rowCount).result());
        logger.info("onDelete end, affected rows: {}", rowCount);
        context.setCompleted();
    }

    @On(event = CqnService.EVENT_DELETE)
    @HandlerOrder(HandlerOrder.LATE)
    public void debugLast(CdsDeleteEventContext ctx) {
        logger.warn("LAST DELETE handler reached (should NOT happen)");
    }


    /**
     * Automatically adds isDeleted = false filter to READ operations on soft-delete enabled entities.
     * Skips filtering if:
     * - User explicitly specified isDeleted in their query
     * - Query is a by-key access (direct access to single entity)
     */
    @Before(event = CqnService.EVENT_READ)
    public void beforeRead(CdsReadEventContext context) {
        CqnSelect select = context.getCqn();

        // Get target entity using qualified name
        CdsModel model = context.getModel();
        String targetName = context.getTarget().getQualifiedName();
        CdsEntity entity = model.getEntity(targetName);

        if (entity == null || !isSoftDeleteEnabled(entity)) {
            // Not a soft-delete enabled entity, proceed normally
            return;
        }

        logger.info("beforeRead called for entity: {}", targetName);
        logger.debug("Applying soft delete filter to READ for entity: {}", targetName);

        // Check if this is by-key access (skip main entity filtering for direct key access)
        boolean isByKeyAccess = isByKeyAccess(select, model);
        if (isByKeyAccess) {
            logger.debug("By-key access detected, skipping main entity filter but still processing expands");
        }

        // Check if user already specified isDeleted filter and get its value
        Boolean userIsDeletedValue = getIsDeletedValueFromWhere(select);
        boolean userSpecifiedIsDeleted = userIsDeletedValue != null;
        if (userSpecifiedIsDeleted) {
            logger.debug("User specified isDeleted filter with value: {}", userIsDeletedValue);
        }

        // Determine the isDeleted value to use for expand filters
        Boolean expandIsDeletedValue = null;

        if (isByKeyAccess) {
            // For by-key access, we need to check the parent's actual isDeleted value
            expandIsDeletedValue = getParentIsDeletedValue(context, select, entity);
            logger.debug("Parent isDeleted value for by-key access: {}", expandIsDeletedValue);
        } else if (userSpecifiedIsDeleted) {
            // User specified isDeleted filter, propagate that value to expands
            expandIsDeletedValue = userIsDeletedValue;
        } else {
            // Default: filter for non-deleted entities
            expandIsDeletedValue = false;
        }

        // Determine if we should apply main entity filter
        boolean applyMainFilter = !isByKeyAccess && !userSpecifiedIsDeleted;

        // Add isDeleted = false filter for main entity
        Predicate isDeletedFilter = CQL.get(FIELD_IS_DELETED).eq(false);

        // Final values for use in Modifier
        final Boolean finalExpandIsDeletedValue = expandIsDeletedValue;

        CqnSelect modifiedSelect = CQL.copy(select, new Modifier() {
            @Override
            public Predicate where(Predicate where) {
                if (!applyMainFilter) {
                    return where; // Don't modify main filter for by-key access
                }
                if (where != null) {
                    return CQL.and(where, isDeletedFilter);
                }
                return isDeletedFilter;
            }

            @Override
            public List<CqnSelectListItem> items(List<CqnSelectListItem> items) {
                return items.stream()
                    .map(item -> addFilterToExpandItem(item, model, entity, finalExpandIsDeletedValue))
                    .collect(Collectors.toList());
            }
        });

        context.setCqn(modifiedSelect);
    }

    /**
     * Gets the isDeleted value from the parent entity for by-key access.
     */
    private Boolean getParentIsDeletedValue(CdsReadEventContext context, CqnSelect select, CdsEntity entity) {
        try {
            // Extract keys from the select statement
            CdsModel model = context.getModel();
            CqnAnalyzer analyzer = CqnAnalyzer.create(model);
            AnalysisResult analysisResult = analyzer.analyze(select.ref());
            Map<String, Object> keys = analysisResult.rootKeys();

            if (keys.isEmpty()) {
                return false; // Default to false if no keys found
            }

            // Get the database entity name
            String dbEntityName = entity.getQualifiedName();
            if (entity.query().isPresent()) {
                String source = entity.query().get().ref().firstSegment();
                if (source != null) {
                    dbEntityName = source;
                }
            }

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

            return false; // Default to false if not found
        } catch (Exception e) {
            logger.warn("Failed to get parent isDeleted value, defaulting to false", e);
            return false;
        }
    }

    /**
     * Extracts the isDeleted value from the WHERE clause if present.
     * Returns null if isDeleted is not specified, true/false if specified.
     */
    private Boolean getIsDeletedValueFromWhere(CqnSelect select) {
        if (!select.where().isPresent()) {
            return null;
        }
        return extractIsDeletedValue(select.where().get());
    }

    /**
     * Recursively extracts the isDeleted value from a predicate.
     */
    private Boolean extractIsDeletedValue(CqnPredicate predicate) {
        if (predicate instanceof com.sap.cds.ql.cqn.CqnComparisonPredicate) {
            com.sap.cds.ql.cqn.CqnComparisonPredicate comparison =
                (com.sap.cds.ql.cqn.CqnComparisonPredicate) predicate;

            if (comparison.left().isRef()) {
                String refName = comparison.left().asRef().lastSegment();
                if (FIELD_IS_DELETED.equals(refName) && comparison.right().isLiteral()) {
                    Object value = comparison.right().asLiteral().value();
                    if (value instanceof Boolean) {
                        return (Boolean) value;
                    }
                }
            }
        } else if (predicate instanceof com.sap.cds.ql.cqn.CqnConnectivePredicate) {
            com.sap.cds.ql.cqn.CqnConnectivePredicate connective =
                (com.sap.cds.ql.cqn.CqnConnectivePredicate) predicate;

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
     * Adds isDeleted filter to expand items if the target entity is soft-delete enabled.
     * @param expandIsDeletedValue The isDeleted value to filter on (true/false)
     */
    private CqnSelectListItem addFilterToExpandItem(CqnSelectListItem item, CdsModel model, CdsEntity parentEntity, Boolean expandIsDeletedValue) {
        if (!item.isExpand()) {
            return item;
        }

        CqnExpand expand = item.asExpand();
        String associationName = expand.ref().lastSegment();

        // Find the target entity through the association in the parent entity
        CdsEntity targetEntity = findTargetEntity(parentEntity, associationName);

        // Process nested items recursively (with the new target entity as parent)
        final CdsEntity finalTargetEntity = targetEntity;
        List<CqnSelectListItem> nestedItems = expand.items().stream()
            .map(nestedItem -> addFilterToExpandItem(nestedItem, model, finalTargetEntity, expandIsDeletedValue))
            .collect(Collectors.toList());

        // Determine if we need to add the soft delete filter
        boolean needsSoftDeleteFilter = targetEntity != null && isSoftDeleteEnabled(targetEntity);

        // If no changes needed, return original item
        if (!needsSoftDeleteFilter && nestedItems.equals(expand.items())) {
            return item;
        }

        if (needsSoftDeleteFilter) {
            logger.debug("Adding soft delete filter to expand: {} with isDeleted={}", associationName, expandIsDeletedValue);
        }

        // Build the filter
        CqnPredicate existingFilter = expand.ref().targetSegment().filter().orElse(null);
        CqnPredicate newFilter = existingFilter;

        if (needsSoftDeleteFilter && expandIsDeletedValue != null) {
            Predicate softDeleteFilter = CQL.get(FIELD_IS_DELETED).eq(expandIsDeletedValue);
            newFilter = existingFilter != null
                ? CQL.and((Predicate) existingFilter, softDeleteFilter)
                : softDeleteFilter;
        }

        // Create new expand using CQL.to().expand()
        if (nestedItems.isEmpty()) {
            if (newFilter != null) {
                return CQL.to(associationName).filter((Predicate) newFilter).expand();
            } else {
                return CQL.to(associationName).expand();
            }
        } else {
            if (newFilter != null) {
                return CQL.to(associationName).filter((Predicate) newFilter)
                    .expand(nestedItems.toArray(new CqnSelectListItem[0]));
            } else {
                return CQL.to(associationName)
                    .expand(nestedItems.toArray(new CqnSelectListItem[0]));
            }
        }
    }

    /**
     * Finds the target entity of an association element.
     */
    private CdsEntity findTargetEntity(CdsEntity parentEntity, String associationName) {
        if (parentEntity == null) {
            return null;
        }
        Optional<CdsElement> assocElement = parentEntity.findElement(associationName);
        if (assocElement.isPresent() && assocElement.get().getType().isAssociation()) {
            CdsAssociationType assocType = (CdsAssociationType) assocElement.get().getType();
            return assocType.getTarget();
        }
        return null;
    }

    /**
     * Recursively soft deletes composition children of a given entity.
     */
    private void softDeleteCompositionChildren(CdsDeleteEventContext context, CdsEntity entity,
                                               CqnPredicate parentKeys, Map<String, Object> deletionData) {
        List<CdsElement> compositionElements = getCompositionElements(entity);

        for (CdsElement element : compositionElements) {
            CdsAssociationType assocType = (CdsAssociationType) element.getType();
            CdsEntity childEntity = assocType.getTarget();

            if (!isSoftDeleteEnabled(childEntity)) {
                logger.debug("Composition child '{}' is not soft-delete enabled, skipping",
                    childEntity.getQualifiedName());
                continue;
            }

            logger.debug("Cascading soft delete to composition child: {}", childEntity.getQualifiedName());

            try {
                // Get the underlying database entity name for the child
                String childDbEntityName = childEntity.getQualifiedName();
                if (childEntity.query().isPresent()) {
                    String source = childEntity.query().get().ref().firstSegment();
                    if (source != null) {
                        childDbEntityName = source;
                    }
                }

                // Build update for composition children
                // Note: This updates ALL children without WHERE clause - needs improvement
                // to only update children related to the deleted parent
                CqnUpdate childUpdate = Update.entity(childDbEntityName)
                    .data(deletionData);

                // Execute the update using PersistenceService
                PersistenceService db = context.getServiceCatalog().getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);
                db.run(childUpdate);

                // Recursively handle nested compositions
                softDeleteCompositionChildren(context, childEntity, null, deletionData);

            } catch (Exception e) {
                logger.warn("Failed to cascade soft delete to child entity '{}': {}",
                    childEntity.getQualifiedName(), e.getMessage());
            }
        }
    }

    /**
     * Returns all composition elements of an entity.
     */
    private List<CdsElement> getCompositionElements(CdsEntity entity) {
        return entity.elements()
            .filter(element -> element.getType().isAssociation())
            .filter(element -> {
                CdsAssociationType assocType = (CdsAssociationType) element.getType();
                return assocType.isComposition();
            })
            .collect(Collectors.toList());
    }

    /**
     * Checks if an entity has the @softdelete.enabled annotation.
     */
    private boolean isSoftDeleteEnabled(CdsEntity entity) {
        if (entity == null) {
            return false;
        }
        Boolean annotationValue = entity.getAnnotationValue(ANNOTATION_SOFTDELETE_ENABLED, Boolean.FALSE);
        return Boolean.TRUE.equals(annotationValue);
    }

    /**
     * Checks if a query is a by-key access (direct access using primary keys).
     * By-key access should not be filtered to allow direct access to soft-deleted entities.
     *
     * Key access: GET /Books(1001) - filter is on the ref segment (from.ref[0].where in Node.js)
     * Filter query: GET /Books?$filter=ID eq 1001 - filter is in WHERE clause
     */
    private boolean isByKeyAccess(CqnSelect select, CdsModel model) {
        // Check if the ref has a filter on the root segment (indicates by-key access like /Books(1001))
        // This is equivalent to checking from.ref[0].where in Node.js
        boolean result = select.ref().rootSegment().filter().isPresent();
        logger.debug("isByKeyAccess check: rootSegment filter present = {}", result);
        return result;
    }

    /**
     * Checks if a WHERE clause already contains a reference to isDeleted field.
     */
    private boolean hasIsDeletedInWhere(CqnPredicate predicate) {
        // This is a simplified implementation
        // A complete implementation would recursively traverse the predicate tree
        String predicateString = predicate.toString();
        return predicateString.contains(FIELD_IS_DELETED);
    }
}
