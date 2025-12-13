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
import com.sap.cds.services.draft.DraftCancelEventContext;
import com.sap.cds.services.draft.DraftService;
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

@ServiceName(value = "*", type = {ApplicationService.class, DraftService.class})
public class SoftDeleteHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(SoftDeleteHandler.class);
    private static final String ANNOTATION_SOFTDELETE_ENABLED = "@softdelete.enabled";
    private static final String ANNOTATION_DRAFT_ENABLED = "@odata.draft.enabled";
    private static final String FIELD_IS_DELETED = "isDeleted";
    private static final String FIELD_DELETED_AT = "deletedAt";
    private static final String FIELD_DELETED_BY = "deletedBy";
    private static final List<String> DRAFT_VIRTUAL_KEYS = Arrays.asList("IsActiveEntity", "HasActiveEntity", "HasDraftEntity");

    /**
     * Intercepts draft cancel (delete) operations for draft-enabled entities.
     * This handler is triggered when deleting draft entities.
     * Note: Draft root deletion (discard) should NOT be converted to soft delete - it should be physical delete.
     * For draft children:
     * - If active entity exists (existing record): soft delete (set isDeleted=true)
     * - If active entity does not exist (new record): physical delete (proceed with deletion)
     */
    @On(event = DraftService.EVENT_DRAFT_CANCEL)
    @HandlerOrder(HandlerOrder.EARLY)
    public void onDraftCancel(DraftCancelEventContext context) {
        CdsModel model = context.getModel();
        CdsEntity targetEntity = model.getEntity(context.getTarget().getQualifiedName());

        if (!isSoftDeleteEnabled(targetEntity)) {
            context.proceed();
            return;
        }

        // Check if this is a draft root deletion or a child deletion
        // Draft root entities have @odata.draft.enabled annotation
        boolean isDraftRoot = isDraftRootEntity(targetEntity);

        if (isDraftRoot) {
            logger.debug("Draft root discard detected for entity: {} - using physical delete",
                targetEntity.getQualifiedName());
            context.proceed();
            return;
        }

        logger.debug("Draft child delete triggered for entity: {}", targetEntity.getQualifiedName());

        // Extract keys from the draft cancel CQN
        CqnAnalyzer analyzer = CqnAnalyzer.create(model);
        AnalysisResult analysisResult = analyzer.analyze(context.getCqn());

        // Try to get keys from targetKeys first, then rootKeys
        Map<String, Object> targetKeys = analysisResult.targetKeys();
        Map<String, Object> keys = targetKeys != null ? new HashMap<>(targetKeys) : new HashMap<>(analysisResult.rootKeys());

        // Filter keys to only include those that belong to the entity
        Map<String, Object> filteredKeys = filterKeysForEntity(keys, targetEntity);

        if (filteredKeys.isEmpty()) {
            logger.warn("Draft cancel request did not contain entity keys – skipping soft delete.");
            context.proceed();
            return;
        }

        // Check if active entity exists (to determine physical vs soft delete)
        boolean activeEntityExists = checkActiveEntityExists(context, targetEntity, filteredKeys);

        if (!activeEntityExists) {
            // New draft child (never activated): use physical delete
            logger.debug("Active entity does not exist for draft child - using physical delete");
            context.proceed();
            return;
        }

        // Existing draft child (previously activated): use soft delete
        logger.debug("Active entity exists for draft child - using soft delete");

        // Prepare soft delete data
        Instant now = Instant.now();
        String userName = context.getUserInfo().getName();
        if (userName == null || userName.isEmpty()) {
            userName = "system";
        }

        // Update draft entity with soft delete fields using DraftService.patchDraft
        Map<String, Object> deletionData = new HashMap<>();
        deletionData.put(FIELD_IS_DELETED, true);
        deletionData.put(FIELD_DELETED_AT, now);
        deletionData.put(FIELD_DELETED_BY, userName);
        // Include IsActiveEntity=false to target draft records
        deletionData.put("IsActiveEntity", false);

        // Use PersistenceService instead of DraftService.patchDraft to bypass @readonly restrictions
        PersistenceService db = context.getServiceCatalog()
            .getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);

        // Get draft table name (append _drafts suffix to service entity name)
        String draftTableName = targetEntity.getQualifiedName() + "_drafts";

        // Build the update query for draft entity
        // Include IsActiveEntity=false in the matching condition
        Map<String, Object> matchKeys = new HashMap<>(filteredKeys);
        matchKeys.put("IsActiveEntity", false);

        CqnUpdate update = Update.entity(draftTableName)
            .data(deletionData)
            .matching(matchKeys);

        db.run(update);

        // Cascade soft delete to composition children in draft mode
        softDeleteDraftCompositionChildren(context, db, targetEntity, filteredKeys, deletionData);

        // Mark as completed and return success
        context.setResult(ResultBuilder.deletedRows(1).result());
        context.setCompleted();
    }

    /**
     * Intercepts DELETE operations and converts them to UPDATE operations that set soft delete fields.
     * Also cascades soft delete to composition children.
     * This handles deletion of active (non-draft) entities.
     */
    @On(event = CqnService.EVENT_DELETE)
    @HandlerOrder(HandlerOrder.EARLY)
    public void onDelete(CdsDeleteEventContext context) {
        CdsModel model = context.getModel();
        CdsEntity targetEntity = model.getEntity(context.getTarget().getQualifiedName());

        if (!isSoftDeleteEnabled(targetEntity)) {
            context.proceed();
            return;
        }

        logger.debug("Soft delete triggered for entity: {}", targetEntity.getQualifiedName());

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
        Map<String, Object> keys = new HashMap<>(analysisResult.rootKeys());
        removeDraftKeys(keys);

        // Get the underlying database entity name from the service entity
        String dbEntityName = getDbEntityName(targetEntity);

        // Convert DELETE to UPDATE using extracted keys
        CqnUpdate update = Update.entity(dbEntityName)
            .data(deletionData)
            .matching(keys);

        // Use PersistenceService to run the update directly on the database
        PersistenceService db = context.getServiceCatalog().getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);
        Result result = db.run(update);

        // Cascade soft delete to composition children
        softDeleteCompositionChildren(context, targetEntity, keys, deletionData);

        // Mark the event as completed and set result with affected row count (HTTP 204)
        context.setResult(ResultBuilder.deletedRows((int) result.rowCount()).result());
        context.setCompleted();
    }

    /**
     * Automatically adds isDeleted = false filter to READ operations on soft-delete enabled entities.
     * Skips filtering if:
     * - User explicitly specified isDeleted in their query
     * - Query is a by-key access (direct access to single entity)
     * - Entity is a draft table (ends with _drafts suffix)
     * - Query is for draft records (IsActiveEntity=false in query)
     */
    @Before(event = CqnService.EVENT_READ)
    public void beforeRead(CdsReadEventContext context) {
        CqnSelect select = context.getCqn();

        // Get target entity using qualified name
        CdsModel model = context.getModel();
        String targetName = context.getTarget().getQualifiedName();
        CdsEntity entity = model.getEntity(targetName);

        // Log all READ requests for debugging
        boolean isDraft = entity != null && isDraftEntity(entity);
        boolean isQueryingDrafts = isDraft && isQueryingDraftRecords(select);
        logger.info("READ request for entity: {}, isDraftEntity: {}, endsWith_drafts: {}, isQueryingDrafts: {}",
            targetName,
            isDraft,
            targetName.endsWith("_drafts"),
            isQueryingDrafts);

        // Skip filtering for draft table (CAP draft activation needs to read soft-deleted draft records)
        if (targetName.endsWith("_drafts")) {
            logger.info("Skipping isDeleted filter for draft table: {}", targetName);
            return;
        }

        // Skip filtering if query is for draft records (IsActiveEntity=false)
        if (isQueryingDrafts) {
            logger.info("Skipping isDeleted filter for draft records query: {}", targetName);
            return;
        }

        if (entity == null || !isSoftDeleteEnabled(entity)) {
            return;
        }

        logger.debug("Applying soft delete filter for entity: {}", targetName);

        // Check if this is by-key access (skip main entity filtering for direct key access)
        boolean isByKeyAccess = isByKeyAccess(select);

        // Check if this is a navigation path (e.g., Orders(ID=...,IsActiveEntity=true)/items)
        boolean isNavigationPath = isNavigationPath(select);

        // Check if user already specified isDeleted filter and get its value
        Boolean userIsDeletedValue = getIsDeletedValueFromWhere(select);
        boolean userSpecifiedIsDeleted = userIsDeletedValue != null;

        // Determine the isDeleted value to use for main entity and expand filters
        Boolean mainIsDeletedValue;
        Boolean expandIsDeletedValue;

        if (isByKeyAccess) {
            // By-key access: query parent's isDeleted value for expand filtering
            expandIsDeletedValue = getParentIsDeletedValue(context, select, entity);
            mainIsDeletedValue = null; // Don't filter main entity for by-key access
        } else if (userSpecifiedIsDeleted) {
            // User specified isDeleted filter - ALWAYS prioritize user's explicit filter
            // This applies to both regular queries and navigation paths
            expandIsDeletedValue = userIsDeletedValue;
            mainIsDeletedValue = null; // User already specified filter
        } else if (isNavigationPath) {
            // Navigation path without explicit user filter: query parent's isDeleted value and apply to main entity
            mainIsDeletedValue = getParentIsDeletedValueFromNavigation(context, select, model);
            expandIsDeletedValue = mainIsDeletedValue;
            logger.info("Navigation path detected, parent isDeleted={}", mainIsDeletedValue);
        } else {
            // Default: filter for non-deleted entities
            expandIsDeletedValue = false;
            mainIsDeletedValue = false;
        }

        // Determine if we should apply main entity filter
        boolean applyMainFilter = !isByKeyAccess && !userSpecifiedIsDeleted && mainIsDeletedValue != null;

        // Add isDeleted filter for main entity
        final Boolean finalMainIsDeletedValue = mainIsDeletedValue;
        Predicate isDeletedFilter = finalMainIsDeletedValue != null ?
            CQL.get(FIELD_IS_DELETED).eq(finalMainIsDeletedValue) : null;

        // Final values for use in Modifier
        final Boolean finalExpandIsDeletedValue = expandIsDeletedValue;

        CqnSelect modifiedSelect = CQL.copy(select, new Modifier() {
            @Override
            public Predicate where(Predicate where) {
                if (!applyMainFilter || isDeletedFilter == null) {
                    return where; // Don't modify main filter
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
     * Checks if this is a navigation path (e.g., Orders(ID=...,IsActiveEntity=true)/items).
     * Navigation paths have multiple segments in the ref.
     */
    private boolean isNavigationPath(CqnSelect select) {
        // Navigation path has more than one segment
        // e.g., Orders(ID=1)/items has 2 segments: Orders and items
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
     * Gets the isDeleted value from the parent entity in a navigation path.
     * For queries like Orders(ID=...,IsActiveEntity=true)/items, this queries the Order's isDeleted value.
     */
    private Boolean getParentIsDeletedValueFromNavigation(CdsReadEventContext context, CqnSelect select, CdsModel model) {
        try {
            // Get the parent segment (first segment in the navigation path)
            var rootSegment = select.ref().rootSegment();
            String parentEntityName = rootSegment.id();

            // Get the parent entity
            CdsEntity parentEntity = model.getEntity(parentEntityName);
            if (parentEntity == null || !isSoftDeleteEnabled(parentEntity)) {
                return false; // Parent not found or not soft-delete enabled
            }

            // Extract parent keys from the root segment filter
            if (!rootSegment.filter().isPresent()) {
                return false; // No filter on parent
            }

            // Use CqnAnalyzer to extract keys from the parent segment
            CqnAnalyzer analyzer = CqnAnalyzer.create(model);
            AnalysisResult analysisResult = analyzer.analyze(select.ref());
            Map<String, Object> allKeys = new HashMap<>(analysisResult.rootKeys());

            // Filter to get only parent entity keys
            Map<String, Object> parentKeys = filterKeysForEntity(allKeys, parentEntity);
            removeDraftKeys(parentKeys);

            if (parentKeys.isEmpty()) {
                return false; // No parent keys found
            }

            // Get the database entity name for the parent
            String dbEntityName = getDbEntityName(parentEntity);

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
                    Boolean isDeleted = (Boolean) isDeletedObj;
                    logger.info("Parent entity {} isDeleted={}", parentEntityName, isDeleted);
                    return isDeleted;
                }
            }

            return false; // Default to false if not found
        } catch (Exception e) {
            logger.warn("Failed to get parent isDeleted value from navigation, defaulting to false", e);
            return false;
        }
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
            Map<String, Object> keys = new HashMap<>(analysisResult.rootKeys());
            removeDraftKeys(keys);

            if (keys.isEmpty()) {
                return false;
            }

            String dbEntityName = getDbEntityName(entity);

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
     * @param context The delete event context
     * @param entity The parent entity
     * @param parentKeys The key values of the parent entity
     * @param deletionData The deletion metadata (isDeleted, deletedAt, deletedBy)
     */
    private void softDeleteCompositionChildren(CdsDeleteEventContext context, CdsEntity entity,
                                               Map<String, Object> parentKeys, Map<String, Object> deletionData) {
        List<CdsElement> compositionElements = getCompositionElements(entity);
        PersistenceService db = context.getServiceCatalog().getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);

        for (CdsElement element : compositionElements) {
            CdsAssociationType assocType = (CdsAssociationType) element.getType();
            CdsEntity childEntity = assocType.getTarget();

            if (!isSoftDeleteEnabled(childEntity)) {
                continue;
            }

            try {
                String childDbEntityName = getDbEntityName(childEntity);

                // Extract foreign key name from the composition's on clause
                String foreignKeyName = extractForeignKeyName(element, parentKeys);
                if (foreignKeyName == null) {
                    logger.warn("Could not determine foreign key for composition '{}', skipping cascade",
                        element.getName());
                    continue;
                }

                // Get the parent key value (assuming single key for simplicity)
                Object parentKeyValue = parentKeys.values().iterator().next();

                // First, query children to get their keys for recursive cascade
                CqnSelect childSelect = Select.from(childDbEntityName)
                    .where(CQL.get(foreignKeyName).eq(parentKeyValue));
                Result childrenResult = db.run(childSelect);

                // Update children with soft delete data, but ONLY if they are not already deleted
                // This prevents overwriting deletedAt/deletedBy metadata for items that were deleted earlier
                CqnUpdate childUpdate = Update.entity(childDbEntityName)
                    .data(deletionData)
                    .where(b -> b.get(foreignKeyName).eq(parentKeyValue)
                              .and(b.get("isDeleted").eq(false)));

                db.run(childUpdate);

                // Recursively handle nested compositions for each child
                for (var child : childrenResult) {
                    // Extract child's keys (excluding draft virtual keys)
                    Map<String, Object> childKeys = new HashMap<>();
                    for (String keyName : getEntityKeyNames(childEntity)) {
                        Object keyValue = child.get(keyName);
                        if (keyValue != null) {
                            childKeys.put(keyName, keyValue);
                        }
                    }

                    if (!childKeys.isEmpty()) {
                        softDeleteCompositionChildren(context, childEntity, childKeys, deletionData);
                    }
                }

            } catch (Exception e) {
                logger.warn("Failed to cascade soft delete to child entity '{}': {}",
                    childEntity.getQualifiedName(), e.getMessage());
            }
        }
    }

    /**
     * Extracts the foreign key name from a composition element.
     * For example, for composition "items" pointing to OrderItems with back-association "order",
     * it extracts "order_ID".
     */
    private String extractForeignKeyName(CdsElement element, Map<String, Object> parentKeys) {
        CdsAssociationType assocType = (CdsAssociationType) element.getType();
        CdsEntity targetEntity = assocType.getTarget();

        // Find the back-association in the target entity that points to the parent
        String associationName = null;
        for (var targetElement : targetEntity.elements().collect(Collectors.toList())) {
            if (targetElement.getType().isAssociation()) {
                CdsAssociationType targetAssocType = (CdsAssociationType) targetElement.getType();
                // Check if this association points back to parent entity
                String parentEntityName = element.getDeclaringType().getQualifiedName();
                String targetOfTarget = targetAssocType.getTarget().getQualifiedName();

                if (targetOfTarget.equals(parentEntityName)) {
                    associationName = targetElement.getName();
                    break;
                }
            }
        }

        if (associationName == null) {
            return null;
        }

        // Build foreign key name: [associationName]_[parentKeyName]
        String parentKeyName = parentKeys.keySet().iterator().next();
        return associationName + "_" + parentKeyName;
    }

    /**
     * Gets the key names of an entity, excluding draft virtual keys.
     */
    private List<String> getEntityKeyNames(CdsEntity entity) {
        return entity.elements()
            .filter(CdsElement::isKey)
            .map(CdsElement::getName)
            .filter(keyName -> !DRAFT_VIRTUAL_KEYS.contains(keyName))
            .collect(Collectors.toList());
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
     * Checks if an entity is a draft root entity (has @odata.draft.enabled annotation).
     * Draft root entities should use physical delete when discarded.
     * Draft child entities should use soft delete.
     */
    private boolean isDraftRootEntity(CdsEntity entity) {
        if (entity == null) {
            return false;
        }
        Boolean draftEnabled = entity.getAnnotationValue(ANNOTATION_DRAFT_ENABLED, Boolean.FALSE);
        return Boolean.TRUE.equals(draftEnabled);
    }

    /**
     * Checks if a query is a by-key access (direct access using primary keys).
     * Navigation paths (e.g., Orders(...)/items) should return false.
     */
    private boolean isByKeyAccess(CqnSelect select) {
        // Check if there's a filter on the root segment (potential by-key access)
        if (!select.ref().rootSegment().filter().isPresent()) {
            return false;
        }

        // Check if this is a navigation path (ref has more than one segment)
        // e.g., Orders(ID=1)/items would have 2 segments
        int segmentCount = select.ref().segments().size();
        if (segmentCount > 1) {
            logger.debug("Navigation path detected (ref segments: {}), not by-key access", segmentCount);
            return false;
        }

        return true;
    }

    /**
     * Removes draft-specific virtual keys from a map.
     */
    private void removeDraftKeys(Map<String, Object> keys) {
        DRAFT_VIRTUAL_KEYS.forEach(keys::remove);
    }

    /**
     * Gets the underlying database entity name from a service entity.
     */
    private String getDbEntityName(CdsEntity entity) {
        if (entity.query().isPresent()) {
            String source = entity.query().get().ref().firstSegment();
            if (source != null) {
                return source;
            }
        }
        return entity.getQualifiedName();
    }

    /**
     * Checks if an entity is a draft-enabled entity (has IsActiveEntity key).
     * In CAP Java, draft entities are identified by the IsActiveEntity virtual key,
     * not by a .drafts suffix like in Node.js.
     */
    private boolean isDraftEntity(CdsEntity entity) {
        if (entity == null) {
            return false;
        }
        // Check if entity has IsActiveEntity key (indicates draft-enabled entity)
        return entity.elements()
            .filter(CdsElement::isKey)
            .anyMatch(element -> "IsActiveEntity".equals(element.getName()));
    }

    /**
     * Checks if the query is specifically for draft records (IsActiveEntity=false).
     * Returns true if the query contains IsActiveEntity=false in the WHERE clause or ref.
     */
    private boolean isQueryingDraftRecords(CqnSelect select) {
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
    private boolean extractIsActiveEntityValue(CqnPredicate predicate) {
        if (predicate instanceof com.sap.cds.ql.cqn.CqnComparisonPredicate) {
            com.sap.cds.ql.cqn.CqnComparisonPredicate comparison =
                (com.sap.cds.ql.cqn.CqnComparisonPredicate) predicate;

            if (comparison.left().isRef()) {
                String refName = comparison.left().asRef().lastSegment();
                if ("IsActiveEntity".equals(refName) && comparison.right().isLiteral()) {
                    Object value = comparison.right().asLiteral().value();
                    // Return true only if IsActiveEntity=false (querying draft records)
                    return Boolean.FALSE.equals(value);
                }
            }
        } else if (predicate instanceof com.sap.cds.ql.cqn.CqnConnectivePredicate) {
            com.sap.cds.ql.cqn.CqnConnectivePredicate connective =
                (com.sap.cds.ql.cqn.CqnConnectivePredicate) predicate;

            for (CqnPredicate p : connective.predicates()) {
                if (extractIsActiveEntityValue(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Filters keys to only include those that belong to the target entity.
     * This is important for navigation path deletions (e.g., Orders(...)/items(...))
     * where keys contain both parent and child entity keys.
     */
    private Map<String, Object> filterKeysForEntity(Map<String, Object> keys, CdsEntity entity) {
        Map<String, Object> filtered = new HashMap<>();
        List<String> entityKeyNames = getEntityKeyNames(entity);

        for (Map.Entry<String, Object> entry : keys.entrySet()) {
            if (entityKeyNames.contains(entry.getKey())) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }

        return filtered;
    }

    /**
     * Checks if an active entity exists for the given draft entity.
     * Used to determine whether a draft child deletion should be physical (new record) or soft (existing record).
     * @param context The draft cancel event context
     * @param entity The draft entity
     * @param keys The entity keys (without draft virtual keys)
     * @return true if active entity exists, false otherwise
     */
    private boolean checkActiveEntityExists(DraftCancelEventContext context, CdsEntity entity, Map<String, Object> keys) {
        try {
            // Get the database entity name
            String dbEntityName = getDbEntityName(entity);

            // Query for active entity (without _drafts suffix, using entity keys only)
            CqnSelect activeQuery = Select.from(dbEntityName)
                .columns(CQL.get("ID")) // Just check existence, no need to fetch all columns
                .matching(keys);

            PersistenceService db = context.getServiceCatalog()
                .getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);

            Result result = db.run(activeQuery);

            boolean exists = result.rowCount() > 0;
            logger.debug("Active entity check for {}: exists={}", entity.getQualifiedName(), exists);
            return exists;

        } catch (Exception e) {
            logger.warn("Failed to check active entity existence, defaulting to soft delete", e);
            // If we can't determine, safer to use soft delete (preserve data)
            return true;
        }
    }

    /**
     * Recursively soft deletes composition children of a draft entity using PersistenceService.
     */
    private void softDeleteDraftCompositionChildren(DraftCancelEventContext context, PersistenceService db,
                                                     CdsEntity entity, Map<String, Object> parentKeys,
                                                     Map<String, Object> deletionData) {
        List<CdsElement> compositionElements = getCompositionElements(entity);

        for (CdsElement element : compositionElements) {
            CdsAssociationType assocType = (CdsAssociationType) element.getType();
            CdsEntity childEntity = assocType.getTarget();

            // Check if the child entity is soft-delete enabled
            if (!isSoftDeleteEnabled(childEntity)) {
                continue;
            }

            try {
                String childDraftTableName = childEntity.getQualifiedName() + "_drafts";

                // Extract foreign key name from the composition's on clause
                String foreignKeyName = extractForeignKeyName(element, parentKeys);
                if (foreignKeyName == null) {
                    logger.warn("Could not determine foreign key for composition '{}', skipping cascade",
                        element.getName());
                    continue;
                }

                // Get the parent key value (assuming single key for simplicity)
                Object parentKeyValue = parentKeys.values().iterator().next();

                // First, query draft children to get their keys for recursive cascade
                CqnSelect childSelect = Select.from(childDraftTableName)
                    .where(CQL.get(foreignKeyName).eq(parentKeyValue));

                Result childrenResult = db.run(childSelect);

                // For each child, update with soft delete data and recursively process
                for (var child : childrenResult) {
                    // Skip already deleted children to preserve their original deletion metadata
                    Object isDeleted = child.get("isDeleted");
                    if (isDeleted != null && (Boolean) isDeleted) {
                        continue;
                    }

                    // Extract child's keys
                    Map<String, Object> childKeys = new HashMap<>();
                    for (String keyName : getEntityKeyNames(childEntity)) {
                        Object keyValue = child.get(keyName);
                        if (keyValue != null) {
                            childKeys.put(keyName, keyValue);
                        }
                    }

                    if (!childKeys.isEmpty()) {
                        // Include IsActiveEntity=false in the matching condition
                        Map<String, Object> childMatchKeys = new HashMap<>(childKeys);
                        childMatchKeys.put("IsActiveEntity", false);

                        // Extract deletionData without IsActiveEntity (only isDeleted, deletedAt, deletedBy)
                        Map<String, Object> childDeletionData = new HashMap<>();
                        childDeletionData.put(FIELD_IS_DELETED, deletionData.get(FIELD_IS_DELETED));
                        childDeletionData.put(FIELD_DELETED_AT, deletionData.get(FIELD_DELETED_AT));
                        childDeletionData.put(FIELD_DELETED_BY, deletionData.get(FIELD_DELETED_BY));

                        CqnUpdate childUpdate = Update.entity(childDraftTableName)
                            .data(childDeletionData)
                            .matching(childMatchKeys);

                        db.run(childUpdate);

                        // Recursively handle nested compositions
                        softDeleteDraftCompositionChildren(context, db, childEntity, childKeys, deletionData);
                    }
                }

            } catch (Exception e) {
                logger.warn("Failed to cascade soft delete to draft child entity '{}': {}",
                    childEntity.getQualifiedName(), e.getMessage());
            }
        }
    }

}
