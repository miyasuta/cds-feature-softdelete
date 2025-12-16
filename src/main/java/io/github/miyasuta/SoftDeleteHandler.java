package io.github.miyasuta;

import com.sap.cds.Result;
import com.sap.cds.ResultBuilder;
import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Predicate;
import com.sap.cds.ql.Update;
import com.sap.cds.ql.cqn.CqnSelect;
import com.sap.cds.ql.cqn.CqnSelectListItem;
import com.sap.cds.ql.cqn.CqnUpdate;
import com.sap.cds.ql.cqn.CqnStructuredTypeRef;
import com.sap.cds.ql.cqn.CqnElementRef;
import com.sap.cds.ql.cqn.CqnValue;
import com.sap.cds.ql.cqn.Modifier;
import com.sap.cds.ql.RefBuilder;
import com.sap.cds.ql.StructuredTypeRef;
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
import io.github.miyasuta.util.CascadeDeleteHandler;
import io.github.miyasuta.util.EntityMetadataHelper;
import io.github.miyasuta.util.ExpandFilterBuilder;
import io.github.miyasuta.util.QueryAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Event handler for soft delete functionality.
 * Intercepts DELETE, DRAFT_CANCEL, and READ operations to implement soft delete behavior.
 */
@ServiceName(value = "*", type = {ApplicationService.class, DraftService.class})
public class SoftDeleteHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(SoftDeleteHandler.class);
    private static final String FIELD_IS_DELETED = "isDeleted";
    private static final String FIELD_DELETED_AT = "deletedAt";
    private static final String FIELD_DELETED_BY = "deletedBy";

    /**
     * Intercepts draft cancel (delete) operations for draft-enabled entities.
     * For draft children:
     * - If active entity exists (existing record): soft delete
     * - If active entity does not exist (new record): physical delete
     */
    @On(event = DraftService.EVENT_DRAFT_CANCEL)
    @HandlerOrder(HandlerOrder.EARLY)
    public void onDraftCancel(DraftCancelEventContext context) {
        CdsModel model = context.getModel();
        CdsEntity targetEntity = model.getEntity(context.getTarget().getQualifiedName());

        if (!EntityMetadataHelper.isSoftDeleteEnabled(targetEntity)) {
            context.proceed();
            return;
        }

        // Draft root entities should use physical delete when discarded
        if (EntityMetadataHelper.isDraftRootEntity(targetEntity)) {
            logger.debug("Draft root discard detected for entity: {} - using physical delete",
                targetEntity.getQualifiedName());
            context.proceed();
            return;
        }

        logger.debug("Draft child delete triggered for entity: {}", targetEntity.getQualifiedName());

        // Extract keys from the draft cancel CQN
        CqnAnalyzer analyzer = CqnAnalyzer.create(model);
        AnalysisResult analysisResult = analyzer.analyze(context.getCqn());

        Map<String, Object> targetKeys = analysisResult.targetKeys();
        Map<String, Object> keys = targetKeys != null ? new HashMap<>(targetKeys) : new HashMap<>(analysisResult.rootKeys());

        // Filter keys to only include those that belong to the entity
        Map<String, Object> filteredKeys = EntityMetadataHelper.filterKeysForEntity(keys, targetEntity);

        if (filteredKeys.isEmpty()) {
            logger.warn("Draft cancel request did not contain entity keys – skipping soft delete.");
            context.proceed();
            return;
        }

        // Check if active entity exists (to determine physical vs soft delete)
        boolean activeEntityExists = CascadeDeleteHandler.checkActiveEntityExists(context, targetEntity, filteredKeys);

        if (!activeEntityExists) {
            // New draft child (never activated): use physical delete
            logger.debug("Active entity does not exist for draft child - using physical delete");
            context.proceed();
            return;
        }

        // Existing draft child (previously activated): use soft delete
        logger.debug("Active entity exists for draft child - using soft delete");

        // Prepare soft delete data
        Map<String, Object> deletionData = prepareDeletionData(context.getUserInfo().getName());
        deletionData.put("IsActiveEntity", false);

        // Use PersistenceService to bypass @readonly restrictions
        PersistenceService db = context.getServiceCatalog()
            .getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);

        // Get draft table name
        String draftTableName = targetEntity.getQualifiedName() + "_drafts";

        // Build the update query for draft entity
        Map<String, Object> matchKeys = new HashMap<>(filteredKeys);
        matchKeys.put("IsActiveEntity", false);

        CqnUpdate update = Update.entity(draftTableName)
            .data(deletionData)
            .matching(matchKeys);

        db.run(update);

        // Cascade soft delete to composition children in draft mode
        CascadeDeleteHandler.softDeleteDraftCompositionChildren(context, db, targetEntity, filteredKeys, deletionData);

        // Mark as completed and return success
        context.setResult(ResultBuilder.deletedRows(1).result());
        context.setCompleted();
    }

    /**
     * Intercepts DELETE operations and converts them to UPDATE operations that set soft delete fields.
     * Also cascades soft delete to composition children.
     */
    @On(event = CqnService.EVENT_DELETE)
    @HandlerOrder(HandlerOrder.EARLY)
    public void onDelete(CdsDeleteEventContext context) {
        CdsModel model = context.getModel();
        CdsEntity targetEntity = model.getEntity(context.getTarget().getQualifiedName());

        if (!EntityMetadataHelper.isSoftDeleteEnabled(targetEntity)) {
            context.proceed();
            return;
        }

        logger.debug("Soft delete triggered for entity: {}", targetEntity.getQualifiedName());

        // Prepare soft delete data
        Map<String, Object> deletionData = prepareDeletionData(context.getUserInfo().getName());

        // Extract keys from the DELETE CQN
        CqnAnalyzer analyzer = CqnAnalyzer.create(model);
        AnalysisResult analysisResult = analyzer.analyze(context.getCqn());

        // Use targetKeys for navigation paths, rootKeys for direct access
        Map<String, Object> targetKeys = analysisResult.targetKeys();
        Map<String, Object> keys = targetKeys != null ? new HashMap<>(targetKeys) : new HashMap<>(analysisResult.rootKeys());
        EntityMetadataHelper.removeDraftKeys(keys);

        // Get the underlying database entity name
        String dbEntityName = EntityMetadataHelper.getDbEntityName(targetEntity);

        // Convert DELETE to UPDATE
        CqnUpdate update = Update.entity(dbEntityName)
            .data(deletionData)
            .matching(keys);

        // Use PersistenceService to run the update directly on the database
        PersistenceService db = context.getServiceCatalog().getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);
        Result result = db.run(update);

        // Cascade soft delete to composition children
        CascadeDeleteHandler.softDeleteCompositionChildren(context, targetEntity, keys, deletionData);

        // Mark the event as completed and set result with affected row count
        context.setResult(ResultBuilder.deletedRows((int) result.rowCount()).result());
        context.setCompleted();
    }

    /**
     * Automatically adds isDeleted = false filter to READ operations on soft-delete enabled entities.
     * Skips filtering for draft tables and draft record queries.
     */
    @Before(event = CqnService.EVENT_READ)
    public void beforeRead(CdsReadEventContext context) {
        CqnSelect select = context.getCqn();

        // Get target entity using qualified name
        CdsModel model = context.getModel();
        String targetName = context.getTarget().getQualifiedName();
        CdsEntity entity = model.getEntity(targetName);

        // ISSUE-009: Rewrite isDeletedDisplay references to isDeleted BEFORE early returns
        // This needs to happen for: WHERE clause, rootSegment filter, targetSegment filter
        // Navigation path queries have $filter in targetSegment, not WHERE
        select = rewriteIsDeletedDisplayInSelect(select);
        context.setCqn(select);

        // Skip filtering for draft table (CAP draft activation needs to read soft-deleted draft records)
        if (targetName.endsWith("_drafts")) {
            return;
        }

        // Skip filtering if query is for draft records (IsActiveEntity=false)
        boolean isDraft = entity != null && EntityMetadataHelper.isDraftEntity(entity);
        boolean isQueryingDrafts = isDraft && QueryAnalyzer.isQueryingDraftRecords(select);
        if (isQueryingDrafts) {
            return;
        }

        if (entity == null || !EntityMetadataHelper.isSoftDeleteEnabled(entity)) {
            return;
        }

        logger.debug("Applying soft delete filter for entity: {}", targetName);

        // Analyze query characteristics
        boolean isByKeyAccess = QueryAnalyzer.isByKeyAccess(select);
        boolean isNavigationPath = QueryAnalyzer.isNavigationPath(select);

        // Check if user already specified isDeleted filter
        Boolean userIsDeletedValue = QueryAnalyzer.getIsDeletedValueFromWhere(select);
        boolean userSpecifiedIsDeleted = userIsDeletedValue != null;

        // Determine the isDeleted value to use for main entity and expand filters
        Boolean mainIsDeletedValue;
        Boolean expandIsDeletedValue;

        if (isByKeyAccess) {
            // By-key access: query parent's isDeleted value for expand filtering
            expandIsDeletedValue = ExpandFilterBuilder.getParentIsDeletedValue(context, select, entity);
            mainIsDeletedValue = null; // Don't filter main entity for by-key access
        } else if (userSpecifiedIsDeleted) {
            // User specified isDeleted filter - prioritize user's explicit filter
            expandIsDeletedValue = userIsDeletedValue;
            mainIsDeletedValue = null; // User already specified filter
        } else if (isNavigationPath) {
            // Navigation path: query parent's isDeleted value and apply to main entity
            mainIsDeletedValue = ExpandFilterBuilder.getParentIsDeletedValueFromNavigation(context, select, model);
            expandIsDeletedValue = mainIsDeletedValue;
        } else {
            // Default: filter for non-deleted entities
            expandIsDeletedValue = false;
            mainIsDeletedValue = false;
        }

        // Determine if we should apply main entity filter
        boolean applyMainFilter = !isByKeyAccess && !userSpecifiedIsDeleted && mainIsDeletedValue != null;

        // Build isDeleted filter for main entity
        final Boolean finalMainIsDeletedValue = mainIsDeletedValue;
        Predicate isDeletedFilter = finalMainIsDeletedValue != null ?
            CQL.get(FIELD_IS_DELETED).eq(finalMainIsDeletedValue) : null;

        // Final values for use in Modifier
        final Boolean finalExpandIsDeletedValue = expandIsDeletedValue;

        // Apply filters using CQL.copy with Modifier
        CqnSelect modifiedSelect = CQL.copy(select, new Modifier() {
            @Override
            public Predicate where(Predicate where) {
                // isDeletedDisplay has already been rewritten to isDeleted at the beginning of beforeRead

                if (!applyMainFilter || isDeletedFilter == null) {
                    return where;
                }
                if (where != null) {
                    return CQL.and(where, isDeletedFilter);
                }
                return isDeletedFilter;
            }

            @Override
            public List<CqnSelectListItem> items(List<CqnSelectListItem> items) {
                return items.stream()
                    .map(item -> ExpandFilterBuilder.addFilterToExpandItem(item, model, entity, finalExpandIsDeletedValue))
                    .collect(Collectors.toList());
            }
        });

        context.setCqn(modifiedSelect);
    }

    /**
     * Rewrites isDeletedDisplay references in a CqnSelect.
     * Handles WHERE clause and ref segment filters (for navigation paths).
     * ISSUE-009: Support isDeletedDisplay filter conditions
     */
    private CqnSelect rewriteIsDeletedDisplayInSelect(CqnSelect select) {
        // Use CQL.copy with Modifier to rewrite isDeletedDisplay references in WHERE clause
        // The $filter parameter from navigation paths is converted to WHERE clause by CAP runtime
        return CQL.copy(select, new Modifier() {
            @Override
            public Predicate where(Predicate where) {
                Predicate rewritten = rewriteIsDeletedDisplayReferences(where);
                if (rewritten != where) {
                    logger.debug("Rewrote isDeletedDisplay filter to isDeleted in WHERE clause");
                }
                return rewritten;
            }
        });
    }

    /**
     * Rewrites isDeletedDisplay references in predicates to isDeleted.
     * This allows users to filter by isDeletedDisplay while the plugin uses the internal isDeleted field.
     * ISSUE-009: Support isDeletedDisplay filter conditions
     */
    private Predicate rewriteIsDeletedDisplayReferences(Predicate predicate) {
        if (predicate == null) {
            return null;
        }

        return CQL.copy(predicate, new Modifier() {
            @Override
            public CqnStructuredTypeRef ref(CqnStructuredTypeRef ref) {
                String lastSegment = ref.lastSegment();
                if ("isDeletedDisplay".equals(lastSegment)) {
                    // Replace isDeletedDisplay with isDeleted
                    return (CqnStructuredTypeRef) CQL.get(FIELD_IS_DELETED);
                }
                return ref;
            }

            @Override
            public CqnValue ref(CqnElementRef ref) {
                String lastSegment = ref.lastSegment();
                if ("isDeletedDisplay".equals(lastSegment)) {
                    // Replace isDeletedDisplay with isDeleted
                    return CQL.get(FIELD_IS_DELETED);
                }
                return ref;
            }
        });
    }

    /**
     * Prepares deletion metadata with current timestamp and user.
     */
    private Map<String, Object> prepareDeletionData(String userName) {
        Instant now = Instant.now();
        if (userName == null || userName.isEmpty()) {
            userName = "system";
        }

        Map<String, Object> deletionData = new HashMap<>();
        deletionData.put(FIELD_IS_DELETED, true);
        deletionData.put(FIELD_DELETED_AT, now);
        deletionData.put(FIELD_DELETED_BY, userName);
        return deletionData;
    }
}
