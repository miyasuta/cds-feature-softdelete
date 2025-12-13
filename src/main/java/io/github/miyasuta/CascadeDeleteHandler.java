package io.github.miyasuta;

import com.sap.cds.Result;
import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Select;
import com.sap.cds.ql.Update;
import com.sap.cds.ql.cqn.CqnSelect;
import com.sap.cds.ql.cqn.CqnUpdate;
import com.sap.cds.reflect.CdsAssociationType;
import com.sap.cds.reflect.CdsElement;
import com.sap.cds.reflect.CdsEntity;
import com.sap.cds.services.cds.CdsDeleteEventContext;
import com.sap.cds.services.draft.DraftCancelEventContext;
import com.sap.cds.services.persistence.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Handles cascading soft delete operations for composition children.
 */
class CascadeDeleteHandler {

    private static final Logger logger = LoggerFactory.getLogger(CascadeDeleteHandler.class);
    private static final String FIELD_IS_DELETED = "isDeleted";
    private static final String FIELD_DELETED_AT = "deletedAt";
    private static final String FIELD_DELETED_BY = "deletedBy";

    /**
     * Recursively soft deletes composition children of a given entity.
     */
    static void softDeleteCompositionChildren(CdsDeleteEventContext context, CdsEntity entity,
                                              Map<String, Object> parentKeys, Map<String, Object> deletionData) {
        List<CdsElement> compositionElements = EntityMetadataHelper.getCompositionElements(entity);
        PersistenceService db = context.getServiceCatalog().getService(PersistenceService.class, PersistenceService.DEFAULT_NAME);

        for (CdsElement element : compositionElements) {
            CdsAssociationType assocType = (CdsAssociationType) element.getType();
            CdsEntity childEntity = assocType.getTarget();

            if (!EntityMetadataHelper.isSoftDeleteEnabled(childEntity)) {
                continue;
            }

            try {
                String childDbEntityName = EntityMetadataHelper.getDbEntityName(childEntity);

                // Extract foreign key name from the composition's on clause
                String foreignKeyName = EntityMetadataHelper.extractForeignKeyName(element, parentKeys);
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
                CqnUpdate childUpdate = Update.entity(childDbEntityName)
                    .data(deletionData)
                    .where(b -> b.get(foreignKeyName).eq(parentKeyValue)
                              .and(b.get(FIELD_IS_DELETED).eq(false)));

                db.run(childUpdate);

                // Recursively handle nested compositions for each child
                for (var child : childrenResult) {
                    // Extract child's keys (excluding draft virtual keys)
                    Map<String, Object> childKeys = new HashMap<>();
                    for (String keyName : EntityMetadataHelper.getEntityKeyNames(childEntity)) {
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
     * Recursively soft deletes composition children of a draft entity using PersistenceService.
     */
    static void softDeleteDraftCompositionChildren(DraftCancelEventContext context, PersistenceService db,
                                                   CdsEntity entity, Map<String, Object> parentKeys,
                                                   Map<String, Object> deletionData) {
        List<CdsElement> compositionElements = EntityMetadataHelper.getCompositionElements(entity);

        for (CdsElement element : compositionElements) {
            CdsAssociationType assocType = (CdsAssociationType) element.getType();
            CdsEntity childEntity = assocType.getTarget();

            // Check if the child entity is soft-delete enabled
            if (!EntityMetadataHelper.isSoftDeleteEnabled(childEntity)) {
                continue;
            }

            try {
                String childDraftTableName = childEntity.getQualifiedName() + "_drafts";

                // Extract foreign key name from the composition's on clause
                String foreignKeyName = EntityMetadataHelper.extractForeignKeyName(element, parentKeys);
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
                    Object isDeleted = child.get(FIELD_IS_DELETED);
                    if (isDeleted != null && (Boolean) isDeleted) {
                        continue;
                    }

                    // Extract child's keys
                    Map<String, Object> childKeys = new HashMap<>();
                    for (String keyName : EntityMetadataHelper.getEntityKeyNames(childEntity)) {
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

    /**
     * Checks if an active entity exists for the given draft entity.
     * Used to determine whether a draft child deletion should be physical (new record) or soft (existing record).
     */
    static boolean checkActiveEntityExists(DraftCancelEventContext context, CdsEntity entity, Map<String, Object> keys) {
        try {
            // Get the database entity name
            String dbEntityName = EntityMetadataHelper.getDbEntityName(entity);

            // Query for active entity (without _drafts suffix, using entity keys only)
            CqnSelect activeQuery = Select.from(dbEntityName)
                .columns(CQL.get("ID"))
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
}
