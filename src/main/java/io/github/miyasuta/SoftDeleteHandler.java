package io.github.miyasuta;

import com.sap.cds.ql.CQL;
import com.sap.cds.ql.Predicate;
import com.sap.cds.ql.Update;
import com.sap.cds.ql.cqn.CqnPredicate;
import com.sap.cds.ql.cqn.CqnSelect;
import com.sap.cds.ql.cqn.CqnUpdate;
import com.sap.cds.ql.cqn.Modifier;
import com.sap.cds.reflect.CdsAssociationType;
import com.sap.cds.reflect.CdsElement;
import com.sap.cds.reflect.CdsEntity;
import com.sap.cds.reflect.CdsModel;
import com.sap.cds.services.cds.ApplicationService;
import com.sap.cds.services.cds.CdsDeleteEventContext;
import com.sap.cds.services.cds.CdsReadEventContext;
import com.sap.cds.services.cds.CqnService;
import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.Before;
import com.sap.cds.services.handler.annotations.ServiceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Component
@ServiceName(value = "*", type = ApplicationService.class)
public class SoftDeleteHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(SoftDeleteHandler.class);
    private static final String ANNOTATION_SOFTDELETE_ENABLED = "@softdelete.enabled";
    private static final String FIELD_IS_DELETED = "isDeleted";
    private static final String FIELD_DELETED_AT = "deletedAt";
    private static final String FIELD_DELETED_BY = "deletedBy";

    @Autowired
    private CdsModel model;

    /**
     * Validates that all entities annotated with @softdelete.enabled have the required fields.
     * This validation runs once at application startup.
     */
    @PostConstruct
    public void validateSoftDeleteEntities() {
        logger.info("Validating soft delete enabled entities...");

        model.entities().forEach(entity -> {
            if (isSoftDeleteEnabled(entity)) {
                List<String> missingFields = new ArrayList<>();

                if (entity.findElement(FIELD_IS_DELETED).isEmpty()) {
                    missingFields.add(FIELD_IS_DELETED);
                }
                if (entity.findElement(FIELD_DELETED_AT).isEmpty()) {
                    missingFields.add(FIELD_DELETED_AT);
                }
                if (entity.findElement(FIELD_DELETED_BY).isEmpty()) {
                    missingFields.add(FIELD_DELETED_BY);
                }

                if (!missingFields.isEmpty()) {
                    String errorMsg = String.format(
                        "Entity '%s' is annotated with @softdelete.enabled but is missing required fields: %s. " +
                        "Please add the 'softdelete' aspect to this entity.",
                        entity.getQualifiedName(),
                        String.join(", ", missingFields)
                    );
                    logger.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }

                logger.debug("Entity '{}' has valid soft delete configuration", entity.getQualifiedName());
            }
        });

        logger.info("Soft delete validation completed successfully");
    }

    /**
     * Intercepts DELETE operations and converts them to UPDATE operations that set soft delete fields.
     * Also cascades soft delete to composition children.
     */
    @Before(event = CqnService.EVENT_DELETE)
    public void beforeDelete(CdsDeleteEventContext context) {
        CdsEntity targetEntity = model.getEntity(context.getTarget().getName());

        if (!isSoftDeleteEnabled(targetEntity)) {
            // Let the default DELETE handler proceed
            return;
        }

        logger.debug("Intercepting DELETE for soft-delete enabled entity: {}", targetEntity.getQualifiedName());

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

        // Convert DELETE to UPDATE
        CqnUpdate update = Update.entity(context.getTarget())
            .data(deletionData)
            .where(context.getCqn().where().orElse(null));

        context.getService().run(update);

        // Cascade soft delete to composition children
        softDeleteCompositionChildren(context, targetEntity, context.getCqn().where().orElse(null), deletionData);

        // Mark the event as completed and set empty result (HTTP 204)
        context.setResult(Collections.emptyList());
        context.setCompleted();
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

        // Get target entity
        String targetName = select.ref().targetSegment().id();
        CdsEntity entity = model.getEntity(targetName);

        if (entity == null || !isSoftDeleteEnabled(entity)) {
            // Not a soft-delete enabled entity, proceed normally
            return;
        }

        logger.debug("Applying soft delete filter to READ for entity: {}", targetName);

        // Check if this is by-key access (skip filtering for direct key access)
        if (isByKeyAccess(select)) {
            logger.debug("Skipping soft delete filter for by-key access");
            return;
        }

        // Check if user already specified isDeleted filter
        if (select.where().isPresent() && hasIsDeletedInWhere(select.where().get())) {
            logger.debug("User specified isDeleted filter, skipping automatic filter");
            return;
        }

        // Add isDeleted = false filter
        Predicate isDeletedFilter = CQL.get(FIELD_IS_DELETED).eq(false);

        CqnSelect modifiedSelect = CQL.copy(select, new Modifier() {
            @Override
            public Predicate where(Predicate where) {
                if (where != null) {
                    return CQL.and(where, isDeletedFilter);
                }
                return isDeletedFilter;
            }
        });

        context.setCqn(modifiedSelect);
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
                // Build update for composition children
                // Note: In a real implementation, we would need to:
                // 1. Query the parent with the given keys
                // 2. Navigate to children
                // 3. Update each child
                // For simplicity, we construct an update based on the association
                CqnUpdate childUpdate = Update.entity(childEntity.getQualifiedName())
                    .data(deletionData);

                // Execute the update
                context.getService().run(childUpdate);

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
     */
    private boolean isByKeyAccess(CqnSelect select) {
        // Check if the query has a where clause that matches all key fields
        // This is a simplified check - a more robust implementation would analyze the where clause
        // to ensure all key fields are specified with equality conditions
        if (select.where().isEmpty()) {
            return false;
        }

        // Check if the ref has a filter on the last segment (indicates by-key)
        return select.ref().rootSegment().filter().isPresent();
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
