package io.github.miyasuta.util;

import com.sap.cds.reflect.CdsAssociationType;
import com.sap.cds.reflect.CdsElement;
import com.sap.cds.reflect.CdsEntity;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Helper class for entity metadata operations.
 * Provides utility methods for checking entity annotations, extracting keys,
 * and finding relationships.
 */
public class EntityMetadataHelper {

    private static final String ANNOTATION_SOFTDELETE_ENABLED = "@softdelete.enabled";
    private static final String ANNOTATION_DRAFT_ENABLED = "@odata.draft.enabled";
    private static final List<String> DRAFT_VIRTUAL_KEYS = Arrays.asList("IsActiveEntity", "HasActiveEntity", "HasDraftEntity");

    /**
     * Checks if an entity has the @softdelete.enabled annotation.
     */
    public static boolean isSoftDeleteEnabled(CdsEntity entity) {
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
    public static boolean isDraftRootEntity(CdsEntity entity) {
        if (entity == null) {
            return false;
        }
        Boolean draftEnabled = entity.getAnnotationValue(ANNOTATION_DRAFT_ENABLED, Boolean.FALSE);
        return Boolean.TRUE.equals(draftEnabled);
    }

    /**
     * Checks if an entity is a draft-enabled entity (has IsActiveEntity key).
     * In CAP Java, draft entities are identified by the IsActiveEntity virtual key,
     * not by a .drafts suffix like in Node.js.
     */
    public static boolean isDraftEntity(CdsEntity entity) {
        if (entity == null) {
            return false;
        }
        return entity.elements()
            .filter(CdsElement::isKey)
            .anyMatch(element -> "IsActiveEntity".equals(element.getName()));
    }

    /**
     * Gets the underlying database entity name from a service entity.
     */
    public static String getDbEntityName(CdsEntity entity) {
        if (entity.query().isPresent()) {
            String source = entity.query().get().ref().firstSegment();
            if (source != null) {
                return source;
            }
        }
        return entity.getQualifiedName();
    }

    /**
     * Gets the key names of an entity, excluding draft virtual keys.
     */
    public static List<String> getEntityKeyNames(CdsEntity entity) {
        return entity.elements()
            .filter(CdsElement::isKey)
            .map(CdsElement::getName)
            .filter(keyName -> !DRAFT_VIRTUAL_KEYS.contains(keyName))
            .collect(Collectors.toList());
    }

    /**
     * Returns all composition elements of an entity.
     */
    public static List<CdsElement> getCompositionElements(CdsEntity entity) {
        return entity.elements()
            .filter(element -> element.getType().isAssociation())
            .filter(element -> {
                CdsAssociationType assocType = (CdsAssociationType) element.getType();
                return assocType.isComposition();
            })
            .collect(Collectors.toList());
    }

    /**
     * Finds the target entity of an association element.
     */
    public static CdsEntity findTargetEntity(CdsEntity parentEntity, String associationName) {
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
     * Filters keys to only include those that belong to the target entity.
     * This is important for navigation path deletions (e.g., Orders(...)/items(...))
     * where keys contain both parent and child entity keys.
     */
    public static Map<String, Object> filterKeysForEntity(Map<String, Object> keys, CdsEntity entity) {
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
     * Removes draft-specific virtual keys from a map.
     */
    public static void removeDraftKeys(Map<String, Object> keys) {
        DRAFT_VIRTUAL_KEYS.forEach(keys::remove);
    }

    /**
     * Extracts the foreign key name from a composition element.
     * For example, for composition "items" pointing to OrderItems with back-association "order",
     * it extracts "order_ID".
     */
    public static String extractForeignKeyName(CdsElement element, Map<String, Object> parentKeys) {
        CdsAssociationType assocType = (CdsAssociationType) element.getType();
        CdsEntity targetEntity = assocType.getTarget();

        // Find the back-association in the target entity that points to the parent
        String associationName = null;
        for (var targetElement : targetEntity.elements().collect(Collectors.toList())) {
            if (targetElement.getType().isAssociation()) {
                CdsAssociationType targetAssocType = (CdsAssociationType) targetElement.getType();
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
}
