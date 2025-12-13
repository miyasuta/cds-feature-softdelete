# cds-feature-softdelete

A plugin for implementing soft delete functionality in SAP Cloud Application Programming Model (CAP) Java applications.

## What It Does

This plugin automatically converts DELETE operations to soft deletes (marking records as deleted instead of removing them) and filters out soft-deleted records from READ operations. When a parent entity is deleted, its composition children are automatically soft-deleted as well.

## Installation

Add the plugin as a dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.miyasuta</groupId>
    <artifactId>cds-feature-softdelete</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

### 1. Add the `softdelete` aspect to your entities

```cds
// db/schema.cds
using { io.github.miyasuta.softdelete } from 'cds-feature-softdelete';

entity Orders: softdelete {
  key ID    : UUID;
      total : Decimal(9,2);
      items : Composition of many OrderItems on items.order = $self;
}

entity OrderItems: softdelete {
  key ID       : UUID;
      order    : Association to Orders;
      quantity : Integer;
}
```

The `softdelete` aspect adds these fields:
- `isDeleted` (Boolean) - Deletion flag
- `deletedAt` (Timestamp) - Deletion timestamp
- `deletedBy` (String) - User who deleted the record
- `isDeletedDisplay` (Boolean) - Computed field for UI display
- `deletedAtDisplay` (Timestamp) - Computed field for UI display
- `deletedByDisplay` (String) - Computed field for UI display

### 2. Enable soft delete in your service

Add `@softdelete.enabled` to every entity that should use soft delete:

```cds
// srv/order-service.cds
service OrderService {
    @softdelete.enabled
    entity Orders as projection on my.Orders;

    @softdelete.enabled
    entity OrderItems as projection on my.OrderItems;
}
```

**Important**: Both parent and child entities need `@softdelete.enabled` for cascade delete to work. The annotation is required because service projections do not automatically inherit aspect behavior.

### 3. Use display fields in UI

For UI annotations, use the `*Display` computed fields instead of the internal fields:

```cds
annotate OrderService.Orders with @(
  UI.LineItem : [
      ...
      // DO: Use display fields (read-only, safe for UI)
      {  Value: isDeletedDisplay },
      {  Value: deletedAtDisplay },
      {  Value: deletedByDisplay },

      // DON'T: Use internal fields (not protected, users can modify them)
      {  Value: isDeleted },
      {  Value: deletedAt },
      {  Value: deletedBy },
      ...
  ],
)
```

**Why?** Due to CAP Java draft activation constraints, the internal fields (`isDeleted`, `deletedAt`, `deletedBy`) cannot be annotated with `@readonly`. The display fields are computed and safe to show in the UI without allowing user modifications.

## How It Works

### DELETE Operations

Delete operations are converted to updates that set `isDeleted=true`:

```javascript
DELETE /Orders(123)
// Sets: isDeleted=true, deletedAt=<timestamp>, deletedBy=<user>
```

Composition children are automatically soft-deleted when the parent is deleted.

### READ Operations

Soft-deleted records are automatically excluded from list queries:

```javascript
GET /Orders
// Only returns records where isDeleted=false
```

### Retrieving Deleted Data

Use `isDeleted=true` filter to access soft-deleted records:

```javascript
GET /Orders?$filter=isDeleted eq true
```

## Draft Support

Soft delete works in draft mode (Fiori Elements Object Page). Note that in draft edit mode, deleted items may remain visible until the draft is activated. This is expected behavior to ensure proper synchronization.

## Limitations

- **Draft Edit Mode**: Deleted items remain visible in draft edit mode until the draft is activated
- **Field Protection**: Cannot use `@readonly` on soft delete fields due to CAP Java draft activation constraints (use `*Display` fields for UI instead)

## License

[MIT](LICENSE)

## Links

- [SAP Cloud Application Programming Model](https://cap.cloud.sap/)
- [CAP Java Plugin Documentation](https://cap.cloud.sap/docs/java/building-plugins)
- [Node.js version](https://github.com/miyasuta/cds-softdelete-plugin)
