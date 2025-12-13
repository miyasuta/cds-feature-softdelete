namespace io.github.miyasuta;

aspect softdelete {
    isDeleted: Boolean default false @UI.Hidden;
    deletedAt: Timestamp @UI.Hidden;
    deletedBy: String @UI.Hidden;
    // For display purposes
    isDeletedDisplay = isDeleted;
    deletedAtDisplay = deletedAt;
    deletedByDisplay = deletedBy;
}