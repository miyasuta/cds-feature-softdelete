namespace io.github.miyasuta;

aspect softdelete {
    isDeleted: Boolean default false @UI.Hidden;
    deletedAt: Timestamp @UI.Hidden;
    deletedBy: String @UI.Hidden;
    // For display purposes (type explicitly specified to reset annotation inheritance)
    isDeletedDisplay: Boolean = isDeleted;
    deletedAtDisplay: Timestamp = deletedAt;
    deletedByDisplay: String = deletedBy;
}