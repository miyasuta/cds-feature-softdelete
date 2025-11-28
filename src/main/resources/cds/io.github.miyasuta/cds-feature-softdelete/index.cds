namespace io.github.miyasuta;

aspect softdelete {
    isDeleted: Boolean default false;
    deletedAt: Timestamp;
    deletedBy: String;
}