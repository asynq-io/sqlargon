class RepositoryError(Exception):
    pass


class EntityNotFoundError(RepositoryError):
    pass


class DuplicateEntityError(RepositoryError):
    pass


class RelatedEntityDoesNotExistError(RepositoryError):
    pass
