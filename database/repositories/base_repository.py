# database/repositories/base_repository.py
"""
Base repository pattern implementation.
Provides common CRUD operations that can be extended by specific repositories.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from sqlalchemy.exc import IntegrityError, InvalidRequestError, SQLAlchemyError

from database.base import Base
from database.core.database_manager import DatabaseManager
from exceptions import DatabaseOperationError

T = TypeVar("T", bound=Base)


class BaseRepository(ABC, Generic[T]):
    """
    Abstract base repository providing common database operations.
    """

    def __init__(self, db_manager: DatabaseManager, model_class: Type[T]):
        """
        Initialize repository with database manager and model class.

        Args:
            db_manager: Database manager instance
            model_class: SQLAlchemy model class for this repository

        Raises:
            ValueError: If db_manager or model_class is invalid
        """
        if not isinstance(db_manager, DatabaseManager):
            raise ValueError("db_manager must be a DatabaseManager instance")

        if not issubclass(model_class, Base):
            raise ValueError("model_class must be a SQLAlchemy model")

        self.db_manager = db_manager
        self.model_class = model_class

    def create(self, entity_data: Dict[str, Any]) -> T:
        """
        Create a new entity record.

        Args:
            entity_data: Entity data dictionary

        Returns:
            Created entity object

        Raises:
            ValueError: If entity_data is invalid
            DatabaseOperationError: For database errors
        """
        if not entity_data:
            raise ValueError("Entity data cannot be empty")

        try:
            with self.db_manager.get_session() as session:
                entity = self.model_class.from_dict(entity_data)
                session.add(entity)
                session.flush()
                session.refresh(entity)
                return entity
        except IntegrityError as error:
            raise DatabaseOperationError(
                f"Entity with provided keys already exists: {error}"
            ) from error
        except InvalidRequestError as error:
            raise ValueError(f"Invalid entity data: {error}")
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error creating entity: {error}")

    @abstractmethod
    def get_by_id(self, entity_id: str) -> Optional[T]:
        """
        Retrieve entity by ID. Must be implemented by subclasses.
        """
        pass

    def get_all(self, active_only: bool = True) -> List[T]:
        """
        Retrieve all entities.

        Args:
            active_only: Whether to return only active entities

        Returns:
            List of entity objects

        Raises:
            DatabaseOperationError: For database errors
        """
        try:
            with self.db_manager.get_session() as session:
                query = session.query(self.model_class)
                if active_only and hasattr(self.model_class, "is_active"):
                    query = query.filter(self.model_class.is_active)

                entities = query.all()
                return entities
        except SQLAlchemyError as error:
            raise DatabaseOperationError(f"Database error retrieving entities: {error}")

    @abstractmethod
    def update(self, entity_id: str, update_data: Dict[str, Any]) -> Optional[T]:
        """
        Update entity record. Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def delete(self, entity_id: str, soft_delete: bool = True) -> bool:
        """
        Delete entity record. Must be implemented by subclasses.
        """
        pass
