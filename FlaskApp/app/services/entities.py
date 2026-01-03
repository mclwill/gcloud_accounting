from sqlalchemy.exc import IntegrityError
from FlaskApp.app.accounting_db import db
from FlaskApp.app.models.entity import Entity

def get_entities():
    return (
        db.session.query(Entity)
        .order_by(Entity.id)
        .all()
    )

def create_entity(name: str, type_: str | None = None, description: str | None = None) -> Entity:
    """Create a new Entity row and commit.

    Raises:
        IntegrityError: if (name,type) violates unique constraint
        ValueError: if name is blank
    """
    name = (name or "").strip()
    if not name:
        raise ValueError("Entity name is required")

    ent = Entity(name=name, type=(type_ or None), description=(description or None))
    db.session.add(ent)
    db.session.commit()
    return ent
