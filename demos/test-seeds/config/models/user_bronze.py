"""Demo model: user (bronze) - raw/seed table."""
from airlakeflow.models import Model, Field, layer


@layer("bronze")
class UserBronzeModel(Model):
    __table__ = "user"

    id = Field.serial(primary_key=True)
    name = Field.varchar(255, nullable=False)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
