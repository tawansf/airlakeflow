"""Demo model: task (silver)."""
from airlakeflow.models import Model, Field, layer


@layer("silver")
class TaskModel(Model):
    __table__ = "task"

    id = Field.serial(primary_key=True)
    user_id = Field.int(nullable=False)
    title = Field.varchar(255, nullable=False)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
