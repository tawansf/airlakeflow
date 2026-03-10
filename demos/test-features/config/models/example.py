"""Example model (silver). Edit or create new ones with 'alf new model NAME'."""

from airlakeflow.models import Model, Field, layer


@layer("silver")
class ExampleModel(Model):
    __table__ = "example"

    id = Field.serial(primary_key=True)
    name = Field.varchar(255, nullable=False)
    created_at = Field.timestamp(default="CURRENT_TIMESTAMP")
    updated_at = Field.timestamp(nullable=False)
