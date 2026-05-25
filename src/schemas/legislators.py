from pydantic import BaseModel, Field


class LegislatorSchema(BaseModel):
    # Mandatory columns
    bioguideId: str = Field(min_length=3)
    state: str = Field(min_length=1)

    # Optional columns
    name: str | None = None
    partyName: str | None = None
