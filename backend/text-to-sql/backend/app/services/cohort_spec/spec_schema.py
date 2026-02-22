from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class EvidenceRef(BaseModel):
    model_config = ConfigDict(extra="allow")

    page: int | None = None
    quote: str | None = None
    section: str | None = None
    span: list[int] | None = None


class Condition(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    type: str
    evidence_refs: list[EvidenceRef] = Field(default_factory=list)


class PopulationSpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    require_icu: bool = False
    index_event: str | None = None
    episode_unit: str | None = None
    episode_selector: str | None = None


class CohortSpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    metadata: dict[str, Any] = Field(default_factory=dict)
    population: PopulationSpec = Field(default_factory=PopulationSpec)
    inclusion: list[Condition] = Field(default_factory=list)
    exclusion: list[Condition] = Field(default_factory=list)
    requirements: list[Condition] = Field(default_factory=list)
    ambiguities: list[dict[str, Any]] = Field(default_factory=list)


def validate_cohort_spec(spec: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    try:
        model = CohortSpec.model_validate(spec or {})
        return model.model_dump(), []
    except ValidationError as exc:
        return spec or {}, [str(err.get("msg") or "validation_error") for err in exc.errors()]
