from pydantic import BaseModel, ConfigDict, Field
from typing import Optional

class EtlTriggerEvent(BaseModel):
    eventType: str
    jobId: str
    completedAt: str
    recomputeDays: Optional[int] = 1

    model_config = ConfigDict(extra='ignore')

class ManualTriggerRequest(BaseModel):
    runId: str = Field(min_length=1)

    model_config = ConfigDict(extra='ignore')

class UtmAggregateCompletedEvent(BaseModel):
    eventType: str
    brand: str
    jobId: str
    completedAt: str
    status: str

    model_config = ConfigDict(extra='ignore')

class UtmAggregateFailedEvent(BaseModel):
    eventType: str
    brand: str
    jobId: str
    completedAt: str
    status: str
    error: str

    model_config = ConfigDict(extra='ignore')
