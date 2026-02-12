from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MetricItem(BaseModel):
    virus_name: Optional[str] = Field(default=None, description="病毒名称")
    strain: Optional[str] = Field(default=None, description="毒株/变种/分支")
    subtype: Optional[str] = Field(default=None, description="亚型/基因型")
    country: Optional[str] = Field(default=None, description="国家或地区")
    location: Optional[str] = Field(default=None, description="具体地点")
    time: Optional[str] = Field(default=None, description="时间或时间范围")
    transmission_process: Optional[str] = Field(default=None, description="传播/暴露途径或流程")
    proportion: Optional[str] = Field(default=None, description="占比/比例")
    infection_rate: Optional[str] = Field(default=None, description="感染率")
    infection_count: Optional[str] = Field(default=None, description="感染人数")
    severity: Optional[str] = Field(default=None, description="疫情严重程度")
    extra_fields: Dict[str, Any] = Field(default_factory=dict)
    evidence: Optional[str] = Field(default=None, description="原文证据或摘要")


class ExtractionResult(BaseModel):
    file_name: str
    file_type: str
    records: List[MetricItem]
    raw_summary: Optional[str] = None
    model_provider: str
    model_name: str
