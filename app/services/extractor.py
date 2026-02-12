import logging
import re
from typing import Any, Dict, List

from app.schemas import MetricItem
from app.services.llm_client import call_llm, parse_llm_json
from app.services.prompt_templates import EXTRACTION_USER_PROMPT
from app.utils.text import normalize_whitespace

logger = logging.getLogger(__name__)


COUNTRY_EN_MAP: Dict[str, str] = {
    "中国": "China",
    "美国": "United States",
    "英国": "United Kingdom",
    "法国": "France",
    "德国": "Germany",
    "日本": "Japan",
    "韩国": "South Korea",
    "印度": "India",
    "巴西": "Brazil",
    "南非": "South Africa",
    "全球": "Global",
}

SEVERITY_EN_MAP: Dict[str, str] = {
    "轻微": "mild",
    "中等": "moderate",
    "严重": "severe",
    "暴发": "outbreak",
    "扩散": "spreading",
    "高致病性": "high pathogenicity",
    "低致病性": "low pathogenicity",
}

TRANSMISSION_PHRASE_MAP: Dict[str, str] = {
    "蚊媒传播": "mosquito-borne transmission",
    "接触传播": "contact transmission",
    "空气传播": "airborne transmission",
    "飞沫传播": "droplet transmission",
}


def _format_date_match(match: re.Match[str]) -> str:
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    return f"{year:04d}-{month:02d}-{day:02d}"


def _translate_time(value: str) -> str:
    translated = value.strip()
    translated = translated.replace("至", " to ").replace("到", " to ")
    translated = re.sub(r"\s+", " ", translated)
    translated = re.sub(r"(\d{4})年(\d{1,2})月(\d{1,2})日", _format_date_match, translated)
    translated = re.sub(r"(\d{4})年(\d{1,2})月", r"\1-\2", translated)
    translated = re.sub(r"(\d{4})年", r"\1", translated)
    translated = translated.replace("月", "").replace("日", "")
    translated = re.sub(r"\s+", " ", translated).strip()
    return translated


def _translate_transmission(value: str) -> str:
    translated = value
    for zh, en in TRANSMISSION_PHRASE_MAP.items():
        translated = translated.replace(zh, en)

    translated = translated.replace("接触", "contact")
    translated = translated.replace("暴露", "exposure")
    translated = translated.replace("传播模式", "transmission pattern")
    translated = translated.replace("传播", "transmission")
    translated = translated.replace("感染", "infection")
    translated = translated.replace("途径", "route")
    return translated.strip()


def _translate_virus_name(value: str) -> str:
    translated = value
    translated = translated.replace("基孔肯雅热", "Chikungunya")
    translated = translated.replace("登革热", "Dengue")
    translated = translated.replace("新冠", "COVID-19")
    translated = translated.replace("流感", "Influenza")
    translated = translated.replace("病毒", "virus")
    return translated.strip()


def _translate_heuristic_value(field: str, value: str) -> str:
    if not value:
        return value
    if field == "country":
        return COUNTRY_EN_MAP.get(value, value)
    if field == "severity":
        return SEVERITY_EN_MAP.get(value, value)
    if field == "time":
        return _translate_time(value)
    if field == "transmission_process":
        return _translate_transmission(value)
    if field == "virus_name":
        return _translate_virus_name(value)
    return value


def _heuristic_extract(text: str) -> List[MetricItem]:
    records: List[MetricItem] = []
    patterns = {
        "virus_name": r"(?:病毒|流感|新冠|登革热|基孔肯雅热|RSV|H5N1|H1N1|SARS-CoV-2)[^\s，。；,;]{0,10}",
        "strain": r"(?:clade|分支)\s*\d+(?:\.\d+)*|[A-Z]{1,3}\.\d+(?:\.\d+)*",
        "subtype": r"\bD\d+(?:\.\d+)?\b|\bB\d+(?:\.\d+)?\b",
        "country": r"(?:中国|美国|英国|法国|德国|日本|韩国|印度|巴西|南非|全球)",
        "location": r"[A-Za-z\u4e00-\u9fff]{1,20}(?:州|省|市|地区|县|乡|镇)",
        "transmission_process": r"(?:接触|暴露|传播|感染|途径|传播模式)[^\s，。；,;]{0,16}",
        "proportion": r"\b\d+(?:\.\d+)?\s*%",
        "infection_count": r"\b\d{1,3}(?:,\d{3})+\b|\b\d+\b",
        "time": r"\b20\d{2}(?:[-/]\d{1,2})?(?:[-/]\d{1,2})?\b|20\d{2}年(?:\d{1,2}月(?:\d{1,2}日)?)?",
        "severity": r"(?:轻微|中等|严重|暴发|扩散|高致病性|低致病性)",
    }

    matches: Dict[str, str] = {}
    for key, pattern in patterns.items():
        hit = re.search(pattern, text)
        if hit:
            matches[key] = _translate_heuristic_value(key, hit.group(0))

    if matches:
        records.append(MetricItem(**matches, evidence="; ".join(v for v in matches.values() if v)))
    return records


def extract_metrics(content: str) -> Dict[str, Any]:
    prompt = EXTRACTION_USER_PROMPT.format(content=content)
    llm_text = call_llm(prompt)
    data = parse_llm_json(llm_text)

    records: List[MetricItem] = []
    for item in data.get("records", []):
        try:
            records.append(MetricItem(**item))
        except Exception:
            logger.warning("Invalid record skipped: %s", item)

    if not records:
        heuristic = _heuristic_extract(normalize_whitespace(content))
        records.extend(heuristic)

    return {
        "records": [r.dict() for r in records],
        "raw_summary": data.get("raw_summary"),
    }
