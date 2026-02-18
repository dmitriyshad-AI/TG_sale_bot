from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, ValidationError, field_validator, model_validator

from sales_agent.sales_core.config import get_settings


class CatalogValidationError(ValueError):
    """Raised when catalog data does not match the expected schema."""


@dataclass(frozen=True)
class SearchCriteria:
    brand: Optional[str] = None
    grade: Optional[int] = None
    goal: Optional[str] = None
    subject: Optional[str] = None
    format: Optional[str] = None


class ProductSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=2, max_length=120)
    start_date: date
    end_date: Optional[date] = None
    price_rub: Optional[int] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_dates(self) -> "ProductSession":
        if self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return self


class Product(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,63}$")
    brand: Literal["kmipt", "foton"]
    title: str = Field(min_length=5, max_length=180)
    url: HttpUrl
    category: Literal["camp", "ege", "oge", "olympiad", "base", "intensive"]
    grade_min: int = Field(ge=1, le=11)
    grade_max: int = Field(ge=1, le=11)
    subjects: List[str] = Field(min_length=1)
    format: Literal["online", "offline", "hybrid"]
    sessions: List[ProductSession] = Field(default_factory=list)
    usp: List[str] = Field(min_length=3, max_length=7)

    @field_validator("subjects")
    @classmethod
    def validate_subjects(cls, value: List[str]) -> List[str]:
        normalized = [item.strip().lower() for item in value if item and item.strip()]
        if not normalized:
            raise ValueError("subjects must contain at least one non-empty value")
        deduplicated = list(dict.fromkeys(normalized))
        return deduplicated

    @field_validator("usp")
    @classmethod
    def validate_usp(cls, value: List[str]) -> List[str]:
        cleaned = [item.strip() for item in value if item and item.strip()]
        if len(cleaned) < 3:
            raise ValueError("usp must contain at least 3 non-empty bullets")
        if len(cleaned) > 7:
            raise ValueError("usp must contain up to 7 bullets")
        return cleaned

    @model_validator(mode="after")
    def validate_grade_range(self) -> "Product":
        if self.grade_min > self.grade_max:
            raise ValueError("grade_min must be <= grade_max")
        return self


class Catalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    products: List[Product] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_uniques(self) -> "Catalog":
        seen: Set[str] = set()
        duplicates: Set[str] = set()
        for product in self.products:
            if product.id in seen:
                duplicates.add(product.id)
            seen.add(product.id)
        duplicates_sorted = sorted(duplicates)
        if duplicates_sorted:
            raise ValueError(f"duplicate product ids found: {', '.join(duplicates_sorted)}")
        return self


def project_root() -> Path:
    # /project_root/sales_agent/sales_core/catalog.py -> project_root
    return Path(__file__).resolve().parent.parent.parent


def default_catalog_path() -> Path:
    return project_root() / "catalog" / "products.yaml"


def _format_validation_error(error: ValidationError, source: Path) -> str:
    lines = [f"Catalog validation failed for {source}:"]
    for item in error.errors():
        location = ".".join(str(part) for part in item.get("loc", ()))
        message = item.get("msg", "validation error")
        lines.append(f"- {location}: {message}")
    return "\n".join(lines)


def parse_catalog(raw_data: Dict[str, Any], source: Path) -> Catalog:
    try:
        return Catalog.model_validate(raw_data)
    except ValidationError as exc:
        raise CatalogValidationError(_format_validation_error(exc, source)) from exc


def load_catalog(path: Optional[Path] = None) -> Catalog:
    catalog_path = path or default_catalog_path()
    with catalog_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise CatalogValidationError(
            f"Catalog at {catalog_path} must be a mapping with top-level key 'products'."
        )
    return parse_catalog(data, catalog_path)


def load_products(path: Optional[Path] = None) -> List[Product]:
    return load_catalog(path).products


GOAL_ALIASES = {
    "ege": "ege",
    "егэ": "ege",
    "oge": "oge",
    "огэ": "oge",
    "olympiad": "olympiad",
    "олимп": "olympiad",
    "олимпиада": "olympiad",
    "camp": "camp",
    "лагерь": "camp",
    "base": "base",
    "база": "base",
    "успеваемость": "base",
    "intensive": "intensive",
    "интенсив": "intensive",
}

SUBJECT_ALIASES = {
    "math": "math",
    "математика": "math",
    "physics": "physics",
    "физика": "physics",
    "informatics": "informatics",
    "информатика": "informatics",
}

FORMAT_ALIASES = {
    "online": "online",
    "онлайн": "online",
    "offline": "offline",
    "очно": "offline",
    "hybrid": "hybrid",
    "смешанный": "hybrid",
}

BRAND_ALIASES = {
    "kmipt": "kmipt",
    "фотон": "foton",
    "foton": "foton",
}


def normalize_goal(goal: Optional[str]) -> Optional[str]:
    if not goal:
        return None
    return GOAL_ALIASES.get(goal.strip().lower(), goal.strip().lower())


def normalize_subject(subject: Optional[str]) -> Optional[str]:
    if not subject:
        return None
    return SUBJECT_ALIASES.get(subject.strip().lower(), subject.strip().lower())


def normalize_format(format: Optional[str]) -> Optional[str]:
    if not format:
        return None
    return FORMAT_ALIASES.get(format.strip().lower(), format.strip().lower())


def normalize_brand(brand: Optional[str]) -> Optional[str]:
    if not brand:
        return None
    return BRAND_ALIASES.get(brand.strip().lower(), brand.strip().lower())


def _effective_brand(brand: Optional[str], brand_default: Optional[str]) -> str:
    normalized_brand = normalize_brand(brand)
    if normalized_brand:
        return normalized_brand
    default_brand = normalize_brand(brand_default) if brand_default else None
    if default_brand:
        return default_brand
    settings_brand = normalize_brand(get_settings().brand_default)
    return settings_brand or "kmipt"


def _matches_format(product_format: str, requested_format: Optional[str]) -> bool:
    if not requested_format:
        return True
    if requested_format == "hybrid":
        return product_format == "hybrid"
    if product_format == requested_format:
        return True
    if product_format == "hybrid":
        return True
    return False


def filter_products(
    products: List[Product],
    brand: Optional[str] = None,
    grade: Optional[int] = None,
    goal: Optional[str] = None,
    subject: Optional[str] = None,
    format: Optional[str] = None,
    brand_default: Optional[str] = None,
) -> List[Product]:
    normalized_brand = _effective_brand(brand, brand_default)
    normalized_goal = normalize_goal(goal)
    normalized_subject = normalize_subject(subject)
    normalized_format = normalize_format(format)

    filtered: List[Product] = []
    for product in products:
        if product.brand != normalized_brand:
            continue
        if grade is not None and not (product.grade_min <= grade <= product.grade_max):
            continue
        if normalized_goal and product.category != normalized_goal:
            continue
        if normalized_subject and normalized_subject not in product.subjects:
            continue
        if not _matches_format(product.format, normalized_format):
            continue
        filtered.append(product)

    return filtered


def _score_product(
    product: Product,
    criteria: SearchCriteria,
    normalized_brand: str,
    normalized_goal: Optional[str],
    normalized_subject: Optional[str],
    normalized_format: Optional[str],
) -> int:
    score = 0

    if product.brand == normalized_brand:
        score += 10

    if criteria.grade is not None and product.grade_min <= criteria.grade <= product.grade_max:
        score += 35
        distance_to_edge = min(criteria.grade - product.grade_min, product.grade_max - criteria.grade)
        score += max(0, 5 - distance_to_edge)

    if normalized_goal and product.category == normalized_goal:
        score += 30

    if normalized_subject:
        if normalized_subject in product.subjects:
            score += 25
        elif len(product.subjects) > 1:
            score += 5

    if normalized_format:
        if product.format == normalized_format:
            score += 15
        elif product.format == "hybrid" and normalized_format in {"online", "offline"}:
            score += 8

    score += max(0, 6 - (product.grade_max - product.grade_min))
    return score


def rank_products(products: List[Product], criteria: SearchCriteria) -> List[Product]:
    normalized_brand = _effective_brand(criteria.brand, None)
    normalized_goal = normalize_goal(criteria.goal)
    normalized_subject = normalize_subject(criteria.subject)
    normalized_format = normalize_format(criteria.format)

    return sorted(
        products,
        key=lambda item: (
            -_score_product(
                item,
                criteria,
                normalized_brand=normalized_brand,
                normalized_goal=normalized_goal,
                normalized_subject=normalized_subject,
                normalized_format=normalized_format,
            ),
            (item.grade_max - item.grade_min),
            item.id,
        ),
    )


def explain_match(product: Product, criteria: SearchCriteria) -> str:
    reasons: List[str] = []
    goal = normalize_goal(criteria.goal)
    subject = normalize_subject(criteria.subject)
    requested_format = normalize_format(criteria.format)

    if criteria.grade is not None and product.grade_min <= criteria.grade <= product.grade_max:
        reasons.append(f"подходит для {criteria.grade} класса")
    if goal and product.category == goal:
        reasons.append(f"цель совпадает: {goal}")
    if subject and subject in product.subjects:
        reasons.append(f"есть профиль по предмету: {subject}")
    if requested_format and product.format == requested_format:
        reasons.append(f"формат совпадает: {requested_format}")
    elif requested_format and product.format == "hybrid" and requested_format in {"online", "offline"}:
        reasons.append("доступен гибридный формат")

    if not reasons:
        return "Подходит по бренду и возрастной группе."
    return "; ".join(reasons) + "."


def select_top_products(
    criteria: SearchCriteria,
    path: Optional[Path] = None,
    top_k: int = 3,
    brand_default: Optional[str] = None,
) -> List[Product]:
    products = load_products(path)
    filtered = filter_products(
        products=products,
        brand=criteria.brand,
        grade=criteria.grade,
        goal=criteria.goal,
        subject=criteria.subject,
        format=criteria.format,
        brand_default=brand_default,
    )
    ranked = rank_products(filtered, criteria)
    return ranked[:top_k]
