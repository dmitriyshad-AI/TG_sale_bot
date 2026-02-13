from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sales_agent.sales_core.catalog import Product, SearchCriteria


@dataclass
class SalesReply:
    answer_text: str
    next_question: Optional[str]
    call_to_action: str
    recommended_product_ids: List[str]
    used_fallback: bool
    error: Optional[str] = None


class LLMClient:
    def __init__(
        self,
        api_key: str,
        model: str,
        endpoint: str = "https://api.openai.com/v1/responses",
        timeout_seconds: float = 25.0,
    ) -> None:
        self.api_key = api_key.strip()
        self.model = model.strip() or "gpt-4.1"
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def build_sales_reply(self, criteria: SearchCriteria, top_products: List[Product]) -> SalesReply:
        if not top_products:
            return SalesReply(
                answer_text="Пока не вижу подходящих программ по этим параметрам.",
                next_question="Уточните, пожалуйста, класс или предмет.",
                call_to_action="Оставьте телефон, и менеджер подберет варианты вручную.",
                recommended_product_ids=[],
                used_fallback=True,
            )

        if not self.is_configured():
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = "OPENAI_API_KEY is not configured"
            return fallback

        payload = self._build_payload(criteria, top_products)
        request = Request(
            self.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
                raw = json.loads(body) if body else {}
        except HTTPError as exc:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = f"OpenAI HTTP error: {exc.code}"
            return fallback
        except URLError as exc:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = f"OpenAI connection error: {exc.reason}"
            return fallback
        except json.JSONDecodeError:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = "OpenAI response is not valid JSON"
            return fallback

        parsed = self._parse_openai_reply(raw, allowed_ids=[product.id for product in top_products])
        if parsed is None:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = "Could not parse structured LLM response"
            return fallback

        return parsed

    def _build_payload(self, criteria: SearchCriteria, top_products: List[Product]) -> Dict[str, Any]:
        criteria_payload = {
            "brand": criteria.brand,
            "grade": criteria.grade,
            "goal": criteria.goal,
            "subject": criteria.subject,
            "format": criteria.format,
        }
        products_payload = [self._product_payload(product) for product in top_products]

        system_prompt = (
            "Ты sales-ассистент для образовательных программ. "
            "Используй только факты из переданного каталога. "
            "Не выдумывай цены, даты, условия и ссылки. "
            "Если данных не хватает, проси уточнение и предлагай оставить контакт. "
            "Ответ обязателен строго в JSON с ключами: "
            "answer_text, next_question, call_to_action, recommended_product_ids."
        )

        user_prompt = (
            "Критерии клиента:\n"
            f"{json.dumps(criteria_payload, ensure_ascii=False)}\n\n"
            "Доступные продукты (использовать только их):\n"
            f"{json.dumps(products_payload, ensure_ascii=False)}\n\n"
            "Сформируй продающий, но точный ответ. "
            "recommended_product_ids должен содержать только id из списка продуктов."
        )

        return {
            "model": self.model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_prompt}],
                },
            ],
            "temperature": 0.2,
            "max_output_tokens": 600,
        }

    def _product_payload(self, product: Product) -> Dict[str, Any]:
        sessions = []
        for session in product.sessions:
            sessions.append(
                {
                    "name": session.name,
                    "start_date": str(session.start_date),
                    "end_date": str(session.end_date) if session.end_date else None,
                    "price_rub": session.price_rub,
                }
            )

        return {
            "id": product.id,
            "title": product.title,
            "url": str(product.url),
            "category": product.category,
            "grade_min": product.grade_min,
            "grade_max": product.grade_max,
            "subjects": product.subjects,
            "format": product.format,
            "usp": product.usp,
            "sessions": sessions,
        }

    def _extract_text(self, response: Dict[str, Any]) -> str:
        output_text = response.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()

        output = response.get("output")
        if not isinstance(output, list):
            return ""

        chunks: List[str] = []
        for item in output:
            content = item.get("content") if isinstance(item, dict) else None
            if not isinstance(content, list):
                continue
            for piece in content:
                if not isinstance(piece, dict):
                    continue
                text_value = piece.get("text")
                if isinstance(text_value, str):
                    chunks.append(text_value)
        return "\n".join(chunks).strip()

    def _extract_json_object(self, text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None

        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()

        try:
            candidate = json.loads(cleaned)
            if isinstance(candidate, dict):
                return candidate
        except json.JSONDecodeError:
            pass

        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None

        fragment = cleaned[start : end + 1]
        try:
            candidate = json.loads(fragment)
        except json.JSONDecodeError:
            return None
        return candidate if isinstance(candidate, dict) else None

    def _parse_openai_reply(
        self,
        response: Dict[str, Any],
        allowed_ids: List[str],
    ) -> Optional[SalesReply]:
        text = self._extract_text(response)
        parsed = self._extract_json_object(text)
        if not parsed:
            return None

        answer_text = parsed.get("answer_text")
        call_to_action = parsed.get("call_to_action")
        next_question = parsed.get("next_question")

        if not isinstance(answer_text, str) or not answer_text.strip():
            return None
        if not isinstance(call_to_action, str) or not call_to_action.strip():
            return None

        ids = parsed.get("recommended_product_ids")
        recommended: List[str] = []
        if isinstance(ids, list):
            for item in ids:
                if isinstance(item, str) and item in allowed_ids and item not in recommended:
                    recommended.append(item)

        return SalesReply(
            answer_text=answer_text.strip(),
            next_question=next_question.strip() if isinstance(next_question, str) and next_question.strip() else None,
            call_to_action=call_to_action.strip(),
            recommended_product_ids=recommended,
            used_fallback=False,
        )

    def _fallback_reply(self, criteria: SearchCriteria, top_products: List[Product]) -> SalesReply:
        lead = top_products[0]
        secondary = top_products[1:3]
        options = [lead.title] + [item.title for item in secondary]
        options_text = "; ".join(options)

        criteria_hint = []
        if criteria.grade is not None:
            criteria_hint.append(f"класс: {criteria.grade}")
        if criteria.goal:
            criteria_hint.append(f"цель: {criteria.goal}")
        if criteria.subject:
            criteria_hint.append(f"предмет: {criteria.subject}")
        if criteria.format:
            criteria_hint.append(f"формат: {criteria.format}")
        hint = ", ".join(criteria_hint)

        answer = (
            "По вашим параметрам подобрал релевантные программы. "
            f"Рекомендую начать с: {options_text}."
        )
        if hint:
            answer = f"По параметрам ({hint}) подобрал релевантные программы. {options_text}."

        return SalesReply(
            answer_text=answer,
            next_question="Подтвердите, пожалуйста, удобный формат обучения.",
            call_to_action="Оставьте телефон, и я помогу выбрать лучший вариант и следующий шаг.",
            recommended_product_ids=[item.id for item in top_products[:3]],
            used_fallback=True,
        )
