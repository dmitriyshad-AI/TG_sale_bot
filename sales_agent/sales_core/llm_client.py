from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import httpx

from sales_agent.sales_core.catalog import Product, SearchCriteria
from sales_agent.sales_core.tone import ToneProfile, load_tone_profile, tone_as_prompt_block


@dataclass
class SalesReply:
    answer_text: str
    next_question: Optional[str]
    call_to_action: str
    recommended_product_ids: List[str]
    used_fallback: bool
    error: Optional[str] = None


@dataclass
class KnowledgeReply:
    answer_text: str
    sources: List[str]
    used_fallback: bool
    error: Optional[str] = None


@dataclass
class GeneralHelpReply:
    answer_text: str
    used_fallback: bool
    error: Optional[str] = None


class LLMClient:
    def __init__(
        self,
        api_key: str,
        model: str,
        endpoint: str = "https://api.openai.com/v1/responses",
        timeout_seconds: float = 25.0,
        tone_profile: Optional[ToneProfile] = None,
    ) -> None:
        self.api_key = api_key.strip()
        self.model = model.strip() or "gpt-4.1"
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.tone_profile = tone_profile or load_tone_profile()

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

        payload = self._build_sales_payload(criteria, top_products)
        raw, error = self._send_request(payload)
        if error:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = error
            return fallback

        parsed = self._parse_openai_sales_reply(raw or {}, allowed_ids=[product.id for product in top_products])
        if parsed is None:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = "Could not parse structured LLM response"
            return fallback

        return parsed

    async def build_sales_reply_async(
        self,
        criteria: SearchCriteria,
        top_products: List[Product],
    ) -> SalesReply:
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

        payload = self._build_sales_payload(criteria, top_products)
        raw, error = await self._send_request_async(payload)
        if error:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = error
            return fallback

        parsed = self._parse_openai_sales_reply(raw or {}, allowed_ids=[product.id for product in top_products])
        if parsed is None:
            fallback = self._fallback_reply(criteria, top_products)
            fallback.error = "Could not parse structured LLM response"
            return fallback

        return parsed

    async def build_consultative_reply_async(
        self,
        *,
        user_message: str,
        criteria: SearchCriteria,
        top_products: List[Product],
        missing_fields: List[str],
        repeat_count: int = 0,
        product_offer_allowed: bool = True,
        recent_history: Optional[List[Dict[str, str]]] = None,
    ) -> SalesReply:
        if not self.is_configured():
            fallback = self._fallback_consultative_reply(
                criteria=criteria,
                top_products=top_products,
                missing_fields=missing_fields,
                product_offer_allowed=product_offer_allowed,
            )
            fallback.error = "OPENAI_API_KEY is not configured"
            return fallback

        payload = self._build_consultative_payload(
            user_message=user_message,
            criteria=criteria,
            top_products=top_products,
            missing_fields=missing_fields,
            repeat_count=repeat_count,
            product_offer_allowed=product_offer_allowed,
            recent_history=recent_history,
        )
        raw, error = await self._send_request_async(payload)
        if error:
            fallback = self._fallback_consultative_reply(
                criteria=criteria,
                top_products=top_products,
                missing_fields=missing_fields,
                product_offer_allowed=product_offer_allowed,
            )
            fallback.error = error
            return fallback

        parsed = self._parse_openai_sales_reply(raw or {}, allowed_ids=[product.id for product in top_products])
        if parsed is None:
            fallback = self._fallback_consultative_reply(
                criteria=criteria,
                top_products=top_products,
                missing_fields=missing_fields,
                product_offer_allowed=product_offer_allowed,
            )
            fallback.error = "Could not parse structured LLM response"
            return fallback

        return parsed

    async def build_general_help_reply_async(
        self,
        *,
        user_message: str,
        dialogue_state: Optional[str] = None,
        recent_history: Optional[List[Dict[str, str]]] = None,
    ) -> GeneralHelpReply:
        if not user_message.strip():
            return GeneralHelpReply(
                answer_text="Сформулируйте вопрос, и я постараюсь объяснить простыми словами.",
                used_fallback=True,
            )

        if not self.is_configured():
            fallback = self._fallback_general_help_reply(user_message=user_message, dialogue_state=dialogue_state)
            fallback.error = "OPENAI_API_KEY is not configured"
            return fallback

        payload = self._build_general_help_payload(
            user_message=user_message,
            dialogue_state=dialogue_state,
            recent_history=recent_history,
        )
        raw, error = await self._send_request_async(payload)
        if error:
            fallback = self._fallback_general_help_reply(user_message=user_message, dialogue_state=dialogue_state)
            fallback.error = error
            return fallback

        text = self._extract_text(raw or {})
        if not text:
            fallback = self._fallback_general_help_reply(user_message=user_message, dialogue_state=dialogue_state)
            fallback.error = "empty response text"
            return fallback

        return GeneralHelpReply(answer_text=text.strip(), used_fallback=False)

    def answer_knowledge_question(
        self,
        question: str,
        vector_store_id: Optional[str],
    ) -> KnowledgeReply:
        if not question.strip():
            return KnowledgeReply(
                answer_text="Задайте вопрос по условиям, документам или оплате.",
                sources=[],
                used_fallback=True,
            )

        if not self.is_configured():
            return KnowledgeReply(
                answer_text=(
                    "LLM не настроен. Заполните OPENAI_API_KEY и повторите запрос."
                ),
                sources=[],
                used_fallback=True,
                error="OPENAI_API_KEY is not configured",
            )

        if not vector_store_id:
            return KnowledgeReply(
                answer_text=(
                    "База знаний пока не подключена. Запустите синхронизацию: "
                    "python3 scripts/sync_vector_store.py"
                ),
                sources=[],
                used_fallback=True,
                error="vector_store_id is not configured",
            )

        payload = self._build_knowledge_payload(question=question, vector_store_id=vector_store_id)
        raw, error = self._send_request(payload)
        if error:
            return KnowledgeReply(
                answer_text=(
                    "Не удалось обратиться к knowledge-базе. "
                    "Попробуйте позже или уточните вопрос менеджеру."
                ),
                sources=[],
                used_fallback=True,
                error=error,
            )

        text = self._extract_text(raw or {})
        if not text:
            return KnowledgeReply(
                answer_text=(
                    "Не удалось получить ответ из knowledge-базы. "
                    "Попробуйте переформулировать вопрос."
                ),
                sources=[],
                used_fallback=True,
                error="empty response text",
            )

        sources = self._extract_source_names(raw or {})
        return KnowledgeReply(
            answer_text=text,
            sources=sources,
            used_fallback=False,
        )

    async def answer_knowledge_question_async(
        self,
        question: str,
        vector_store_id: Optional[str],
    ) -> KnowledgeReply:
        if not question.strip():
            return KnowledgeReply(
                answer_text="Задайте вопрос по условиям, документам или оплате.",
                sources=[],
                used_fallback=True,
            )

        if not self.is_configured():
            return KnowledgeReply(
                answer_text=(
                    "LLM не настроен. Заполните OPENAI_API_KEY и повторите запрос."
                ),
                sources=[],
                used_fallback=True,
                error="OPENAI_API_KEY is not configured",
            )

        if not vector_store_id:
            return KnowledgeReply(
                answer_text=(
                    "База знаний пока не подключена. Запустите синхронизацию: "
                    "python3 scripts/sync_vector_store.py"
                ),
                sources=[],
                used_fallback=True,
                error="vector_store_id is not configured",
            )

        payload = self._build_knowledge_payload(question=question, vector_store_id=vector_store_id)
        raw, error = await self._send_request_async(payload)
        if error:
            return KnowledgeReply(
                answer_text=(
                    "Не удалось обратиться к knowledge-базе. "
                    "Попробуйте позже или уточните вопрос менеджеру."
                ),
                sources=[],
                used_fallback=True,
                error=error,
            )

        text = self._extract_text(raw or {})
        if not text:
            return KnowledgeReply(
                answer_text=(
                    "Не удалось получить ответ из knowledge-базы. "
                    "Попробуйте переформулировать вопрос."
                ),
                sources=[],
                used_fallback=True,
                error="empty response text",
            )

        sources = self._extract_source_names(raw or {})
        return KnowledgeReply(
            answer_text=text,
            sources=sources,
            used_fallback=False,
        )

    def _build_sales_payload(self, criteria: SearchCriteria, top_products: List[Product]) -> Dict[str, Any]:
        criteria_payload = {
            "brand": criteria.brand,
            "grade": criteria.grade,
            "goal": criteria.goal,
            "subject": criteria.subject,
            "format": criteria.format,
        }
        products_payload = [self._product_payload(product) for product in top_products]

        tone_block = tone_as_prompt_block(self.tone_profile)
        system_prompt = (
            "Ты опытный консультант по школьному образованию. "
            "Сначала принеси пользу клиенту: объясни стратегию и следующий шаг, "
            "затем мягко предложи программу. "
            "Тон общения: уважительный, дружелюбный, как у квалифицированного сотрудника отдела продаж. "
            "Обращайся на 'вы', без давления и манипуляций. "
            "Используй только факты из переданного каталога. "
            "Не выдумывай цены, даты, условия и ссылки. "
            "Если данных не хватает, честно скажи и попроси уточнение. "
            "Ответ обязателен строго в JSON с ключами: "
            "answer_text, next_question, call_to_action, recommended_product_ids.\n\n"
            f"{tone_block}"
        )

        user_prompt = (
            "Критерии клиента:\n"
            f"{json.dumps(criteria_payload, ensure_ascii=False)}\n\n"
            "Доступные продукты (использовать только их):\n"
            f"{json.dumps(products_payload, ensure_ascii=False)}\n\n"
            "Сформируй полезный, человечный и точный ответ в тоне сильного консультанта. "
            "Без навязчивых продаж, без категоричности и без шаблонных фраз. "
            "recommended_product_ids должен содержать только id из списка продуктов."
        )

        return {
            "model": self.model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_prompt}],
                },
            ],
            "temperature": 0.2,
            "max_output_tokens": 600,
        }

    def _build_consultative_payload(
        self,
        *,
        user_message: str,
        criteria: SearchCriteria,
        top_products: List[Product],
        missing_fields: List[str],
        repeat_count: int,
        product_offer_allowed: bool,
        recent_history: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        criteria_payload = {
            "brand": criteria.brand,
            "grade": criteria.grade,
            "goal": criteria.goal,
            "subject": criteria.subject,
            "format": criteria.format,
        }
        products_payload = [self._product_payload(product) for product in top_products]
        history_payload = recent_history or []

        tone_block = tone_as_prompt_block(self.tone_profile)
        system_prompt = (
            "Ты консультант УНПК МФТИ по выбору образовательной траектории. "
            "Цель: сначала помочь родителю и ученику с понятным планом действий, "
            "и только потом мягко предложить релевантные программы. "
            "Не используй агрессивные продажи. "
            "Пиши уважительно, дружелюбно и профессионально, в тоне квалифицированного сотрудника отдела продаж. "
            "Обращайся на 'вы', не спорь с клиентом и не дави на срочное решение. "
            "Без канцелярита и без заученных рекламных клише. "
            "Для фактов о программах используй только переданный каталог. "
            "Не выдумывай цены, даты, условия и ссылки. "
            "Если данных недостаточно, попроси одно конкретное уточнение. "
            "Верни строго JSON с ключами: "
            "answer_text, next_question, call_to_action, recommended_product_ids.\n\n"
            f"{tone_block}"
        )
        user_prompt = (
            "Сообщение клиента:\n"
            f"{user_message.strip()}\n\n"
            "Известные параметры клиента:\n"
            f"{json.dumps(criteria_payload, ensure_ascii=False)}\n\n"
            "Какие поля пока не заполнены:\n"
            f"{json.dumps(missing_fields, ensure_ascii=False)}\n\n"
            "Краткая история последних сообщений в диалоге:\n"
            f"{json.dumps(history_payload, ensure_ascii=False)}\n\n"
            f"Повторов одинакового запроса подряд: {repeat_count}\n\n"
            f"Можно ли на этом шаге предлагать программы: {'да' if product_offer_allowed else 'нет'}\n\n"
            "Доступные программы (использовать только их):\n"
            f"{json.dumps(products_payload, ensure_ascii=False)}\n\n"
            "Сделай ответ максимально полезным, конкретным и человечным. "
            "Сначала польза. Если предлагать программы пока нельзя, не перечисляй курсы и не проси оставить контакт. "
            "Если предлагать программы можно, предложи мягко, без давления. "
            "В конце задай один короткий уточняющий вопрос. "
            "recommended_product_ids должен содержать только id из списка программ."
        )
        return {
            "model": self.model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_prompt}],
                },
            ],
            "temperature": 0.35,
            "max_output_tokens": 800,
        }

    def _build_general_help_payload(
        self,
        user_message: str,
        dialogue_state: Optional[str],
        recent_history: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        history_payload = recent_history or []
        tone_block = tone_as_prompt_block(self.tone_profile)
        system_prompt = (
            "Вы образовательный консультант-наставник. "
            "Задача: отвечать по-человечески, понятно и полезно, как живой эксперт. "
            "Сфокусируйтесь на пользе и объяснении. "
            "Не давите продажей и не просите контакт, если пользователь сам об этом не просил. "
            "Коротко и конкретно: 3-6 предложений, можно один мини-пример.\n\n"
            f"{tone_block}"
        )
        user_prompt = (
            "Контекст состояния диалога:\n"
            f"{dialogue_state or 'unknown'}\n\n"
            "Краткая история последних сообщений:\n"
            f"{json.dumps(history_payload, ensure_ascii=False)}\n\n"
            "Вопрос пользователя:\n"
            f"{user_message.strip()}\n\n"
            "Дайте спокойный, полезный и естественный ответ."
        )
        return {
            "model": self.model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_prompt}],
                },
            ],
            "temperature": 0.35,
            "max_output_tokens": 500,
        }

    def _build_knowledge_payload(self, question: str, vector_store_id: str) -> Dict[str, Any]:
        system_prompt = (
            "Ты консультант по условиям образовательных программ. "
            "Отвечай строго на основе найденных документов из file_search. "
            "Если фактов недостаточно, честно скажи, что нужно уточнить у менеджера. "
            "Не придумывай юридические и финансовые условия."
        )
        user_prompt = (
            "Вопрос клиента:\n"
            f"{question.strip()}\n\n"
            "Дай короткий, понятный ответ и укажи, что уточнить при нехватке данных."
        )
        return {
            "model": self.model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_prompt}],
                },
            ],
            "tools": [
                {
                    "type": "file_search",
                    "vector_store_ids": [vector_store_id],
                }
            ],
            "tool_choice": "auto",
            "temperature": 0.2,
            "max_output_tokens": 700,
        }

    def _send_request(self, payload: Dict[str, Any]) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
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
            details = ""
            try:
                body = exc.read().decode("utf-8", errors="ignore")
                if body:
                    details = body[:500]
            except Exception:
                details = ""
            if details:
                return None, f"OpenAI HTTP error: {exc.code}. {details}"
            return None, f"OpenAI HTTP error: {exc.code}"
        except URLError as exc:
            return None, f"OpenAI connection error: {exc.reason}"
        except json.JSONDecodeError:
            return None, "OpenAI response is not valid JSON"

        return raw, None

    async def _send_request_async(self, payload: Dict[str, Any]) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    self.endpoint,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                )
        except httpx.RequestError as exc:
            return None, f"OpenAI connection error: {exc}"

        if response.status_code >= 400:
            details = (response.text or "").strip()
            if details:
                details = details[:500]
                return None, f"OpenAI HTTP error: {response.status_code}. {details}"
            return None, f"OpenAI HTTP error: {response.status_code}"

        try:
            raw = response.json() if response.text else {}
        except ValueError:
            return None, "OpenAI response is not valid JSON"

        return raw, None

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

    def _extract_source_names(self, response: Dict[str, Any]) -> List[str]:
        output = response.get("output")
        if not isinstance(output, list):
            return []

        names: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for piece in content:
                if not isinstance(piece, dict):
                    continue
                annotations = piece.get("annotations")
                if not isinstance(annotations, list):
                    continue
                for annotation in annotations:
                    if not isinstance(annotation, dict):
                        continue
                    filename = annotation.get("filename")
                    if isinstance(filename, str) and filename and filename not in names:
                        names.append(filename)
        return names

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

    def _parse_openai_sales_reply(
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
            "Спасибо за ваш запрос. По вашим параметрам подобрал релевантные программы. "
            f"Рекомендую начать с: {options_text}."
        )
        if hint:
            answer = f"Спасибо за ваш запрос. По параметрам ({hint}) подобрал релевантные программы: {options_text}."

        return SalesReply(
            answer_text=answer,
            next_question="Подскажите, пожалуйста, какой формат вам удобнее: онлайн, очно или гибрид?",
            call_to_action=(
                "Если вам удобно, оставьте телефон: помогу спокойно сравнить варианты "
                "и выбрать оптимальный следующий шаг."
            ),
            recommended_product_ids=[item.id for item in top_products[:3]],
            used_fallback=True,
        )

    def _fallback_consultative_reply(
        self,
        *,
        criteria: SearchCriteria,
        top_products: List[Product],
        missing_fields: List[str],
        product_offer_allowed: bool,
    ) -> SalesReply:
        lead_phrase = (
            "Спасибо за ваш вопрос. Понимаю ваш запрос. Давайте соберем реалистичный план подготовки, "
            "чтобы без перегруза двигаться к цели."
        )
        if criteria.grade:
            lead_phrase = (
                f"Спасибо за ваш вопрос. Для {criteria.grade} класса важно выстроить "
                "стабильный план подготовки и контроль прогресса."
            )

        options = "; ".join(product.title for product in top_products[:2]) if top_products else ""
        answer_text = lead_phrase
        if options and product_offer_allowed:
            answer_text += f" Уже вижу подходящие направления: {options}."

        next_question_map = {
            "grade": "Подскажите, пожалуйста, какой сейчас класс у ученика?",
            "goal": "Что сейчас в приоритете: ЕГЭ, олимпиады или усиление школьной базы?",
            "subject": "Какой предмет сейчас главный: математика, физика или информатика?",
            "format": "Как удобнее заниматься: онлайн, очно или гибрид?",
        }
        next_question = next_question_map.get(
            missing_fields[0] if missing_fields else "",
            "Что для вас сейчас важнее всего: темп подготовки, нагрузка или расписание?",
        )

        return SalesReply(
            answer_text=answer_text,
            next_question=next_question,
            call_to_action=(
                "Если захотите, после уточнения спокойно сравню 2-3 программы "
                "и подскажу, с какой начать без лишней нагрузки."
            ),
            recommended_product_ids=[item.id for item in top_products[:2]],
            used_fallback=True,
        )

    def _fallback_general_help_reply(
        self,
        *,
        user_message: str,
        dialogue_state: Optional[str] = None,
    ) -> GeneralHelpReply:
        normalized = user_message.lower()

        if "косинус" in normalized:
            answer = (
                "Косинус угла в прямоугольном треугольнике — это отношение прилежащего катета к гипотенузе. "
                "Обозначают так: cos(a) = прилежащий / гипотенуза. "
                "Например, если прилежащий катет 3, а гипотенуза 5, то cos(a)=0.6."
            )
            return GeneralHelpReply(answer_text=answer, used_fallback=True)

        if "синус" in normalized:
            answer = (
                "Синус угла в прямоугольном треугольнике — это отношение противолежащего катета к гипотенузе: "
                "sin(a) = противолежащий / гипотенуза. "
                "Если нужно, разберем на конкретной задаче."
            )
            return GeneralHelpReply(answer_text=answer, used_fallback=True)

        if "поступить" in normalized and "мгу" in normalized:
            answer = (
                "Для поступления в МГУ обычно важно три вещи: выбрать направление, зафиксировать нужные ЕГЭ и "
                "собрать реалистичный график подготовки. "
                "Сначала проверьте проходные баллы прошлых лет по вашему факультету, затем разложите подготовку "
                "по предметам на еженедельный план с контрольными точками."
            )
            return GeneralHelpReply(answer_text=answer, used_fallback=True)

        answer = (
            "Хороший вопрос. Могу объяснить это простыми словами и затем разобрать на примере из школьной задачи. "
            "Если хотите, напишите класс и тему, и я подстрою объяснение под ваш уровень."
        )
        if dialogue_state:
            answer += " После этого можем вернуться к вашему плану подготовки."
        return GeneralHelpReply(answer_text=answer, used_fallback=True)
