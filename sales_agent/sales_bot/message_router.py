from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class RoutePlan:
    is_program_info: bool
    should_try_consultative: bool
    should_force_consultative: bool
    is_knowledge: bool
    is_general_education: bool
    is_flow_interrupt_general: bool
    should_try_small_talk: bool


def build_route_plan(
    *,
    raw_text: str,
    current_state: Optional[str],
    current_state_payload: dict,
    is_program_info_query: Callable[[str], bool],
    is_knowledge_query: Callable[[str], bool],
    is_general_education_query: Callable[[str], bool],
    is_flow_interrupt_question: Callable[[str], bool],
    is_active_flow_state: Callable[[Optional[str]], bool],
    looks_like_fragmented_context_message: Callable[[str, dict], bool],
) -> RoutePlan:
    program_info = bool(is_program_info_query(raw_text))
    knowledge = bool(is_knowledge_query(raw_text))
    general = bool(is_general_education_query(raw_text))
    flow_interrupt = bool(is_active_flow_state(current_state) and is_flow_interrupt_question(raw_text))
    force_consultative = bool(looks_like_fragmented_context_message(raw_text, current_state_payload))
    return RoutePlan(
        is_program_info=program_info,
        should_try_consultative=not program_info,
        should_force_consultative=force_consultative,
        is_knowledge=knowledge,
        is_general_education=general,
        is_flow_interrupt_general=flow_interrupt,
        should_try_small_talk=True,
    )
