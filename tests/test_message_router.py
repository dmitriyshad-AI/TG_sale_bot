import unittest

from sales_agent.sales_bot.message_router import build_route_plan


class MessageRouterTests(unittest.TestCase):
    def test_route_plan_program_info_has_priority(self) -> None:
        plan = build_route_plan(
            raw_text="Что ты знаешь про IT лагерь?",
            current_state="ask_goal",
            current_state_payload={"state": "ask_goal"},
            is_program_info_query=lambda text: True,
            is_knowledge_query=lambda text: True,
            is_general_education_query=lambda text: True,
            is_flow_interrupt_question=lambda text: True,
            is_active_flow_state=lambda state: True,
            looks_like_fragmented_context_message=lambda text, state: True,
        )
        self.assertTrue(plan.is_program_info)
        self.assertFalse(plan.should_try_consultative)
        self.assertTrue(plan.should_force_consultative)

    def test_route_plan_regular_text(self) -> None:
        plan = build_route_plan(
            raw_text="11",
            current_state="ask_grade",
            current_state_payload={"state": "ask_grade"},
            is_program_info_query=lambda text: False,
            is_knowledge_query=lambda text: False,
            is_general_education_query=lambda text: False,
            is_flow_interrupt_question=lambda text: False,
            is_active_flow_state=lambda state: state == "ask_grade",
            looks_like_fragmented_context_message=lambda text, state: False,
        )
        self.assertFalse(plan.is_program_info)
        self.assertTrue(plan.should_try_consultative)
        self.assertFalse(plan.is_knowledge)
        self.assertFalse(plan.is_general_education)
        self.assertFalse(plan.is_flow_interrupt_general)
        self.assertTrue(plan.should_try_small_talk)


if __name__ == "__main__":
    unittest.main()
