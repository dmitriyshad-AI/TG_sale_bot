import unittest

try:
    from sales_agent.sales_core.flow import (
        STATE_ASK_CONTACT,
        STATE_ASK_FORMAT,
        STATE_ASK_GOAL,
        STATE_ASK_GRADE,
        STATE_ASK_SUBJECT,
        STATE_DONE,
        STATE_SUGGEST_PRODUCTS,
        advance_flow,
        ensure_state,
    )

    HAS_FLOW_DEPS = True
except ModuleNotFoundError:
    HAS_FLOW_DEPS = False


@unittest.skipUnless(HAS_FLOW_DEPS, "flow dependencies are not installed")
class FlowTests(unittest.TestCase):
    def test_ensure_state_initializes_defaults(self) -> None:
        state = ensure_state(None, brand_default="kmipt")
        self.assertEqual(state["state"], STATE_ASK_GRADE)
        self.assertEqual(state["criteria"]["brand"], "kmipt")

    def test_grade_step_accepts_callback(self) -> None:
        state = ensure_state(None, brand_default="kmipt")
        step = advance_flow(state, brand_default="kmipt", callback_data="grade:9")
        self.assertEqual(step.next_state, STATE_ASK_GOAL)
        self.assertEqual(step.state_data["criteria"]["grade"], 9)

    def test_full_path_to_suggest_products(self) -> None:
        state = ensure_state(None, brand_default="kmipt")
        step = advance_flow(state, brand_default="kmipt", callback_data="grade:10")
        step = advance_flow(step.state_data, brand_default="kmipt", callback_data="goal:ege")
        self.assertEqual(step.next_state, STATE_ASK_SUBJECT)

        step = advance_flow(step.state_data, brand_default="kmipt", callback_data="subject:math")
        self.assertEqual(step.next_state, STATE_ASK_FORMAT)

        step = advance_flow(step.state_data, brand_default="kmipt", callback_data="format:online")
        self.assertEqual(step.next_state, STATE_SUGGEST_PRODUCTS)
        self.assertTrue(step.should_suggest_products)

    def test_suggest_to_contact_to_done(self) -> None:
        state = {
            "state": STATE_SUGGEST_PRODUCTS,
            "criteria": {
                "brand": "kmipt",
                "grade": 10,
                "goal": "ege",
                "subject": "math",
                "format": "online",
            },
            "contact": None,
        }
        step = advance_flow(state, brand_default="kmipt", callback_data="contact:start")
        self.assertEqual(step.next_state, STATE_ASK_CONTACT)
        self.assertTrue(step.ask_contact_now)

        step = advance_flow(step.state_data, brand_default="kmipt", message_text="+79991234567")
        self.assertEqual(step.next_state, STATE_DONE)
        self.assertTrue(step.completed)
        self.assertEqual(step.state_data["contact"], "+79991234567")

    def test_suggest_state_with_free_text_does_not_repeat_product_suggestion(self) -> None:
        state = {
            "state": STATE_SUGGEST_PRODUCTS,
            "criteria": {
                "brand": "kmipt",
                "grade": 11,
                "goal": "ege",
                "subject": "physics",
                "format": "offline",
            },
            "contact": None,
        }
        step = advance_flow(
            state_data=state,
            brand_default="kmipt",
            message_text="что такое косинус?",
        )
        self.assertEqual(step.next_state, STATE_SUGGEST_PRODUCTS)
        self.assertFalse(step.should_suggest_products)
        self.assertIn("отвечу", step.message.lower())

    def test_restart_from_any_step(self) -> None:
        state = {
            "state": STATE_ASK_CONTACT,
            "criteria": {
                "brand": "foton",
                "grade": 8,
                "goal": "camp",
                "subject": None,
                "format": "offline",
            },
            "contact": "+79990001122",
        }
        step = advance_flow(state, brand_default="kmipt", callback_data="flow:restart")
        self.assertEqual(step.next_state, STATE_ASK_GRADE)
        self.assertEqual(step.state_data["criteria"]["brand"], "kmipt")
        self.assertIsNone(step.state_data["criteria"]["grade"])

    def test_invalid_grade_keeps_same_step(self) -> None:
        state = ensure_state(None, brand_default="kmipt")
        step = advance_flow(state, brand_default="kmipt", message_text="100")
        self.assertEqual(step.next_state, STATE_ASK_GRADE)
        self.assertIn("класс", step.message.lower())


if __name__ == "__main__":
    unittest.main()
