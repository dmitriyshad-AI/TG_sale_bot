import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import FastAPI

from sales_agent.sales_api.services.runtime_orchestration import (
    faq_lab_loop,
    lead_radar_loop,
    mango_poll_loop,
    process_next_webhook_queue_item,
    webhook_worker_loop,
)


class _Conn:
    def __init__(self, marker: str, closed: list[str]) -> None:
        self.marker = marker
        self._closed = closed

    def close(self) -> None:
        self._closed.append(self.marker)


class RuntimeOrchestrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_next_webhook_queue_item_returns_false_without_telegram_app(self) -> None:
        app = FastAPI()
        result = await process_next_webhook_queue_item(
            app_instance=app,
            database_path="/tmp/test.db",
            get_connection=Mock(),
            claim_webhook_update=Mock(),
            mark_webhook_update_retry=Mock(),
            mark_webhook_update_done=Mock(),
            update_parser=Mock(),
            retry_base_seconds=2,
            max_attempts=5,
            logger=Mock(),
        )
        self.assertFalse(result)

    async def test_process_next_webhook_queue_item_returns_false_when_queue_empty(self) -> None:
        app = FastAPI()
        app.state.telegram_application = SimpleNamespace(bot=object(), process_update=AsyncMock())

        closed: list[str] = []

        result = await process_next_webhook_queue_item(
            app_instance=app,
            database_path="/tmp/test.db",
            get_connection=Mock(return_value=_Conn("claim", closed)),
            claim_webhook_update=Mock(return_value=None),
            mark_webhook_update_retry=Mock(),
            mark_webhook_update_done=Mock(),
            update_parser=Mock(),
            retry_base_seconds=2,
            max_attempts=5,
            logger=Mock(),
        )
        self.assertFalse(result)
        self.assertEqual(closed, ["claim"])

    async def test_process_next_webhook_queue_item_marks_done_on_success(self) -> None:
        app = FastAPI()
        process_update = AsyncMock()
        app.state.telegram_application = SimpleNamespace(bot=object(), process_update=process_update)

        closed: list[str] = []
        connections = iter([_Conn("claim", closed), _Conn("done", closed)])

        get_connection = Mock(side_effect=lambda _path: next(connections))
        claim_webhook_update_mock = Mock(return_value={"id": 11, "payload": {"update_id": 1}, "attempts": 1})
        mark_retry = Mock()
        mark_done = Mock()
        update_obj = SimpleNamespace(update_id=1)
        update_parser = Mock(return_value=update_obj)

        result = await process_next_webhook_queue_item(
            app_instance=app,
            database_path="/tmp/test.db",
            get_connection=get_connection,
            claim_webhook_update=claim_webhook_update_mock,
            mark_webhook_update_retry=mark_retry,
            mark_webhook_update_done=mark_done,
            update_parser=update_parser,
            retry_base_seconds=2,
            max_attempts=5,
            logger=Mock(),
        )

        self.assertTrue(result)
        claim_webhook_update_mock.assert_called_once()
        update_parser.assert_called_once_with({"update_id": 1}, app.state.telegram_application.bot)
        process_update.assert_awaited_once_with(update_obj)
        mark_done.assert_called_once()
        mark_retry.assert_not_called()
        self.assertEqual(closed, ["claim", "done"])

    async def test_process_next_webhook_queue_item_retries_on_processing_error(self) -> None:
        app = FastAPI()
        process_update = AsyncMock(side_effect=RuntimeError("boom"))
        app.state.telegram_application = SimpleNamespace(bot=object(), process_update=process_update)

        closed: list[str] = []
        connections = iter([_Conn("claim", closed), _Conn("retry", closed)])

        mark_retry = Mock(return_value="queued")
        mark_done = Mock()
        logger = Mock()

        result = await process_next_webhook_queue_item(
            app_instance=app,
            database_path="/tmp/test.db",
            get_connection=Mock(side_effect=lambda _path: next(connections)),
            claim_webhook_update=Mock(return_value={"id": 12, "payload": {"update_id": 2}, "attempts": 3}),
            mark_webhook_update_retry=mark_retry,
            mark_webhook_update_done=mark_done,
            update_parser=Mock(return_value=SimpleNamespace(update_id=2)),
            retry_base_seconds=2,
            max_attempts=5,
            logger=logger,
        )

        self.assertTrue(result)
        mark_done.assert_not_called()
        mark_retry.assert_called_once()
        kwargs = mark_retry.call_args.kwargs
        self.assertEqual(kwargs["queue_id"], 12)
        self.assertEqual(kwargs["retry_delay_seconds"], 8)
        self.assertEqual(kwargs["max_attempts"], 5)
        logger.exception.assert_called_once()
        self.assertEqual(closed, ["claim", "retry"])

    async def test_process_next_webhook_queue_item_retries_on_invalid_parser_result(self) -> None:
        app = FastAPI()
        app.state.telegram_application = SimpleNamespace(bot=object(), process_update=AsyncMock())

        closed: list[str] = []
        connections = iter([_Conn("claim", closed), _Conn("retry", closed)])
        mark_retry = Mock(return_value="failed")
        logger = Mock()

        result = await process_next_webhook_queue_item(
            app_instance=app,
            database_path="/tmp/test.db",
            get_connection=Mock(side_effect=lambda _path: next(connections)),
            claim_webhook_update=Mock(return_value={"id": 13, "payload": {"update_id": 3}, "attempts": 1}),
            mark_webhook_update_retry=mark_retry,
            mark_webhook_update_done=Mock(),
            update_parser=Mock(return_value=None),
            retry_base_seconds=2,
            max_attempts=5,
            logger=logger,
        )

        self.assertTrue(result)
        mark_retry.assert_called_once()
        logger.exception.assert_called_once()
        self.assertEqual(closed, ["claim", "retry"])

    async def test_webhook_worker_loop_returns_when_event_absent(self) -> None:
        app = FastAPI()
        await webhook_worker_loop(
            app_instance=app,
            process_next_item=AsyncMock(),
            logger=Mock(),
        )

    async def test_webhook_worker_loop_clears_event_after_wait(self) -> None:
        app = FastAPI()
        app.state.webhook_worker_event = asyncio.Event()
        app.state.webhook_worker_event.set()

        process_next_item = AsyncMock(side_effect=[False, asyncio.CancelledError()])

        with self.assertRaises(asyncio.CancelledError):
            await webhook_worker_loop(
                app_instance=app,
                process_next_item=process_next_item,
                logger=Mock(),
            )

        self.assertFalse(app.state.webhook_worker_event.is_set())

    async def test_lead_radar_loop_runs_and_logs_summary(self) -> None:
        app = FastAPI()
        app.state.lead_radar_event = asyncio.Event()
        app.state.lead_radar_event.set()

        run_once = AsyncMock(
            side_effect=[
                {"created_followups": 2, "created_drafts": 1},
                asyncio.CancelledError(),
            ]
        )
        logger = Mock()

        with self.assertRaises(asyncio.CancelledError):
            await lead_radar_loop(
                app_instance=app,
                interval_seconds=5,
                run_once=run_once,
                logger=logger,
            )

        logger.info.assert_called_once()

    async def test_faq_lab_loop_runs_and_logs_summary(self) -> None:
        app = FastAPI()
        app.state.faq_lab_event = asyncio.Event()
        app.state.faq_lab_event.set()

        run_once = AsyncMock(
            side_effect=[
                {"candidates_upserted": 3, "canonical_synced": 1},
                asyncio.CancelledError(),
            ]
        )
        logger = Mock()

        with self.assertRaises(asyncio.CancelledError):
            await faq_lab_loop(
                app_instance=app,
                interval_seconds=5,
                run_once=run_once,
                logger=logger,
            )

        logger.info.assert_called_once()

    async def test_mango_poll_loop_runs_and_logs_summary(self) -> None:
        app = FastAPI()
        app.state.mango_poll_event = asyncio.Event()
        app.state.mango_poll_event.set()

        run_once = AsyncMock(
            side_effect=[
                {"processed": 2, "created": 1, "duplicates": 0, "failed": 0},
                asyncio.CancelledError(),
            ]
        )
        logger = Mock()

        with self.assertRaises(asyncio.CancelledError):
            await mango_poll_loop(
                app_instance=app,
                interval_seconds=1,
                run_once=run_once,
                logger=logger,
            )

        logger.info.assert_called_once()


if __name__ == "__main__":
    unittest.main()
