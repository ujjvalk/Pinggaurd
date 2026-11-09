import asyncio
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

_tmp = tempfile.TemporaryDirectory()
os.environ['PINGGUARD_DB'] = _tmp.name + '/test.db'
os.environ['PINGGUARD_PASSWORD'] = 'test-only'
import app
import checks
import notifiers


class ValidationTests(unittest.TestCase):
    def test_invalid_targets_and_options(self):
        for kind, target, options in [
            ('tcp', 'localhost:0', {}), ('tcp', ':80', {}),
            ('tcp', 'localhost:65536', {}), ('ssl', 'localhost:abc', {}),
            ('http', 'https://', {}), ('http', 'https://example.com', {'expected_status': 'abc'}),
            ('http', 'https://example.com', {'verify_ssl': 'false'}),
            ('http', 'https://example.com', {'keyword': 123}),
            ('heartbeat', '', {'grace': -1}), ('ssl', 'example.com', {'warn_days': 1.5}),
        ]:
            with self.subTest(kind=kind, target=target, options=options):
                with self.assertRaises(ValueError):
                    checks.validate(kind, target, options)

    def test_valid_targets(self):
        self.assertEqual(checks.host_port('[::1]:443'), ('::1', 443))
        checks.validate('http', 'https://example.com', {'expected_status': 200, 'verify_ssl': False})
        checks.validate('heartbeat', '', {'grace': 0})


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for table in ('checks', 'incidents', 'monitors', 'channels'):
            app.run_sql(f'DELETE FROM {table}')

    def monitor(self):
        mid = app.add_monitor(app.MonitorIn(name='Test', type='tcp', target='localhost:80', threshold=2))['id']
        return dict(app.q('SELECT * FROM monitors WHERE id=?', (mid,), one=True))

    async def test_deleted_monitor_does_not_recreate_history(self):
        m = self.monitor()
        app.delete_monitor(m['id'])
        await app.record(m, False, None, 'failed')
        self.assertEqual(len(app.q('SELECT * FROM checks')), 0)
        self.assertEqual(len(app.q('SELECT * FROM incidents')), 0)
        replacement = self.monitor()
        self.assertEqual(m['id'], replacement['id'])
        await app.record(m, False, None, 'stale')
        self.assertEqual(len(app.q('SELECT * FROM checks')), 0)

    async def test_failure_threshold_and_recovery_use_current_state(self):
        m = self.monitor()
        with patch.object(app, 'dispatch', new_callable=AsyncMock) as dispatch:
            await app.record(m, False, None, 'failed')
            dispatch.assert_not_awaited()
            await app.record(m, False, None, 'failed')
            await app.record(m, True, 1, '')
            self.assertEqual([c.args[1]['status'] for c in dispatch.await_args_list], ['DOWN', 'UP'])
        self.assertIsNotNone(app.q('SELECT * FROM incidents', one=True)['ended'])

    async def test_notification_retries_and_redacts_errors(self):
        channel = {'id': 1, 'name': 'Test', 'type': 'webhook', 'config': '{}'}
        with patch.object(notifiers, 'send', new_callable=AsyncMock) as send, patch.object(app.asyncio, 'sleep', new_callable=AsyncMock):
            send.side_effect = RuntimeError('https://secret-token.example')
            with self.assertLogs('pingguard', level='WARNING') as logs:
                error = await app.send_one(channel, {})
            self.assertEqual(send.await_count, 3)
            self.assertNotIn('secret-token', error + str(logs.output))
            send.reset_mock()
            send.side_effect = [RuntimeError('temporary'), None]
            self.assertIsNone(await app.send_one(channel, {}))
            self.assertEqual(send.await_count, 2)

    async def test_shutdown_cancels_running_checks(self):
        self.monitor()
        started, stopped = asyncio.Event(), asyncio.Event()
        async def slow_check(m):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        with patch.object(checks, 'run', slow_check):
            task = asyncio.create_task(app.scheduler())
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(stopped.is_set())
