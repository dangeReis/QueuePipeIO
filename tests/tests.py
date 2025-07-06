import threading
import unittest

from queuepipeio import LimitedQueueIO, QueueIO  # Updated import statements

MB = 1024 * 1024


class TestQueueIO(unittest.TestCase):
    def setUp(self):
        self.qio = QueueIO()  # Updated class name

    def test_write(self):
        self.assertEqual(self.qio.write(b"Hello, world!"), 13)

    def test_read(self):
        def write_and_close():
            self.qio.write(b"Hello, world!")
            self.qio.close()

        threading.Thread(target=write_and_close).start()
        self.assertEqual(self.qio.read(), b"Hello, world!")

    def test_close(self):
        self.qio.close()
        self.assertTrue(self.qio.closed)


class TestLimitedQueueIO(unittest.TestCase):
    def setUp(self):
        self.lqio = LimitedQueueIO(
            memory_limit=16 * MB, chunk_size=8 * MB, show_progress=False
        )  # Updated class name

    def test_write(self):
        self.assertEqual(self.lqio.write(b"Hello, again!"), 13)

    def test_read(self):
        def write_and_close():
            self.lqio.write(b"Hello, again!")
            self.lqio.close()

        threading.Thread(target=write_and_close).start()
        self.assertEqual(self.lqio.read(), b"Hello, again!")

    def test_close(self):
        self.lqio.close()
        self.assertTrue(self.lqio.closed)


if __name__ == "__main__":
    unittest.main()
