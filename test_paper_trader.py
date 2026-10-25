import unittest
from unittest.mock import patch

import paper_trader


class PaperTraderTrailingStopTests(unittest.TestCase):
    def setUp(self):
        state = paper_trader.grid_state
        state["available_balance"] = paper_trader.START_BALANCE_USD
        state["used_margin"] = 0.0
        state["unrealized_pnl"] = 0.0
        state["total_realized_pnl"] = 0.0
        state["total_fees_paid"] = 0.0
        state["completed_trades"] = 0
        state["liquidations"] = 0
        state["positions"] = []
        state["signals"] = {}
        state["top_gainers"] = []
        state["history"] = []

    def test_long_position_trails_after_profit_trigger(self):
        paper_trader._open_long("ETH/USDT", 100.0)
        position = paper_trader.grid_state["positions"][0]

        with patch.object(paper_trader, "get_single_price", return_value=102.0):
            paper_trader.update_open_positions()

        self.assertEqual(len(paper_trader.grid_state["positions"]), 1)
        self.assertTrue(position["trailing_active"])
        self.assertEqual(position["sl_price"], 101.0)

        with patch.object(paper_trader, "get_single_price", return_value=105.0):
            paper_trader.update_open_positions()

        self.assertEqual(position["sl_price"], 104.0)
        self.assertEqual(len(paper_trader.grid_state["positions"]), 1)

        with patch.object(paper_trader, "get_single_price", return_value=104.0):
            paper_trader.update_open_positions()

        self.assertEqual(paper_trader.grid_state["positions"], [])
        self.assertGreater(paper_trader.grid_state["total_realized_pnl"], 0.0)
        self.assertEqual(paper_trader.grid_state["completed_trades"], 1)

    def test_short_position_trails_after_profit_trigger(self):
        paper_trader._open_short("ETH/USDT", 100.0)
        position = paper_trader.grid_state["positions"][0]

        with patch.object(paper_trader, "get_single_price", return_value=98.0):
            paper_trader.update_open_positions()

        self.assertEqual(len(paper_trader.grid_state["positions"]), 1)
        self.assertTrue(position["trailing_active"])
        self.assertEqual(position["sl_price"], 99.0)

        with patch.object(paper_trader, "get_single_price", return_value=95.0):
            paper_trader.update_open_positions()

        self.assertEqual(position["sl_price"], 96.0)

        with patch.object(paper_trader, "get_single_price", return_value=96.0):
            paper_trader.update_open_positions()

        self.assertEqual(paper_trader.grid_state["positions"], [])
        self.assertGreater(paper_trader.grid_state["total_realized_pnl"], 0.0)

    def test_hourly_signal_waits_when_timeframes_disagree(self):
        paper_trader.grid_state["top_gainers"] = [
            {"symbol": "ETH/USDT", "change_24h": 20.0}
        ]
        candle = {"open": 100.0, "close": 99.0, "candle_time": 1}

        with patch.object(paper_trader, "fetch_last_closed_hour_candle", return_value=candle):
            paper_trader.process_hourly_signal("ETH/USDT")

        self.assertEqual(paper_trader.grid_state["signals"]["ETH/USDT"]["side"], "WAIT")
        self.assertEqual(paper_trader.grid_state["positions"], [])

    def test_hourly_signal_opens_when_timeframes_agree(self):
        paper_trader.grid_state["top_gainers"] = [
            {"symbol": "ETH/USDT", "change_24h": 20.0}
        ]
        candle = {"open": 100.0, "close": 101.0, "candle_time": 1}

        with (
            patch.object(paper_trader, "fetch_last_closed_hour_candle", return_value=candle),
            patch.object(paper_trader, "get_single_price", return_value=101.0),
        ):
            paper_trader.process_hourly_signal("ETH/USDT")

        self.assertEqual(paper_trader.grid_state["signals"]["ETH/USDT"]["side"], "LONG")
        self.assertEqual(len(paper_trader.grid_state["positions"]), 1)


if __name__ == "__main__":
    unittest.main()
