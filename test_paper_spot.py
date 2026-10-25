import csv
import json
import unittest
import tempfile
from collections import deque
from pathlib import Path
from urllib.request import urlopen
from unittest.mock import MagicMock, patch

import paper_spot
from paper_spot import calculate_spot_quantity


class PositionSizingTests(unittest.TestCase):
    def test_quantity_uses_fixed_twenty_dollar_spot_value(self):
        class FakeExchange:
            @staticmethod
            def amount_to_precision(_symbol, quantity):
                return f"{quantity:.8f}"

        with patch.object(paper_spot, "exchange", FakeExchange()):
            quantity = calculate_spot_quantity("ETH/USDT", 100.0, 200.0)
            insufficient = calculate_spot_quantity("ETH/USDT", 100.0, 20.0)
            invalid = calculate_spot_quantity("ETH/USDT", 0.0, 200.0)

        self.assertLessEqual(quantity * 100.0, paper_spot.POSITION_VALUE)
        self.assertGreater(quantity * 100.0, paper_spot.POSITION_VALUE - 0.001)
        self.assertEqual(insufficient, 0.0)
        self.assertEqual(invalid, 0.0)


class SignalTests(unittest.TestCase):
    def _set_trades(self, symbol, now, momentum, buyer_maker):
        start_price = 100.0
        end_price = start_price * (1.0 + momentum / 100.0)
        prices = [
            start_price + (end_price - start_price) * index / 19
            for index in range(20)
        ]
        paper_spot.trade_history[symbol] = deque(
            (
                (now - 57 + index * 3, price, 10.0, buyer_maker)
                for index, price in enumerate(prices)
            )
        )

    def test_spot_signal_matches_leveraged_long_score_and_flow_filters(self):
        now = 200.0
        with patch.object(paper_spot, "trade_history", {}), patch.object(
            paper_spot, "volume_by_symbol", {"ETH/USDT": 3_000_000.0}
        ):
            self._set_trades("ETH/USDT", now, 1.0, False)
            result = paper_spot.get_60s_stats("ETH/USDT", now)

        self.assertEqual(result["side"], "LONG")
        self.assertTrue(result["aligned"])
        self.assertEqual(result["trades"], paper_spot.MIN_TRADES_60S)
        self.assertGreaterEqual(result["score"], paper_spot.MIN_SCORE)

    def test_spot_trade_feed_normalizes_and_records_exchange_trades(self):
        with patch.object(paper_spot, "raw_to_unified", {"ETHUSDT": "ETH/USDT"}), patch.object(
            paper_spot, "universe", ["ETH/USDT"]
        ), patch.object(paper_spot, "trade_history", {}), patch.object(
            paper_spot, "positions", {}
        ):
            for index in range(20):
                paper_spot.process_trade(
                    "ETHUSDT",
                    100.0 + index * 0.01,
                    2.0,
                    200_000 + index * 3_000,
                    False,
                )
            recorded = list(paper_spot.trade_history["ETH/USDT"])
            signal = paper_spot.get_60s_stats("ETH/USDT", 260.0)

        self.assertEqual(len(recorded), 20)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertGreater(signal["score"], 0.0)

    def test_websocket_subscription_resolves_unified_symbols_to_raw_ids(self):
        thread_instance = MagicMock()
        with patch.object(paper_spot, "unified_to_raw", {"ETH/USDT": "ETHUSDT"}), patch.object(
            paper_spot, "ws_started", set()
        ), patch.object(
            paper_spot.threading, "Thread", return_value=thread_instance
        ) as thread_factory:
            paper_spot.start_websockets_for(["ETH/USDT"])
            paper_spot.start_websockets_for(["ETH/USDT"])

        thread_factory.assert_called_once()
        self.assertEqual(thread_factory.call_args.kwargs["args"][0], ["ETHUSDT"])
        thread_instance.start.assert_called_once()

    def test_spot_signal_does_not_make_shorts_actionable(self):
        now = 200.0
        with patch.object(paper_spot, "trade_history", {}), patch.object(
            paper_spot, "volume_by_symbol", {"ETH/USDT": 3_000_000.0}
        ):
            self._set_trades("ETH/USDT", now, -1.0, True)
            result = paper_spot.get_60s_stats("ETH/USDT", now)

        self.assertEqual(result["side"], "SHORT")
        self.assertTrue(result["aligned"])


class UniverseTests(unittest.TestCase):
    def test_universe_contains_liquid_active_spot_usdt_pairs_by_volume(self):
        class FakeExchange:
            @staticmethod
            def load_markets():
                return {
                    "BTC/USDT": {"active": True, "spot": True, "quote": "USDT", "base": "BTC"},
                    "ETH/USDT": {"active": True, "spot": True, "quote": "USDT", "base": "ETH"},
                    "USDC/USDT": {"active": True, "spot": True, "quote": "USDT", "base": "USDC"},
                    "ADA/USDT": {"active": True, "spot": True, "quote": "USDT", "base": "ADA"},
                    "SOL/USDT": {"active": False, "spot": True, "quote": "USDT", "base": "SOL"},
                    "BTC/USDT:USDT": {"active": True, "spot": False, "quote": "USDT", "base": "BTC"},
                }

            @staticmethod
            def fetch_tickers():
                return {
                    "BTC/USDT": {"quoteVolume": 9_000_000, "last": 100, "percentage": 2},
                    "ETH/USDT": {"quoteVolume": 7_000_000, "last": 50, "percentage": 8},
                    "USDC/USDT": {"quoteVolume": 20_000_000, "last": 1, "percentage": 1},
                    "ADA/USDT": {"quoteVolume": 4_000_000, "last": 0.5, "percentage": 30},
                    "SOL/USDT": {"quoteVolume": 15_000_000, "last": 100, "percentage": -2},
                    "BTC/USDT:USDT": {"quoteVolume": 30_000_000, "last": 100, "percentage": 40},
                }

        with patch.object(paper_spot, "exchange", FakeExchange()), patch.object(
            paper_spot, "universe", []
        ), patch.object(
            paper_spot, "volume_by_symbol", {}
        ), patch.object(paper_spot, "change_by_symbol", {}), patch.object(
            paper_spot, "raw_to_unified", {}
        ), patch.object(
            paper_spot, "unified_to_raw", {}
        ), patch.object(paper_spot, "start_websockets_for"):
            paper_spot.update_universe()
            selected_universe = list(paper_spot.universe)
            selected_volumes = dict(paper_spot.volume_by_symbol)
            selected_raw_symbols = dict(paper_spot.raw_to_unified)
            selected_unified_symbols = dict(paper_spot.unified_to_raw)
            with patch.object(paper_spot, "MAX_UNIVERSE", 1):
                paper_spot.update_universe()
                top_one = list(paper_spot.universe)

        self.assertEqual(selected_universe, ["ADA/USDT", "ETH/USDT"])
        self.assertEqual(top_one, ["ADA/USDT"])
        self.assertEqual(selected_volumes, {
            "ADA/USDT": 4_000_000,
            "ETH/USDT": 7_000_000,
        })
        self.assertEqual(selected_raw_symbols["ADAUSDT"], "ADA/USDT")
        self.assertEqual(selected_raw_symbols["ETHUSDT"], "ETH/USDT")
        self.assertEqual(selected_unified_symbols["ADA/USDT"], "ADAUSDT")


class PaperExecutionTests(unittest.TestCase):
    class FakeExchange:
        @staticmethod
        def amount_to_precision(_symbol, quantity):
            return f"{quantity:.8f}"

        @staticmethod
        def market(symbol):
            return {"limits": {"cost": {"min": 5.0}}}

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.state_path = Path(self.temp_directory.name) / "state.json"
        self.csv_path = Path(self.temp_directory.name) / "trades.csv"
        self.patches = [
            patch.object(paper_spot, "STATE_FILE", self.state_path),
            patch.object(paper_spot, "TRADE_LOG_FILE", self.csv_path),
            patch.object(paper_spot, "exchange", self.FakeExchange()),
        ]
        for active_patch in self.patches:
            active_patch.start()
        self.addCleanup(self.temp_directory.cleanup)
        self.addCleanup(self._stop_patches)

        paper_spot.main_balance = paper_spot.START_BALANCE
        paper_spot.positions = {}
        paper_spot.stats = {
            "opened": 0, "closed": 0, "wins": 0, "losses": 0,
            "realized_pnl": 0.0, "total_fees": 0.0,
        }
        paper_spot.recent_closed = []
        paper_spot.trade_id_counter = 0
        paper_spot.initialize_csv()

    def _stop_patches(self):
        for active_patch in reversed(self.patches):
            active_patch.stop()

    @staticmethod
    def _candidate(symbol="ETH/USDT", side="LONG", price=100.0):
        return {
            "symbol": symbol,
            "side": side,
            "price": price,
            "score": 85.0,
            "momentum": 1.0,
            "volume_60s": 20_000.0,
            "trades": 80,
            "buy_pressure": 0.7,
        }

    def test_paper_buy_and_sell_update_cash_and_trade_log(self):
        candidate = self._candidate()
        self.assertFalse(paper_spot.open_position(self._candidate(side="SHORT")))
        self.assertTrue(paper_spot.open_position(candidate))
        self.assertEqual(len(paper_spot.positions), 1)
        position = paper_spot.positions["ETH/USDT"]
        self.assertEqual(position["trade_type"], "TOP30")
        self.assertLessEqual(position["position_value"], paper_spot.POSITION_VALUE)
        self.assertAlmostEqual(
            paper_spot.unrealized_pnl({
                **position,
                "current_price": position["tp_price"],
            }),
            paper_spot.TAKE_PROFIT_USD,
            places=6,
        )
        self.assertAlmostEqual(
            paper_spot.unrealized_pnl({
                **position,
                "current_price": position["stop_price"],
            }),
            -paper_spot.STOP_LOSS_USD,
            places=6,
        )
        expected_equity = paper_spot.START_BALANCE + paper_spot.unrealized_pnl(position)
        self.assertAlmostEqual(paper_spot.equity(), expected_equity, places=6)

        self.assertTrue(paper_spot.close_position(
            "ETH/USDT", position["tp_price"], "TP"
        ))

        self.assertEqual(paper_spot.positions, {})
        self.assertGreater(paper_spot.main_balance, paper_spot.START_BALANCE)
        self.assertEqual(paper_spot.stats["closed"], 1)
        with self.csv_path.open("r", newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["close_reason"], "TP")
        self.assertAlmostEqual(float(rows[0]["net_pnl"]), paper_spot.TAKE_PROFIT_USD, places=6)

    def test_initialize_csv_adds_header_to_existing_headerless_trade_rows(self):
        record = {
            field: str(index)
            for index, field in enumerate(paper_spot.CSV_FIELDS)
        }
        record.update({
            "time": "2026-10-17T17:09:44+00:00",
            "trade_id": "2",
            "symbol": "ETH/USDT",
            "side": "LONG",
            "trade_type": "SECOND",
        })
        with self.csv_path.open("w", newline="", encoding="utf-8") as file:
            csv.writer(file).writerow([record[field] for field in paper_spot.CSV_FIELDS])

        paper_spot.initialize_csv()

        with self.csv_path.open("r", newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["trade_id"], "2")
        self.assertEqual(rows[0]["symbol"], "ETH/USDT")

    def test_trading_continues_after_balance_grows_above_initial_balance(self):
        candidate = self._candidate()
        self.assertTrue(paper_spot.open_position(candidate))
        position = paper_spot.positions["ETH/USDT"]
        paper_spot.close_position("ETH/USDT", position["tp_price"], "TP")

        self.assertGreater(paper_spot.main_balance, paper_spot.START_BALANCE)
        self.assertTrue(paper_spot.open_position(self._candidate("ADA/USDT")))

    def test_trading_continues_after_loss_below_initial_balance(self):
        self.assertTrue(paper_spot.open_position(self._candidate()))
        position = paper_spot.positions["ETH/USDT"]
        paper_spot.close_position("ETH/USDT", position["stop_price"], "SL")

        self.assertLess(paper_spot.main_balance, paper_spot.START_BALANCE)
        self.assertTrue(paper_spot.open_position(self._candidate("ADA/USDT")))

    def test_position_closes_after_one_hour_at_current_market_price(self):
        self.assertTrue(paper_spot.open_position(self._candidate()))
        position = paper_spot.positions["ETH/USDT"]
        position["opened_at"] -= paper_spot.MAX_HOLDING_SECONDS + 1
        paper_spot.check_position_tick(position, position["entry_price"])

        self.assertNotIn("ETH/USDT", paper_spot.positions)
        self.assertEqual(paper_spot.recent_closed[0]["close_reason"], "TIME")


class DashboardTests(unittest.TestCase):
    def test_dashboard_serves_page_and_json_api(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            with patch.object(paper_spot, "STATE_FILE", state_path), patch.object(
                paper_spot, "main_balance", paper_spot.START_BALANCE
            ), patch.object(paper_spot, "HTML_PORT", 0):
                server = paper_spot.start_dashboard()
                self.addCleanup(server.server_close)
                self.addCleanup(server.shutdown)
                base_url = f"http://127.0.0.1:{server.server_port}"

                with urlopen(base_url + "/api", timeout=2) as response:
                    data = json.loads(response.read().decode("utf-8"))
                with urlopen(base_url + "/", timeout=2) as response:
                    page = response.read().decode("utf-8")

        self.assertIn("positions", data)
        self.assertTrue(data["can_open_position"])
        self.assertEqual(data["position_value"], 20.0)
        self.assertIn("PAPER MODE", page)


if __name__ == "__main__":
    unittest.main()
