"""Exchange connector module."""

import time
from datetime import UTC, datetime
from typing import Any

import ccxt
import polars as pl
from loguru import logger

from dl_datalake.ingest.pipeline import IngestPipeline


class ExchangeConnector:
    """Connects to exchanges via CCXT."""

    def __init__(
        self,
        exchange_id: str = "binance",
        market_type: str = "spot",
        exchange_instance: Any = None,  # noqa: ANN401
    ) -> None:
        """Initialize ExchangeConnector.

        Args:
            exchange_id: Exchange ID.
            market_type: Market type (spot, future, swap, etc).
            exchange_instance: An optional pre-initialized CCXT exchange instance.
        """
        self.exchange_id = exchange_id.lower()
        self.market_type = market_type

        if exchange_instance:
            self.exchange = exchange_instance
        else:
            exchange_class = getattr(ccxt, self.exchange_id)
            self.exchange = exchange_class(
                {
                    "enableRateLimit": True,
                    "options": {"defaultType": self.market_type},
                },
            )

        self.pipeline = IngestPipeline()

    def get_all_symbols(self) -> list[str]:
        """Get all active symbols from the exchange.

        Returns:
            List of symbol names.
        """
        markets = self.exchange.load_markets()
        return [symbol for symbol, market in markets.items() if market["active"]]

    def download_ohlcv(  # noqa: C901, PLR0912, PLR0915
        self,
        symbol: str,
        timeframe: str = "1m",
        since_days: int = 30,  # noqa: ARG002
        progress_callback: Any = None,  # noqa: ANN401
        start_date: str | None = None,
    ) -> int:
        """Download OHLCV data with smart incremental updates.

        Args:
            symbol: Trading symbol.
            timeframe: Candle timeframe.
            since_days: Number of days back (used if no existing data).
            progress_callback: Callback for progress updates.
            start_date: ISO date string to start from.

        Returns:
            Number of saved candles.
        """
        # 0. Ensure markets are loaded and normalized
        self.exchange.load_markets()
        if symbol not in self.exchange.markets:
            # Try to find by id if exact match fails
            found = False
            for s, m in self.exchange.markets.items():
                if m.get("id") == symbol:
                    symbol = s
                    found = True
                    break
            if not found:
                logger.error(f"Symbol {symbol} not found on {self.exchange_id}")
                return 0

        # 1. Determine smart 'since' timestamp
        manifest_symbol = symbol.replace("/", "_").replace(":", "_")
        entries = self.pipeline.manifest.list_entries(
            exchange=self.exchange_id,
            symbol=manifest_symbol,
            data_type="raw",
        )

        valid_times = [e.time_to for e in entries if e.time_to]
        last_time_to = max(valid_times) if valid_times else None

        if last_time_to:
            # Resume exactly from the next millisecond of the last known timestamp.
            since = last_time_to + 1
            logger.info(
                f"Incremental update for {symbol}: resuming from {datetime.fromtimestamp(since/1000, UTC)}",
            )

        elif start_date:
            # User specified a start date
            try:
                dt = datetime.fromisoformat(start_date)
                # Ensure UTC
                dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

                since = int(dt.timestamp() * 1000)
                logger.info(f"Manual start date for {symbol}: {dt}")

            except Exception as e:  # noqa: BLE001
                logger.error(
                    "Failed to parse start_date",
                    start_date=start_date,
                    error=e,
                    symbol=symbol,
                    exchange=self.exchange_id,
                )
                since = 0
        else:
            # Full history mode: start with a probe
            logger.info(
                f"Full history download for {symbol}: probing for listing date...",
            )
            since = 0
            
            probe_attempts = 0
            MAX_PROBE_ATTEMPTS = 3
            
            while probe_attempts < MAX_PROBE_ATTEMPTS:
                try:
                    # Try fetching just 1 candle from 'the beginning'
                    probe = self.exchange.fetch_ohlcv(symbol, timeframe, since=0, limit=1)
                    if probe:
                        since = probe[0][0]
                        logger.info(
                            f"Listing date found for {symbol}: {datetime.fromtimestamp(since/1000, UTC)}",
                        )
                    else:
                        # Some exchanges (Bybit/OKX) need a more recent starting point
                        # Try probing 5 years back
                        five_years_ago = int(
                            (datetime.now(UTC).timestamp() - (5 * 365 * 24 * 3600)) * 1000,
                        )
                        probe = self.exchange.fetch_ohlcv(
                            symbol,
                            timeframe,
                            since=five_years_ago,
                            limit=1,
                        )
                        if probe:
                            since = probe[0][0]
                            logger.info(
                                f"Listing date found (fallback probe) for {symbol}: {datetime.fromtimestamp(since/1000, UTC)}",
                            )
                        else:
                            logger.warning(
                                f"Could not find any OHLCV data for {symbol} via probing.",
                            )
                            return 0
                    break # Success
                except ccxt.DDoSProtection as e:
                    probe_attempts += 1
                    logger.warning(
                        f"Rate limit hit during probe (429) for {symbol}. "
                        f"Wait 30s... (Attempt {probe_attempts}/{MAX_PROBE_ATTEMPTS})"
                    )
                    if probe_attempts >= MAX_PROBE_ATTEMPTS:
                        logger.error(f"Max probe attempts reached for {symbol} due to rate limits.")
                        return 0
                    time.sleep(30)
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        "Probing listing date failed",
                        symbol=symbol,
                        exchange=self.exchange_id,
                        error=e,
                    )
                    since = 0  # Fallback to 0 if probe crashes
                    break

        all_ohlcv = []
        now = self.exchange.milliseconds()
        total_saved = 0

        consecutive_empty = 0
        MAX_EMPTY_JUMPS = 10
        failed_requests = 0
        MAX_FAILED_REQUESTS = 5
        prev_last_ts = None

        # Calculate timeframe in ms for continuity checks
        timeframe_ms = self.exchange.parse_timeframe(timeframe) * 1000

        while since < now:
            try:
                # Explicitly request 1000 candles (Binance max) to minimize network roundtrips
                logger.info(
                    f"Fetching OHLCV for {symbol} ({self.exchange_id}) since {since}, limit=1000",
                )
                ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit=1000)
                
                if not ohlcv:
                    consecutive_empty += 1
                    if consecutive_empty > MAX_EMPTY_JUMPS:
                        logger.warning(
                            f"Exceeded max consecutive empty responses ({MAX_EMPTY_JUMPS}) for {symbol}. Stopping.",
                        )
                        break

                    # Gap Jump: skip the requested timeframe chunk
                    # 1000 candles * timeframe_seconds * 1000 ms
                    jump_ms = 1000 * timeframe_ms
                    logger.warning(
                        f"Empty response for {symbol}, jumping forward {jump_ms/1000/60:.1f}m. (Attempt {consecutive_empty}/{MAX_EMPTY_JUMPS})",
                    )
                    since += jump_ms
                    # If we jumped, we expect a discontinuity, so resetting prev_last_ts might avoid false warnings 
                    # OR we want to know about it? Usually we want to know.
                    # But since we explicitly jumped, we KNOW there is a gap.
                    # Let's keep prev_last_ts. If the next chunk comes, it will match 'since', but 'prev_last_ts' will be old.
                    # Actually, if we jump, 'since' increases. 
                    # If we simply proceed, the check below:
                    # expected_ts = prev_last_ts + timeframe_ms. 
                    # actual_ts will be 'since' (jumping ahead). so actual >> expected.
                    # So we will log a warning. which is CORRECT. We want to see gaps in logs.
                    continue
                
                # Success - reset counters
                consecutive_empty = 0
                failed_requests = 0

                # Continuity Check
                if prev_last_ts is not None:
                    expected_ts = prev_last_ts + timeframe_ms
                    actual_ts = ohlcv[0][0]
                    # Allow 1ms slop for potential rounding? usually exact integers.
                    if actual_ts > expected_ts:
                        gap_size = actual_ts - expected_ts
                        logger.warning(
                            f"Discontinuity detected for {symbol}! Gap of {gap_size/1000}s. "
                            f"Prev candle end: {datetime.fromtimestamp(prev_last_ts/1000, UTC)}, "
                            f"Current candle start: {datetime.fromtimestamp(actual_ts/1000, UTC)}"
                        )
                    elif actual_ts < expected_ts:
                        logger.warning(
                            f"Overlap detected for {symbol}! "
                            f"Prev candle end: {datetime.fromtimestamp(prev_last_ts/1000, UTC)}, "
                            f"Current candle start: {datetime.fromtimestamp(actual_ts/1000, UTC)}"
                        )

                prev_last_ts = ohlcv[-1][0]

                all_ohlcv.extend(ohlcv)  # pyright: ignore

                # Check if we got stuck (since not advancing)
                last_ts = ohlcv[-1][0]
                if last_ts <= since:
                    # Some exchanges return the same candle if requested with its own TS
                    # Advance by one timeframe unit (mili-seconds)
                    since = last_ts + timeframe_ms
                else:
                    since = last_ts + 1

                # Update 'now' occasionally to catch up with latest candles during long downloads
                if len(all_ohlcv) % 10000 == 0:
                    now = self.exchange.milliseconds()

                # Incremental saving: save every chunky bit of candles
                incremental_chunk = 5000
                if len(all_ohlcv) >= incremental_chunk:
                    total_saved += self._save_ohlcv_chunk(all_ohlcv, symbol, timeframe)
                    all_ohlcv = []  # Clear memory
                    if progress_callback:
                        progress_callback(total_saved)

            except ccxt.DDoSProtection:
                # Specific handling for Rate Limits (429)
                failed_requests += 1
                logger.warning(
                    f"Rate limit hit (429) for {symbol}. Waiting 30s before retry... "
                    f"(Attempt {failed_requests}/{MAX_FAILED_REQUESTS})"
                )
                if failed_requests > MAX_FAILED_REQUESTS:
                    logger.error("Too many rate limit hits. Aborting.")
                    break
                time.sleep(30)
                continue

            except Exception:  # noqa: BLE001
                failed_requests += 1
                logger.error(
                    f"Failed to fetch OHLCV (Attempt {failed_requests}/{MAX_FAILED_REQUESTS})",
                    symbol=symbol,
                    exchange=self.exchange_id,
                    exc_info=True,
                )
                if failed_requests > MAX_FAILED_REQUESTS:
                    logger.error("Too many failed requests. Aborting.")
                    break
                time.sleep(1)  # Backoff
                continue

        # Save remaining candles
        if all_ohlcv:
            total_saved += self._save_ohlcv_chunk(all_ohlcv, symbol, timeframe)
            if progress_callback:
                progress_callback(total_saved)

        return total_saved

    def _save_ohlcv_chunk(self, data: list[Any], symbol: str, timeframe: str) -> int:
        """Helper to write a chunk of OHLCV data to lake and manifest."""
        if not data:
            return 0

        df = pl.DataFrame(
            data,
            schema=["ts", "open", "high", "low", "close", "volume"],
            orient="row",
        )

        written_data = list(
            self.pipeline.writer.write_ohlc(
                df,
                self.exchange_id,
                self.market_type,
                symbol,
                timeframe,
            ),
        )

        # Sanitize symbol for manifest consistency
        sanitized_symbol = symbol.replace("/", "_").replace(":", "_")

        for path, t_min, t_max in written_data:
            self.pipeline.manifest.add_entry(
                exchange=self.exchange_id,
                market=self.market_type,
                symbol=sanitized_symbol,
                path=str(path),
                type="raw",
                time_from=t_min,
                time_to=t_max,
                metadata_json=f'{{"timeframe": "{timeframe}"}}',
            )
        return len(df)

    def download_funding_rates(
        self,
        symbol: str,
        _since_days: int | None = None,
    ) -> int:
        """Download funding rates.

        Args:
            symbol: Trading symbol.
            since_days: Deprecated. Now downloads full history.

        Returns:
            Number of records saved.
        """
        is_derivative = any(
            d in self.market_type.lower()
            for d in ["future", "swap", "linear", "inverse", "derivative"]
        )
        if not is_derivative:
            return 0

        since = 0
        # Determine smart 'since' timestamp from manifest
        manifest_symbol = symbol.replace("/", "_").replace(":", "_")
        entries = self.pipeline.manifest.list_entries(
            exchange=self.exchange_id,
            symbol=manifest_symbol,
            data_type="alt",
        )
        # Filter for funding entries
        valid_times = [
            e.time_to
            for e in entries
            if e.time_to and (e.metadata_json and "funding" in e.metadata_json)
        ]
        last_time_to = max(valid_times) if valid_times else None

        if last_time_to:
            # Add 1ms to avoid duplicates, though funding rates are usually sparse
            since = last_time_to + 1
            logger.info(
                f"Incremental funding update for {symbol}: starting from {datetime.fromtimestamp(since/1000, UTC)}",
            )
        else:
            logger.info(f"Full funding history download for {symbol}")
        try:
            funding = self.exchange.fetch_funding_rate_history(symbol, since)
        except Exception:  # noqa: BLE001
            logger.error(
                "Failed to fetch funding rates",
                symbol=symbol,
                exchange=self.exchange_id,
                exc_info=True,
            )
            return 0

        if not funding:
            return 0

        df = pl.DataFrame(funding)
        # Convert timestamp for partitioning
        df = df.with_columns(  # pyright: ignore
            pl.from_epoch("timestamp", time_unit="ms").dt.date().alias("date"),
        )

        for (date,), day_df in df.partition_by(
            "date",
            as_dict=True,
        ).items():  # pyright: ignore
            write_df = day_df.drop("date")
            path, t_min, t_max = self.pipeline.writer.write_table(
                write_df,
                self.exchange_id,
                self.market_type,
                symbol,
                "alt",
                "funding",
                date,
            )
            # Sanitize symbol for manifest consistency
            sanitized_symbol = symbol.replace("/", "_").replace(":", "_")
            self.pipeline.manifest.add_entry(
                exchange=self.exchange_id,
                market=self.market_type,
                symbol=sanitized_symbol,
                path=str(path),
                type="alt",
                time_from=t_min,
                time_to=t_max,
                metadata_json='{"category": "funding"}',
            )
        return len(df)
