"""
Unified Symbols Service for NYSE and NASDAQ
Provides symbol lookup, validation, and metadata for Due Diligence
"""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class SymbolInfo:
    symbol: str
    name: str
    exchange: str
    market_category: str = ''
    etf: bool = False
    test_issue: bool = False
    financial_status: str = 'N'

class SymbolsService:
    """Service for managing unified NYSE/NASDAQ symbols dataset"""

    def __init__(self):
        self.symbols: Dict[str, SymbolInfo] = {}
        self.metadata: Dict = {}
        self.loaded = False

        # Path to unified symbols file
        self.data_dir = Path(__file__).parent / 'data'
        self.symbols_file = self.data_dir / 'unified_symbols.json'

    def load_symbols(self) -> bool:
        """Load symbols from unified dataset"""
        try:
            if not self.symbols_file.exists():
                logger.error(f"Symbols file not found: {self.symbols_file}")
                return False

            with open(self.symbols_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.metadata = data.get('metadata', {})
            symbols_data = data.get('symbols', {})

            # Convert to SymbolInfo objects
            self.symbols = {}
            for symbol, info in symbols_data.items():
                self.symbols[symbol.upper()] = SymbolInfo(
                    symbol=info['symbol'],
                    name=info['name'],
                    exchange=info['exchange'],
                    market_category=info.get('market_category', ''),
                    etf=info.get('etf', False),
                    test_issue=info.get('test_issue', False),
                    financial_status=info.get('financial_status', 'N')
                )

            self.loaded = True
            logger.info(f"✅ Loaded {len(self.symbols):,} symbols from unified dataset")
            logger.info(f"📊 NASDAQ: {self.metadata.get('nasdaq_count', 0):,}, NYSE: {self.metadata.get('nyse_count', 0):,}, ETFs: {self.metadata.get('etf_count', 0):,}")

            return True

        except Exception as e:
            logger.error(f"❌ Failed to load symbols: {e}")
            return False

    def get_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        """Get information for a specific symbol"""
        if not self.loaded:
            self.load_symbols()

        return self.symbols.get(symbol.upper())

    def is_valid_symbol(self, symbol: str) -> bool:
        """Check if a symbol is valid and tradeable"""
        info = self.get_symbol_info(symbol)
        return info is not None and not info.test_issue

    def search_symbols(self, query: str, limit: int = 20) -> List[SymbolInfo]:
        """Search symbols by ticker or company name"""
        if not self.loaded:
            self.load_symbols()

        query = query.upper().strip()
        if not query:
            return []

        matches = []

        # Exact symbol match first
        if query in self.symbols:
            matches.append(self.symbols[query])

        # Symbol starts with query
        for symbol_info in self.symbols.values():
            if symbol_info.symbol != query and symbol_info.symbol.startswith(query):
                matches.append(symbol_info)
                if len(matches) >= limit:
                    break

        # Company name contains query
        if len(matches) < limit:
            for symbol_info in self.symbols.values():
                if (symbol_info not in matches and
                    query in symbol_info.name.upper()):
                    matches.append(symbol_info)
                    if len(matches) >= limit:
                        break

        return matches[:limit]

    def get_symbols_by_exchange(self, exchange: str) -> List[SymbolInfo]:
        """Get all symbols for a specific exchange"""
        if not self.loaded:
            self.load_symbols()

        return [info for info in self.symbols.values()
                if info.exchange.upper() == exchange.upper()]

    def get_etf_symbols(self) -> List[SymbolInfo]:
        """Get all ETF symbols"""
        if not self.loaded:
            self.load_symbols()

        return [info for info in self.symbols.values() if info.etf]

    def get_stats(self) -> Dict:
        """Get dataset statistics"""
        if not self.loaded:
            self.load_symbols()

        return {
            **self.metadata,
            'loaded_symbols': len(self.symbols),
            'data_file': str(self.symbols_file)
        }

    def validate_ticker_list(self, tickers: List[str]) -> Tuple[List[str], List[str]]:
        """Validate a list of tickers, return (valid, invalid) lists"""
        if not self.loaded:
            self.load_symbols()

        valid = []
        invalid = []

        for ticker in tickers:
            if self.is_valid_symbol(ticker):
                valid.append(ticker.upper())
            else:
                invalid.append(ticker)

        return valid, invalid

# Global instance
symbols_service = SymbolsService()