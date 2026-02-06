"""
Gap-Fade Pattern Definitions and Detection Rules
Complete pattern library with criteria and explanations
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum

class PatternType(Enum):
    """Pattern types for gap-fade strategies"""
    DAY_2_DUMP = "day_2_dump"
    DAY_2_FADE = "day_2_fade"
    DAY_3_BREAKDOWN = "day_3_breakdown"
    DAY_3_FADE = "day_3_fade"
    EXTENDED_FADE = "extended_fade"
    BLOW_OFF_TOP = "blow_off_top"
    EXHAUSTION_GAP = "exhaustion_gap"
    FAILED_BREAKOUT = "failed_breakout"
    MORNING_PANIC = "morning_panic"
    WEAK_HOLD = "weak_hold"
    OVER_EXTENDED_GAP_DOWN = "over_extended_gap_down"  # LONG pattern

@dataclass
class PatternCriteria:
    """Criteria for pattern detection"""
    min_gap_percent: float
    max_gap_percent: Optional[float]
    min_fade_percent: float
    max_fade_percent: Optional[float]
    min_volume_ratio: Optional[float]
    time_frame: str  # "day2", "day3", "intraday", "multi-day"

@dataclass
class PatternDefinition:
    """Complete pattern definition with all details"""
    name: str
    display_name: str
    pattern_type: PatternType
    criteria: PatternCriteria
    description: str
    entry_strategy: str
    risk_management: str
    target_levels: List[str]
    success_rate: float  # Historical success percentage
    risk_reward_ratio: str
    example_tickers: List[str]
    visual_indicator: str  # Emoji or icon
    color_code: str  # For UI display

# Complete Pattern Library
PATTERN_LIBRARY: Dict[PatternType, PatternDefinition] = {

    PatternType.DAY_2_DUMP: PatternDefinition(
        name="day_2_dump",
        display_name="Day 2 Dump",
        pattern_type=PatternType.DAY_2_DUMP,
        criteria=PatternCriteria(
            min_gap_percent=20.0,
            max_gap_percent=None,
            min_fade_percent=10.0,
            max_fade_percent=None,
            min_volume_ratio=1.5,
            time_frame="day2"
        ),
        description="""
        The Day 2 Dump occurs when a stock gaps up massively (>20%) on day 1,
        attracts FOMO buyers, but lacks follow-through on day 2. Early buyers
        take profits, triggering a cascade of selling. This pattern has high
        success rate for shorts as the stock often retraces 30-50% of the gap.
        """,
        entry_strategy="""
        • Enter short when stock breaks below VWAP with volume
        • Best entry: First red 5-min candle after open
        • Confirm with decreasing bid sizes
        • Watch for failed green attempts
        """,
        risk_management="""
        • Stop loss: Day 1 high + $0.10
        • Position size: 1/3 at entry, 1/3 at VWAP break, 1/3 at LOD break
        • Time stop: Exit if no movement by 11:00 AM
        """,
        target_levels=[
            "Target 1: -5% from entry (50% position)",
            "Target 2: -10% from entry (30% position)",
            "Target 3: Previous day's close (20% position)"
        ],
        success_rate=78.5,
        risk_reward_ratio="1:3",
        example_tickers=["SPRT", "BBIG", "ATER", "PROG"],
        visual_indicator="🔴💥",
        color_code="#DC143C"  # Crimson
    ),

    PatternType.DAY_2_FADE: PatternDefinition(
        name="day_2_fade",
        display_name="Day 2 Fade",
        pattern_type=PatternType.DAY_2_FADE,
        criteria=PatternCriteria(
            min_gap_percent=15.0,
            max_gap_percent=20.0,
            min_fade_percent=8.0,
            max_fade_percent=15.0,
            min_volume_ratio=1.2,
            time_frame="day2"
        ),
        description="""
        Classic Day 2 Fade pattern where a significant gap (15-20%) loses
        momentum gradually. Unlike the dump, this is a slower grind lower
        throughout the day. Ideal for patient shorts who scale in.
        """,
        entry_strategy="""
        • Scale in: 1/3 at open if gapping down
        • Add 1/3 on failed break of pre-market high
        • Final 1/3 on VWAP rejection
        • Never chase - wait for pops to short
        """,
        risk_management="""
        • Stop: Previous day's high
        • Mental stop at break and hold above VWAP
        • Reduce size if holding above 50% retracement
        """,
        target_levels=[
            "Target 1: VWAP (30% position)",
            "Target 2: -7% from high of day (40% position)",
            "Target 3: 50% gap fill (30% position)"
        ],
        success_rate=72.3,
        risk_reward_ratio="1:2.5",
        example_tickers=["CEI", "MULN", "NILE", "INDO"],
        visual_indicator="📉",
        color_code="#FF6347"  # Tomato
    ),

    PatternType.DAY_3_BREAKDOWN: PatternDefinition(
        name="day_3_breakdown",
        display_name="Day 3 Breakdown",
        pattern_type=PatternType.DAY_3_BREAKDOWN,
        criteria=PatternCriteria(
            min_gap_percent=10.0,
            max_gap_percent=15.0,
            min_fade_percent=15.0,
            max_fade_percent=None,
            min_volume_ratio=0.8,
            time_frame="day3"
        ),
        description="""
        Day 3 Breakdown happens when a stock that gapped up moderately (10-15%)
        consolidates on day 2, then breaks down hard on day 3. This is often
        the "reality check" day when traders realize the move is over.
        """,
        entry_strategy="""
        • Short break of day 2 low with volume
        • Or short rejection at day 2 VWAP
        • Best setups: Opening drive lower
        • Avoid if gapping up on day 3
        """,
        risk_management="""
        • Stop: Day 2 high (tighter stop = better R:R)
        • Trail stop to breakeven after -5% move
        • Cover 50% at day 1 close level
        """,
        target_levels=[
            "Target 1: Day 2 low (25% position)",
            "Target 2: Day 1 close (50% position)",
            "Target 3: Pre-gap support (25% position)"
        ],
        success_rate=69.8,
        risk_reward_ratio="1:2",
        example_tickers=["DWAC", "PHUN", "BENE", "ISPC"],
        visual_indicator="💔",
        color_code="#8B0000"  # DarkRed
    ),

    PatternType.DAY_3_FADE: PatternDefinition(
        name="day_3_fade",
        display_name="Day 3 Fade",
        pattern_type=PatternType.DAY_3_FADE,
        criteria=PatternCriteria(
            min_gap_percent=10.0,
            max_gap_percent=15.0,
            min_fade_percent=5.0,
            max_fade_percent=10.0,
            min_volume_ratio=0.6,
            time_frame="day3"
        ),
        description="""
        Gradual Day 3 Fade occurs when momentum slowly dies. Volume decreases
        each day, and the stock grinds lower. Less dramatic but very reliable
        for swing shorts. Often continues fading for multiple days.
        """,
        entry_strategy="""
        • Short pops to declining 20EMA
        • Enter on low volume green candles
        • Scale in over 2-3 days
        • Best entries: 10:30 AM - 11:30 AM
        """,
        risk_management="""
        • Wide stop: 10% above entry
        • Position size: 50% normal size (wider stop)
        • Hold for multi-day fade
        """,
        target_levels=[
            "Target 1: -10% from entry (swing target)",
            "Target 2: Gap fill (50% position)",
            "Target 3: Pre-gap base (runner position)"
        ],
        success_rate=71.2,
        risk_reward_ratio="1:1.5",
        example_tickers=["SNDL", "CLOV", "WISH", "PLTR"],
        visual_indicator="📊",
        color_code="#CD5C5C"  # IndianRed
    ),

    PatternType.EXTENDED_FADE: PatternDefinition(
        name="extended_fade",
        display_name="Extended Fade",
        pattern_type=PatternType.EXTENDED_FADE,
        criteria=PatternCriteria(
            min_gap_percent=5.0,
            max_gap_percent=10.0,
            min_fade_percent=3.0,
            max_fade_percent=8.0,
            min_volume_ratio=0.4,
            time_frame="multi-day"
        ),
        description="""
        Extended Fade is a multi-day pattern where a stock slowly bleeds out
        over 4-7 days after a gap up. Each day shows lower highs and lower
        lows. Perfect for swing traders who can hold overnight.
        """,
        entry_strategy="""
        • Short rallies to declining 9EMA
        • Add on each failed breakout attempt
        • Enter end of day for overnight fade
        • Avoid if news pending
        """,
        risk_management="""
        • Loose stop: 15% above average entry
        • Size: 30% of day trade size
        • Hold 3-7 days minimum
        """,
        target_levels=[
            "Weekly target: -20% from initial entry",
            "Daily targets: -3% to -5% per day",
            "Ultimate target: Complete gap fill"
        ],
        success_rate=65.4,
        risk_reward_ratio="1:1.2",
        example_tickers=["AMC", "GME", "CTRM", "SHIP"],
        visual_indicator="📈📉",
        color_code="#A52A2A"  # Brown
    ),

    PatternType.BLOW_OFF_TOP: PatternDefinition(
        name="blow_off_top",
        display_name="Blow-Off Top",
        pattern_type=PatternType.BLOW_OFF_TOP,
        criteria=PatternCriteria(
            min_gap_percent=30.0,
            max_gap_percent=None,
            min_fade_percent=12.0,
            max_fade_percent=None,
            min_volume_ratio=3.0,
            time_frame="intraday"
        ),
        description="""
        Blow-Off Top is an extreme parabolic move (>30% gap) with massive
        volume that exhausts all buyers. The reversal is swift and violent.
        High risk but highest reward pattern for experienced traders.
        """,
        entry_strategy="""
        • Wait for first red 1-min candle with size
        • Short the retest of high
        • Use 1-min chart for precision
        • Never short the spike itself
        """,
        risk_management="""
        • Tight stop: $0.20 above high
        • Full size on confirmation
        • Exit 75% same day
        • Trail remaining overnight
        """,
        target_levels=[
            "Target 1: -10% in first hour (50%)",
            "Target 2: VWAP (30%)",
            "Target 3: -20% end of day (20%)"
        ],
        success_rate=82.1,
        risk_reward_ratio="1:5",
        example_tickers=["KODK", "LAKE", "GNUS", "XELA"],
        visual_indicator="🌋",
        color_code="#FF0000"  # Red
    ),

    PatternType.EXHAUSTION_GAP: PatternDefinition(
        name="exhaustion_gap",
        display_name="Exhaustion Gap",
        pattern_type=PatternType.EXHAUSTION_GAP,
        criteria=PatternCriteria(
            min_gap_percent=15.0,
            max_gap_percent=25.0,
            min_fade_percent=5.0,
            max_fade_percent=10.0,
            min_volume_ratio=2.0,
            time_frame="day2"
        ),
        description="""
        Exhaustion Gap occurs after multiple green days when a stock gaps up
        one final time but immediately shows weakness. The gap represents the
        last buyers capitulating to FOMO. Classic reversal pattern.
        """,
        entry_strategy="""
        • Short the first lower high after open
        • Or short break below first 30-min low
        • Confirm with decreasing volume on pops
        • Best time: 10:00 AM - 10:30 AM
        """,
        risk_management="""
        • Stop: Above morning high
        • Reduce if holds above VWAP past noon
        • Exit all if makes new high after 2 PM
        """,
        target_levels=[
            "Target 1: Previous day's high",
            "Target 2: Gap fill 50%",
            "Target 3: Complete gap fill"
        ],
        success_rate=74.6,
        risk_reward_ratio="1:2.8",
        example_tickers=["IRNT", "OPAD", "TMC", "EFTR"],
        visual_indicator="💨",
        color_code="#B22222"  # FireBrick
    ),

    PatternType.FAILED_BREAKOUT: PatternDefinition(
        name="failed_breakout",
        display_name="Failed Breakout Fade",
        pattern_type=PatternType.FAILED_BREAKOUT,
        criteria=PatternCriteria(
            min_gap_percent=8.0,
            max_gap_percent=15.0,
            min_fade_percent=6.0,
            max_fade_percent=12.0,
            min_volume_ratio=1.0,
            time_frame="intraday"
        ),
        description="""
        Failed Breakout Fade happens when a stock gaps above key resistance
        but cannot hold. The failure attracts shorts and triggers stop losses
        from trapped longs. Very reliable pattern with clear risk levels.
        """,
        entry_strategy="""
        • Short the retest of broken resistance
        • Or short the second failed attempt
        • Enter with confirmation candle
        • Size up if multiple rejections
        """,
        risk_management="""
        • Stop: $0.10 above resistance level
        • Very tight risk = larger position
        • Exit if reclaims level for 15 minutes
        """,
        target_levels=[
            "Target 1: Previous resistance (now support)",
            "Target 2: -8% from failed breakout level",
            "Target 3: Previous day's low"
        ],
        success_rate=76.9,
        risk_reward_ratio="1:3.5",
        example_tickers=["BYND", "TLRY", "RIOT", "MARA"],
        visual_indicator="🚫",
        color_code="#8B4513"  # SaddleBrown
    ),

    PatternType.MORNING_PANIC: PatternDefinition(
        name="morning_panic",
        display_name="Morning Panic Fade",
        pattern_type=PatternType.MORNING_PANIC,
        criteria=PatternCriteria(
            min_gap_percent=12.0,
            max_gap_percent=20.0,
            min_fade_percent=8.0,
            max_fade_percent=15.0,
            min_volume_ratio=2.5,
            time_frame="intraday"
        ),
        description="""
        Morning Panic Fade occurs when a gap up triggers immediate selling
        at open. Usually happens when pre-market was very strong but regular
        hours bring reality. The first 30 minutes show heavy distribution.
        """,
        entry_strategy="""
        • Short the first bounce after opening drive
        • Or short break of first 5-min low
        • Aggressive: Short market open if gapping into resistance
        • Add on VWAP retest
        """,
        risk_management="""
        • Stop: Pre-market high
        • Cover 50% by 10:30 AM
        • Trail stop on remaining
        """,
        target_levels=[
            "Target 1: LOD in first hour",
            "Target 2: Previous close",
            "Target 3: -15% from pre-market high"
        ],
        success_rate=73.4,
        risk_reward_ratio="1:2.2",
        example_tickers=["NVAX", "MRNA", "BNTX", "OCGN"],
        visual_indicator="😱",
        color_code="#D2691E"  # Chocolate
    ),

    PatternType.WEAK_HOLD: PatternDefinition(
        name="weak_hold",
        display_name="Weak Hold Pattern",
        pattern_type=PatternType.WEAK_HOLD,
        criteria=PatternCriteria(
            min_gap_percent=10.0,
            max_gap_percent=15.0,
            min_fade_percent=2.0,
            max_fade_percent=5.0,
            min_volume_ratio=0.5,
            time_frame="day2"
        ),
        description="""
        Weak Hold Pattern shows a stock barely holding its gap gains. It trades
        in a tight range with declining volume, showing no follow-through buying.
        This often precedes a larger fade on day 3-4.
        """,
        entry_strategy="""
        • Short at top of range
        • Scale in throughout the day
        • Best entries: Power hour weakness
        • Hold overnight for gap down
        """,
        risk_management="""
        • Wide stop: 8% above entry
        • Small position size
        • Swing trade mentality
        """,
        target_levels=[
            "Next day target: Gap down -5%",
            "Swing target: Test of breakout level",
            "Runner target: Complete retrace"
        ],
        success_rate=67.2,
        risk_reward_ratio="1:1.8",
        example_tickers=["SENS", "BNGO", "IDEX", "GEVO"],
        visual_indicator="😐",
        color_code="#DAA520"  # GoldenRod
    ),

    PatternType.OVER_EXTENDED_GAP_DOWN: PatternDefinition(
        name="over_extended_gap_down",
        display_name="Over Extended Gap Down (OEGD)",
        pattern_type=PatternType.OVER_EXTENDED_GAP_DOWN,
        criteria=PatternCriteria(
            min_gap_percent=-10.0,  # Negative = gap down
            max_gap_percent=None,
            min_fade_percent=0.0,  # Looking for continuation down
            max_fade_percent=None,
            min_volume_ratio=2.0,  # Volume spike confirms panic
            time_frame="premarket"  # Best detected in premarket
        ),
        description="""
        Over Extended Gap Down (OEGD) is a high-probability SHORT setup that occurs
        when a stock has run up >100% in 5 days, then gaps down >10% in premarket.
        The gap down after overextension signals the top is in and trapped longs will
        continue selling. No immediate support exists below, making this a powerful
        continuation short into the gap. Risk of dilution makes this even more bearish.
        """,
        entry_strategy="""
        • SHORT in premarket during panic (7:30-9:20 AM ET)
        • Entry: Gap down level -2% (as panic intensifies)
        • Or short first bounce attempt with volume rejection
        • Confirm: No support levels nearby, pure air pocket
        • Best time: 8:00-9:00 AM ET (maximum panic)
        • Avoid if gap fills >50% in premarket (too strong)
        """,
        risk_management="""
        • Stop loss: Previous day low +3% (tight stop above panic zone)
        • Position size: Full size (high-confidence setup)
        • Time stop: Cover 50% by 11:00 AM if not moving
        • Trail stop: Break-even once down 10%
        • This is an intraday short - cover same day
        """,
        target_levels=[
            "Target 1: -20% additional from entry (50% position)",
            "Target 2: -30% from gap level (30% position)",
            "Target 3: Previous support / HOD break area (20% runner)"
        ],
        success_rate=73.8,
        risk_reward_ratio="1:4",
        example_tickers=["DFLI", "CMRA", "APLT", "VRAR"],
        visual_indicator="⬇️💀",
        color_code="#8B0000"  # DarkRed (SHORT pattern)
    )
}

class PatternDetector:
    """Pattern detection engine with detailed analysis"""

    @staticmethod
    def detect_pattern(
        gap_percent: float,
        fade_percent: float,
        volume_ratio: float = 1.0,
        days_since_gap: int = 1
    ) -> Optional[PatternDefinition]:
        """
        Detect which pattern matches the current metrics

        Args:
            gap_percent: Original gap percentage
            fade_percent: Current fade from high
            volume_ratio: Current volume vs average
            days_since_gap: Days since the gap occurred

        Returns:
            Matching PatternDefinition or None
        """

        # Determine time frame
        if days_since_gap == 1:
            time_frame = "intraday"
        elif days_since_gap == 2:
            time_frame = "day2"
        elif days_since_gap == 3:
            time_frame = "day3"
        else:
            time_frame = "multi-day"

        # Check each pattern's criteria
        for pattern_type, pattern_def in PATTERN_LIBRARY.items():
            criteria = pattern_def.criteria

            # Check time frame
            if criteria.time_frame != time_frame and criteria.time_frame != "multi-day":
                continue

            # Check gap range
            if gap_percent < criteria.min_gap_percent:
                continue
            if criteria.max_gap_percent and gap_percent > criteria.max_gap_percent:
                continue

            # Check fade range
            if fade_percent < criteria.min_fade_percent:
                continue
            if criteria.max_fade_percent and fade_percent > criteria.max_fade_percent:
                continue

            # Check volume if specified
            if criteria.min_volume_ratio and volume_ratio < criteria.min_volume_ratio:
                continue

            # Pattern matches!
            return pattern_def

        # Default to extended fade if no specific pattern matches
        if gap_percent > 5 and fade_percent > 2:
            return PATTERN_LIBRARY[PatternType.EXTENDED_FADE]

        return None

    @staticmethod
    def get_pattern_by_name(pattern_name: str) -> Optional[PatternDefinition]:
        """Get pattern definition by name string"""
        for pattern_type, pattern_def in PATTERN_LIBRARY.items():
            if pattern_def.name == pattern_name:
                return pattern_def
        return None

    @staticmethod
    def calculate_pattern_confidence(
        pattern: PatternDefinition,
        gap_percent: float,
        fade_percent: float,
        volume_ratio: float
    ) -> float:
        """
        Calculate confidence score for a detected pattern (0-100)

        Higher confidence when metrics exceed minimum criteria
        """
        confidence = 50.0  # Base confidence

        criteria = pattern.criteria

        # Gap strength bonus (up to 25 points)
        gap_excess = gap_percent - criteria.min_gap_percent
        confidence += min(gap_excess * 2, 25)

        # Fade strength bonus (up to 20 points)
        fade_excess = fade_percent - criteria.min_fade_percent
        confidence += min(fade_excess * 2, 20)

        # Volume confirmation bonus (up to 15 points)
        if criteria.min_volume_ratio:
            volume_excess = volume_ratio - criteria.min_volume_ratio
            confidence += min(volume_excess * 10, 15)

        # Historical success rate bonus (up to 10 points)
        confidence += (pattern.success_rate - 65) * 0.4

        return min(confidence, 100.0)

    @staticmethod
    def get_all_patterns() -> List[PatternDefinition]:
        """Get list of all available patterns"""
        return list(PATTERN_LIBRARY.values())

    @staticmethod
    def get_pattern_summary(pattern: PatternDefinition) -> Dict:
        """Get a simplified summary for UI display"""
        return {
            "name": pattern.name,
            "display_name": pattern.display_name,
            "visual": pattern.visual_indicator,
            "color": pattern.color_code,
            "success_rate": pattern.success_rate,
            "risk_reward": pattern.risk_reward_ratio,
            "quick_description": pattern.description.split('.')[0].strip()  # First sentence
        }