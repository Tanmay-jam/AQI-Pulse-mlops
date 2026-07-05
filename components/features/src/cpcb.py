"""CPCB National AQI calculation (India).

AQI = max over pollutants of the pollutant's sub-index, where each
sub-index linearly interpolates the pollutant's time-averaged
concentration through CPCB breakpoint segments:

    I = I_lo + (I_hi - I_lo) / (C_hi - C_lo) * (C - C_lo)

Averaging windows (handled by the caller): 24 h for PM2.5/PM10/NO2/SO2,
8 h for CO/O3. Units: µg/m³ everywhere except CO in mg/m³.

CPCB defines the 401-500 band as open-ended ("250+" etc.); the final
C_hi below extends the previous segment's slope so interpolation stays
linear, and anything beyond it clamps to 500.

Reporting rule (CPCB): an AQI is only published when at least
MIN_POLLUTANTS sub-indices are available and one of them is PM2.5 or PM10.
"""
from __future__ import annotations

MIN_POLLUTANTS = 3
PM_PARAMETERS = ("pm25", "pm10")

# AQI index bands shared by all pollutants.
_INDEX = [(0, 50), (50, 100), (100, 200), (200, 300), (300, 400), (400, 500)]

# Pollutant -> concentration breakpoints (C_lo of each band + final C_hi).
_CONC = {
    "pm25": [0, 30, 60, 90, 120, 250, 380],
    "pm10": [0, 50, 100, 250, 350, 430, 510],
    "no2":  [0, 40, 80, 180, 280, 400, 520],
    "so2":  [0, 40, 80, 380, 800, 1600, 2400],
    "co":   [0, 1, 2, 10, 17, 34, 51],       # mg/m³
    "o3":   [0, 50, 100, 168, 208, 748, 1287],
}


def sub_index(parameter: str, concentration: float | None) -> float | None:
    """Sub-index for one pollutant's averaged concentration (None passes through)."""
    # NaN (missing pollutant after the pivot) must not fall through the
    # comparisons below, all of which are False for NaN.
    if concentration is None or concentration != concentration or concentration < 0:
        return None
    bounds = _CONC[parameter]
    if concentration >= bounds[-1]:
        return 500.0
    for (i_lo, i_hi), c_lo, c_hi in zip(_INDEX, bounds, bounds[1:]):
        if concentration <= c_hi:
            return round(i_lo + (i_hi - i_lo) / (c_hi - c_lo) * (concentration - c_lo), 1)
    return 500.0  # unreachable, kept for safety


def aqi(sub_indices: dict[str, float | None]) -> tuple[float, str] | None:
    """(AQI, dominant pollutant) from per-pollutant sub-indices, or None
    if the CPCB reporting rule isn't met."""
    available = {p: si for p, si in sub_indices.items() if si is not None}
    if len(available) < MIN_POLLUTANTS:
        return None
    if not any(p in available for p in PM_PARAMETERS):
        return None
    dominant = max(available, key=available.get)
    return available[dominant], dominant
