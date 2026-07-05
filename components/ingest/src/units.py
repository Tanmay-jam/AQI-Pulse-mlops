"""Unit normalization — every pollutant lands in Postgres in µg/m³.

OpenAQ reports the same parameter in different units depending on the
station (e.g. no2 in µg/m³, ppm *and* ppb — parameter ids 5/7/15). Mixing
those in one column would silently corrupt every average downstream, so
gas concentrations are converted to µg/m³ here, before validation.

Conversion: µg/m³ = ppb × M / 24.45  (molar volume at 25 °C, 1 atm — the
reference conditions used by CPCB/US-EPA breakpoint tables). Particulates
(pm25/pm10) are mass concentrations already and only appear as µg/m³.
"""
from __future__ import annotations

CANONICAL_UNIT = "µg/m³"

# Molar masses (g/mol) for the gases we ingest.
MOLAR_MASS: dict[str, float] = {
    "no2": 46.01,
    "so2": 64.07,
    "co": 28.01,
    "o3": 48.00,
}

MOLAR_VOLUME = 24.45  # L/mol at 25 °C, 1 atm

# Unit -> multiplier to ppb, for the gas units OpenAQ actually uses.
_TO_PPB = {"ppb": 1.0, "ppm": 1000.0}

# Spellings OpenAQ uses for the canonical unit.
_UGM3_ALIASES = {"µg/m³", "ug/m3", "µg/m3", "ug/m³"}

_MGM3_ALIASES = {"mg/m³", "mg/m3"}

# OpenAQ mislabels CPCB CO sensors: CPCB reports CO in mg/m³ (its native CO
# unit) but OpenAQ's sensor metadata says "ppb" while passing the numbers
# through unchanged — so a true 1.1 mg/m³ arrives as "1.1 ppb". Real ambient
# CO is ~100–3000 ppb, so any "ppb" CO at or below this threshold cannot be
# genuine ppb and is treated as mg/m³. (Confirmed against the same station's
# legacy µg/m³ twin sensor reporting the same magnitude ×1000.)
CO_MISLABELED_PPB_MAX = 50.0


def to_ugm3(parameter: str, value: float, unit: str | None) -> float | None:
    """Convert one measurement to µg/m³; None if the unit is unconvertible."""
    if unit in _UGM3_ALIASES:
        return value
    if unit in _MGM3_ALIASES:
        return value * 1000.0
    if parameter == "co" and unit == "ppb" and 0 <= value <= CO_MISLABELED_PPB_MAX:
        return value * 1000.0  # mislabelled mg/m³, not ppb — see note above
    if unit in _TO_PPB and parameter in MOLAR_MASS:
        ppb = value * _TO_PPB[unit]
        return ppb * MOLAR_MASS[parameter] / MOLAR_VOLUME
    return None


def normalize(records: list[dict]) -> list[dict]:
    """Return records with value/unit rewritten to µg/m³.

    Records whose unit can't be converted (unknown unit, or a gas unit on
    a particulate) are dropped — better a smaller clean batch than a
    poisoned column. Input records are not mutated.
    """
    out: list[dict] = []
    for rec in records:
        value = rec.get("value")
        if value is None:
            out.append(dict(rec))  # let the validator's null logic decide
            continue
        converted = to_ugm3(rec.get("parameter", ""), value, rec.get("unit"))
        if converted is None:
            continue
        out.append({**rec, "value": converted, "unit": CANONICAL_UNIT})
    return out
