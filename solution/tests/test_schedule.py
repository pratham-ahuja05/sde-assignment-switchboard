"""Window maths (R1). 12 Nov 2026 is a Thursday."""
from datetime import datetime, time, timedelta, timezone

import pytest

from solution.switchboard.config import ALL_DAYS, Window
from solution.switchboard.schedule import is_open, next_open

from .conftest import REG, campaign, ist

RENEWALS = campaign("r", "t", time(10), time(19), frozenset(range(6))).effective   # Mon-Sat
ZEN = campaign("z", "t", time(9, 30), time(18), frozenset(range(5))).effective      # Mon-Fri


@pytest.mark.parametrize("t,expected", [
    (ist(12, 9, 59, 59), False),
    (ist(12, 10, 0, 0), True),          # start is inclusive
    (ist(12, 18, 59, 59), True),
    (ist(12, 19, 0, 0), False),         # end is exclusive
    (ist(15, 12), False),               # Sunday 15 Nov
    (ist(14, 12), True),                # Saturday
])
def test_is_open_boundaries_and_days(t, expected):
    assert is_open(t, RENEWALS) is expected


def test_regulatory_window_clips_campaign_window():
    wide = campaign("w", "t", time(7), time(23), ALL_DAYS).effective
    assert wide.start == time(9) and wide.end == time(21)
    assert not is_open(ist(12, 8, 59), wide)
    assert not is_open(ist(12, 21, 0), wide)
    assert is_open(ist(12, 20, 59), wide)


def test_end_guard_stops_starts_just_before_close():
    assert is_open(ist(12, 18, 59, 30), RENEWALS)
    assert not is_open(ist(12, 18, 59, 30), RENEWALS, end_guard_s=60)
    assert next_open(ist(12, 18, 59, 30), RENEWALS, end_guard_s=60) == ist(13, 10)


def test_next_open_inside_window_is_now():
    t = ist(12, 12, 34, 56)
    assert next_open(t, RENEWALS) == t


def test_next_open_before_open_same_day():
    assert next_open(ist(12, 7), RENEWALS) == ist(12, 10)


def test_next_open_after_close_rolls_to_next_allowed_day():
    assert next_open(ist(12, 18), ZEN) == ist(13, 9, 30)        # Thu after close -> Fri
    assert next_open(ist(13, 18, 30), ZEN) == ist(16, 9, 30)    # Fri after close -> Mon (skip weekend)
    assert next_open(ist(14, 11), ZEN) == ist(16, 9, 30)        # Saturday midday -> Monday
    assert next_open(ist(14, 19), RENEWALS) == ist(16, 10)      # Sat after close -> Mon (no Sunday)


def test_utc_input_is_interpreted_in_ist():
    t = datetime(2026, 11, 12, 4, 0, tzinfo=timezone.utc)       # 09:30 IST
    assert not is_open(t, RENEWALS)
    assert next_open(t, RENEWALS) == ist(12, 10)
    assert is_open(t + timedelta(minutes=30), RENEWALS)


def test_naive_datetimes_are_refused():
    with pytest.raises(ValueError):
        is_open(datetime(2026, 11, 12, 12), RENEWALS)


def test_empty_window_fails_loudly():
    with pytest.raises(ValueError):
        next_open(ist(12, 12), Window(time(10), time(10), ALL_DAYS))


def test_real_fixture_windows(real_config):
    c = real_config.campaigns
    sim_start = ist(12, 17, 30)
    assert is_open(sim_start, c["c_acme_renewals"].effective)
    assert is_open(sim_start, c["c_zen_winback"].effective)
    assert next_open(ist(12, 18, 0), c["c_zen_winback"].effective) == ist(13, 9, 30)
    assert next_open(ist(12, 20, 30), c["c_acme_feedback"].effective) == ist(13, 11)
    assert REG.start == time(9)
