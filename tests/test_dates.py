from datetime import datetime
from zoneinfo import ZoneInfo

from pipeline.normalise.dates import make_occurrence, nightlife_date, parse_dt

BERLIN = ZoneInfo("Europe/Berlin")


def test_iso_with_offset():
    dt, date_only = parse_dt("2026-06-13T23:00:00+02:00")
    assert not date_only and dt.hour == 23


def test_naive_iso_localised_cest():
    # June = CEST (+02:00); fixed-offset localisation would get this wrong half the year
    dt, _ = parse_dt("2026-06-13T20:00:00")
    assert dt.utcoffset().total_seconds() == 2 * 3600


def test_naive_iso_localised_cet_winter():
    dt, _ = parse_dt("2026-12-13T20:00:00")
    assert dt.utcoffset().total_seconds() == 1 * 3600


def test_dst_spring_transition_weekend():
    # clocks jump 29 March 2026: 21:00 local must still map to +02:00 after transition
    dt, _ = parse_dt("2026-03-29T21:00:00")
    assert dt.utcoffset().total_seconds() == 2 * 3600


def test_dst_autumn_transition_weekend():
    dt, _ = parse_dt("2026-10-25T21:00:00")
    assert dt.utcoffset().total_seconds() == 1 * 3600


def test_date_only_flagged():
    dt, date_only = parse_dt("2026-06-14")
    assert date_only and dt.hour == 0


def test_german_date_with_uhr():
    dt, date_only = parse_dt("Sa., 14.06.2026, 20 Uhr")
    assert not date_only and (dt.day, dt.month, dt.hour) == (14, 6, 20)


def test_german_date_without_time_is_date_only():
    dt, date_only = parse_dt("14.06.2026")
    assert date_only and dt.day == 14


def test_german_month_name():
    dt, _ = parse_dt("14. Juni 2026, 19:30")
    assert (dt.month, dt.hour, dt.minute) == (6, 19, 30)


def test_day_month_not_swapped():
    dt, _ = parse_dt("05.06.2026")  # 5 June, not 6 May
    assert (dt.day, dt.month) == (5, 6)


def test_nightlife_date_club_start_after_midnight():
    # Saturday 01:00 belongs to Friday night
    local = datetime(2026, 6, 14, 1, 0, tzinfo=BERLIN)
    assert nightlife_date(local) == "2026-06-13"


def test_nightlife_date_evening_start():
    local = datetime(2026, 6, 13, 23, 0, tzinfo=BERLIN)
    assert nightlife_date(local) == "2026-06-13"


def test_occurrence_club_night_is_not_range():
    # Sat 23:59 → Sun 08:00 is one event, not a multi-day range
    occ, is_range, _ = make_occurrence("2026-06-13T23:59:00", "2026-06-14T08:00:00")
    assert occ and not is_range and occ.ends_at_utc is not None


def test_occurrence_exhibition_is_range():
    occ, is_range, rng = make_occurrence("2026-06-10", "2026-08-30")
    assert is_range and rng == ("2026-06-10", "2026-08-30")


def test_end_before_start_rolls_forward():
    # listing dropped the day from the end time: 23:00 – 04:00
    occ, is_range, _ = make_occurrence("2026-06-13T23:00:00", "2026-06-13T04:00:00")
    assert not is_range and occ.ends_at_utc == "2026-06-14T02:00:00Z"  # 04:00 CEST next day


def test_no_end_stays_null():
    occ, _, _ = make_occurrence("2026-06-13T20:00:00")
    assert occ.ends_at_utc is None
