from includes import get_salah_id, ply_weeks_join_quality


def test_get_salah_id():
    sl_id = get_salah_id()
    assert sl_id == 283


def test_ply_weeks_join_quality():
    number_of_not_joined_week_rows = ply_weeks_join_quality()

    assert number_of_not_joined_week_rows == 0
