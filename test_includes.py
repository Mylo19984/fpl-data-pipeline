from includes import get_sala_id

def test_get_sala_id():

    sl_id = get_sala_id()
    assert sl_id == 283
