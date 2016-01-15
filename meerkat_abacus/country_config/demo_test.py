""" Tests from Demo"""


def test_locations(results):
    """ Test locations from demo file """
    assert len(results.all()) == 11
    for r in results:
        if r.id == 1:
            assert r.name == "Demo"
        if r.id == 5:
            assert r.name == "District 2"
            assert r.parent_location == 2
        if r.id == 7:
            assert r.deviceid == "1,6"
