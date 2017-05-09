""" Tests from Demo"""


def test_locations(results):
    """ Test locations from demo file """
    assert len(results.all()) == 13
    for r in results:
        if r.id == 1:
            assert r.name == "Demo"
        if r.id == 7:
            assert r.name == "District 2"
            assert r.parent_location == 4
        if r.id == 9:
            assert r.deviceid == "1,6"

def test_alert_status(labs, link):
    if labs == "unsure":
        assert "Ongoing" == link.data["status"]
    elif labs == "yes":
        assert "Confirmed" == link.data["status"]
    elif labs == "no":
        assert "Disregarded" == link.data["status"]
