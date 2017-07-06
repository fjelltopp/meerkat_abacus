""" Tests from Demo"""


def test_locations(results):
    """ Test locations from demo file """
    assert len(results.all()) == 21

def test_alert_status(labs, link):
    if labs == "unsure":
        assert "Ongoing" == link.data["status"]
    elif labs == "yes":
        assert "Confirmed" == link.data["status"]
    elif labs == "no":
        assert "Disregarded" == link.data["status"]
