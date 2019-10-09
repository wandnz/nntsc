import unittest
import mock
from libnntsc.parsers.amp_traceroute_pathlen import AmpTraceroutePathlenParser

class TestTraceroutePathlenParser(unittest.TestCase):
    testdata = [{
        # lost measurements
        "target": "lost.example.com",
        "address": "192.0.2.1",
        "length": 5,
        "error_type": None,
        "error_code": None,
        "packet_size": 84,
        "random": False,
        "ip": True,
        "as": False,
        "dscp": "Default",
        "hops": [{"rtt": None, "address": None, "as": None}] * 5,
    }, {
        # good measurements
        "target": "good.example.com",
        "address": "192.0.2.1",
        "length": 2,
        "error_type": None,
        "error_code": None,
        "packet_size": 84,
        "random": False,
        "ip": True,
        "as": False,
        "dscp": "Default",
        "hops": [{"rtt": 5000, "address": "203.0.113.254", "as": None},
                 {"rtt": 10000, "address": "192.0.2.1", "as": None}],
    }, {
        # failed to perform measurement
        "target": "doesnotexist.invalid",
        "address": "0.0.0.0",
        "length": None,
        "error_type": None,
        "error_code": None,
        "packet_size": 84,
        "random": False,
        "ip": True,
        "as": False,
        "dscp": "Default",
        "hops": [],
    }]

    expected = [{
        # lost.example.com
        "path_length": 0.5,
        "count": 1,
    }, {
        # foo.example.com
        "path_length": 2.0,
        "count": 1,
    }, {
        # doesnotexist.invalid
        "path_length": None,
        "count": 1,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))
        database.custom_insert.side_effect = [(0, 0)] * len(self.testdata)

        # create the parser under test
        parser = AmpTraceroutePathlenParser(database, influx)

        # run all the data through the parser
        parser.process_data(0, self.testdata, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_traceroute_pathlen", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
