import unittest
import mock
from libnntsc.parsers.amp_icmp import AmpIcmpParser

class TestIcmpParser(unittest.TestCase):
    testdata = [{
        # lost measurements
        "rtt": None,
        "loss": 1,
        "random": False,
        "target": "lost.example.com",
        "address": "192.0.2.1",
        "ttl": None,
        "error_type": None,
        "error_code": None,
        "dscp": "Default",
        "packet_size": 84
    }, {
        # lost measurements
        "rtt": None,
        "loss": 1,
        "random": False,
        "target": "good.example.com",
        "address": "192.0.2.254",
        "ttl": None,
        "error_type": None,
        "error_code": None,
        "dscp": "Default",
        "packet_size": 84
    }, {
        # good measurements
        "rtt": 1000,
        "loss": 0,
        "random": False,
        "target": "good.example.com",
        "address": "198.51.100.254",
        "ttl": 0,
        "error_type": 0,
        "error_code": 0,
        "dscp": "Default",
        "packet_size": 84
    }, {
        # good measurements
        "rtt": 2000,
        "loss": 0,
        "random": False,
        "target": "good.example.com",
        "address": "203.0.113.254",
        "ttl": 0,
        "error_type": 0,
        "error_code": 0,
        "dscp": "Default",
        "packet_size": 84
    }, {
        # failed to perform measurement
        "rtt": None,
        "loss": None,
        "random": False,
        "target": "doesnotexist.invalid",
        "address": "0.0.0.0",
        "ttl": None,
        "error_type": None,
        "error_code": None,
        "dscp": "Default",
        "packet_size": 84
    }]

    expected = [{
        # lost.example.com
        "loss": 1,
        "rtts": [None],
        "median": None,
        "results": 1,
        "lossrate": 1.0,
        "packet_size": 84
    }, {
        # good.example.com
        "loss": 1,
        "rtts": [1000, 2000, None],
        "median": 1500,
        "results": 3,
        "lossrate": 0.3333333333333333,
        "packet_size": 84
    }, {
        # doesnotexist.invalid
        "loss": None,
        "rtts": [],
        "median": None,
        "results": None,
        "lossrate": None,
        "packet_size": 84,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))

        # create the parser under test
        parser = AmpIcmpParser(database, influx)

        # run all the data through the parser
        parser.process_data(0, self.testdata, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_icmp", mock.ANY, 0, x, {"rtts": str})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
