import unittest
import mock
from libnntsc.parsers.amp_fastping import AmpFastpingParser

class TestFastpingParser(unittest.TestCase):
    testdata = [{
        # lost measurements
        "destination": "lost.example.com",
        "address": "192.0.2.1",
        "packet_rate": 10,
        "packet_size": 84,
        "packet_count": 100,
        "dscp": "Default",
        "preprobe": False,
        "results": [{
            "runtime": 10000000,
            "rtt": None,
            "jitter": None,
        }]
    }, {
        # good measurement
        "destination": "good.example.com",
        "address": "198.51.100.254",
        "packet_rate": 10,
        "packet_size": 84,
        "packet_count": 100,
        "dscp": "Default",
        "preprobe": False,
        "results": [{
            "runtime": 10000000,
            "rtt": {
                "samples": 100,
                "percentiles": [
                    0.0, 0.1, 1.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0,
                    60.0, 70.0, 80.0, 90.0, 95.0, 99.0, 99.9, 100
                ],
            },
            "jitter": {
            },
        }]
    }, {
        "destination": "doesnotexist.invalid",
        "address": "0.0.0.0",
        "packet_rate": 10,
        "packet_size": 84,
        "packet_count": 100,
        "dscp": "Default",
        "preprobe": False,
        "results": [{
            # failed to perform measurement
            "runtime": None,
            "rtt": None,
            "jitter": None,
        }]
    }]

    expected = [{
        # lost.example.com
        "lossrate": 1.0,
        "median": None,
        "percentiles": [],
    }, {
        # good.example.com
        "lossrate": 0,
        "median": 50.0,
        "percentiles": [
            0.0, 0.1, 1.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0,
            60.0, 70.0, 80.0, 90.0, 95.0, 99.0, 99.9, 100
        ],
    }, {
        # doesnotexist.invalid
        "lossrate": None,
        "median": None,
        "percentiles": None,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))

        # create the parser under test
        parser = AmpFastpingParser(database, influx)

        # run all the data through the parser
        for data in self.testdata:
            parser.process_data(0, data, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_fastping", mock.ANY, 0, x,
                           {"percentiles": str})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
