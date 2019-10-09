import unittest
import mock
from libnntsc.parsers.amp_throughput import AmpThroughputParser

class TestThroughputParser(unittest.TestCase):
    testdata = [{
        # lost measurement
        "target": "lost.example.com",
        "address": "192.0.2.1",
        "schedule": "T10",
        "write_size": 131072,
        "dscp": "Default",
        "protocol": "default",
        "results": [{
            "duration": 10000,
            "runtime": 10000,
            "bytes": 0,
            "direction": "out",
            "tcpreused": False,
        }]
    }, {
        # good measurement
        "target": "good.example.com",
        "address": "192.0.2.254",
        "schedule": "T10",
        "write_size": 131072,
        "dscp": "Default",
        "protocol": "default",
        "results": [{
            "duration": 10000,
            "runtime": 10000,
            "bytes": 40000,
            "direction": "out",
            "tcpreused": False,
        }]
    }, {
        # failed measurement
        "target": "doesnotexist.invalid",
        "address": "0.0.0.0",
        "schedule": "T10",
        "write_size": 131072,
        "dscp": "Default",
        "protocol": "default",
        "results": [{
            "duration": 10000,
            "runtime": None,
            "bytes": None,
            "direction": "out",
            "tcpreused": False,
        }]
    }]

    expected = [{
        # lostexample.com
        "packets": None,
        "rate": 0.0,
        "runtime": 10000,
        "bytes": 0,
        "address": "192.0.2.1",
    }, {
        # good.example.com
        "packets": None,
        "rate": 4.0,
        "runtime": 10000,
        "bytes": 40000,
        "address": "192.0.2.254",
    }, {
        # doesnotexist.invalid
        "packets": None,
        "rate": None,
        "runtime": None,
        "bytes": None,
        "address": "0.0.0.0",
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))

        # create the parser under test
        parser = AmpThroughputParser(database, influx)

        # run all the data through the parser
        for data in self.testdata:
            parser.process_data(0, data, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_throughput", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
