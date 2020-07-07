import unittest
import mock
from libnntsc.parsers.amp_sip import AmpSipParser

class TestSipParser(unittest.TestCase):
    testdata = [{
        # lost measurements
        "uri": "sip:user@lost.example.com",
        "hostname": "lost.example.com",
        "address": "192.0.2.1",
        "filename": "default.wav",
        "repeat": True,
        "max_duration": 30,
        "dscp": "Default",
        "proxy": "lost.example.com",
        "results": [{
            "time_till_first_response": None,
            "time_till_connected": None,
            "duration": None,
            "rtt": None,
            "rx": None,
            "tx": None,
        }]
    }, {
        # good measurement
        "uri": "sip:user@good.example.com",
        "address": "192.0.2.254",
        "filename": "default.wav",
        "repeat": True,
        "max_duration": 30,
        "dscp": "Default",
        "proxy": "good.example.com",
        "results": [{
            "time_till_first_response": 100,
            "time_till_connected": 150,
            "duration": 10000,
            "rtt": {"maximum": 60, "minimum": 40, "mean": 50, "sd": 3},
            "rx": {
                "packets": 50000,
                "bytes": 3000000,
                "lost": 50,
                "discarded": 0,
                "reordered": 5,
                "duplicated": 10,
                "jitter": {},
                "loss": {},
                "mos": {"itu_mos": 3.8},
                },
            "tx": {
                "packets": 60000,
                "bytes": 4000000,
                "lost": 60,
                "discarded": 10,
                "reordered": 15,
                "duplicated": 20,
                "jitter": {},
                "loss": {},
                "mos": {"itu_mos": 2.8},
                },
        }]
    }, {
        # failed measurement
        "uri": "sip:doesnotexist.invalid",
        "address": "0.0.0.0",
        "filename": "default.wav",
        "repeat": True,
        "max_duration": 30,
        "dscp": "Default",
        "proxy": "doesnotexist.invalid",
        "results": [{
            "time_till_first_response": None,
            "time_till_connected": None,
            "duration": None,
            "rtt": None,
            "rx": None,
            "tx": None,
        }]
    }]

    expected = [{
        # lost.example.com rx
        "response_time": None,
        "connect_time": None,
        "duration": None,
        "rtt_max": None,
        "rtt_min": None,
        "rtt_mean": None,
        "rtt_sd": None,
        "packets": None,
        "bytes": None,
        "lost": None,
        "discarded": None,
        "reordered": None,
        "duplicated": None,
        "mos": None,
        #"address": "192.0.2.1",
        "unused": True,
    }, {
        # lost.example.com tx
        "response_time": None,
        "connect_time": None,
        "duration": None,
        "rtt_max": None,
        "rtt_min": None,
        "rtt_mean": None,
        "rtt_sd": None,
        "packets": None,
        "bytes": None,
        "lost": None,
        "discarded": None,
        "reordered": None,
        "duplicated": None,
        "mos": None,
        #"address": "192.0.2.1",
        "unused": True,
    }, {
        # good.example.com rx
        "response_time": 100,
        "connect_time": 150,
        "duration": 10000,
        "rtt_max": 60,
        "rtt_min": 40,
        "rtt_mean": 50,
        "rtt_sd": 3,
        "packets": 50000,
        "bytes": 3000000,
        "lost": 50,
        "discarded": 0,
        "reordered": 5,
        "duplicated": 10,
        "mos": 3.8,
        #"address": "192.0.2.254",
        "unused": True,
    }, {
        # good.example.com tx
        "response_time": 100,
        "connect_time": 150,
        "duration": 10000,
        "rtt_max": 60,
        "rtt_min": 40,
        "rtt_mean": 50,
        "rtt_sd": 3,
        "packets": 60000,
        "bytes": 4000000,
        "lost": 60,
        "discarded": 10,
        "reordered": 15,
        "duplicated": 20,
        "mos": 2.8,
        #"address": "192.0.2.254",
        "unused": True,
    }, {
        # doesnotexist.invalid rx
        "response_time": None,
        "connect_time": None,
        "duration": None,
        "rtt_max": None,
        "rtt_min": None,
        "rtt_mean": None,
        "rtt_sd": None,
        "packets": None,
        "bytes": None,
        "lost": None,
        "discarded": None,
        "reordered": None,
        "duplicated": None,
        "mos": None,
        #"address": "0.0.0.0",
        "unused": True,
    }, {
        # doesnotexist.invalid tx
        "response_time": None,
        "connect_time": None,
        "duration": None,
        "rtt_max": None,
        "rtt_min": None,
        "rtt_mean": None,
        "rtt_sd": None,
        "packets": None,
        "bytes": None,
        "lost": None,
        "discarded": None,
        "reordered": None,
        "duplicated": None,
        "mos": None,
        #"address": "0.0.0.0",
        "unused": True,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        # each data item creates two entries, one for each direction
        database.insert_stream.side_effect = range(0, len(self.testdata) * 2)

        # create the parser under test
        parser = AmpSipParser(database, influx)

        # run all the data through the parser
        for data in self.testdata:
            parser.process_data(0, data, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_sip", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
