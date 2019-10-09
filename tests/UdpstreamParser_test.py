import unittest
import mock
from libnntsc.parsers.amp_udpstream import AmpUdpstreamParser

class TestUdpstreamParser(unittest.TestCase):
    testdata = [{
        # lost measurements
        "target": "lost.example.com",
        "address": "192.0.2.1",
        "packet_size": 60,
        "packet_spacing": 20000,
        "packet_count": 100,
        "dscp": "Default",
        "rtt_samples": 1,
        "results": [{
            "direction": 0,
            "rtt": None,
            "jitter": None,
            "percentiles": [],
            "packets_received": 0,
            "loss_periods": [("loss", 100)],
            "loss_percent": 100,
            "voip": None,
        }]
    }, {
        # good measurement
        "target": "good.example.com",
        "address": "192.0.2.254",
        "packet_size": 60,
        "packet_spacing": 20000,
        "packet_count": 100,
        "dscp": "Default",
        "rtt_samples": 1,
        "results": [{
            "direction": 0,
            "rtt": {"mean": 50},
            "jitter": {"mean": 20, "minimum": 5, "maximum": 40},
            "percentiles": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            "packets_received": 100,
            "loss_periods": [],
            "loss_percent": 0,
            "voip": {"itu_mos": 5.0},
        }]
    }, {
        # failed measurement
        "target": "doesnotexist.invalid",
        "address": "0.0.0.0",
        "packet_size": 60,
        "packet_spacing": 20000,
        "packet_count": 21,
        "dscp": "Default",
        "rtt_samples": 1,
        "results": [{
            "direction": 0,
            "rtt": None,
            "jitter": None,
            "percentiles": [],
            "packets_received": None,
            "loss_periods": [],
            "loss_percent": None,
            "voip": None,
        }]
    }]

    expected = [{
        # lost.example.com
        "lossrate": 100,
        "packets_recvd": 0,
        "packets_sent": 100,
        "mean_jitter": None,
        "min_jitter": None,
        "max_jitter": None,
        "mean_rtt": None,
        "itu_mos": None,
        "jitter_percentile_10": None,
        "jitter_percentile_20": None,
        "jitter_percentile_30": None,
        "jitter_percentile_40": None,
        "jitter_percentile_50": None,
        "jitter_percentile_60": None,
        "jitter_percentile_70": None,
        "jitter_percentile_80": None,
        "jitter_percentile_90": None,
        "jitter_percentile_100": None,
        "address": "192.0.2.1",
    }, {
        # good.example.com
        "lossrate": 0,
        "packets_recvd": 100,
        "packets_sent": 100,
        "mean_jitter": 20,
        "min_jitter": 5,
        "max_jitter": 40,
        "mean_rtt": 50,
        "itu_mos": 5.0,
        "jitter_percentile_10": 10,
        "jitter_percentile_20": 20,
        "jitter_percentile_30": 30,
        "jitter_percentile_40": 40,
        "jitter_percentile_50": 50,
        "jitter_percentile_60": 60,
        "jitter_percentile_70": 70,
        "jitter_percentile_80": 80,
        "jitter_percentile_90": 90,
        "jitter_percentile_100": 100,
        "address": "192.0.2.254",
    }, {
        # doesnotexist.invalid
        "lossrate": None,
        "packets_recvd": None,
        "packets_sent": None,
        "mean_jitter": None,
        "min_jitter": None,
        "max_jitter": None,
        "mean_rtt": None,
        "itu_mos": None,
        "jitter_percentile_10": None,
        "jitter_percentile_20": None,
        "jitter_percentile_30": None,
        "jitter_percentile_40": None,
        "jitter_percentile_50": None,
        "jitter_percentile_60": None,
        "jitter_percentile_70": None,
        "jitter_percentile_80": None,
        "jitter_percentile_90": None,
        "jitter_percentile_100": None,
        "address": "0.0.0.0",
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))

        # create the parser under test
        parser = AmpUdpstreamParser(database, influx)

        # run all the data through the parser
        for data in self.testdata:
            parser.process_data(0, data, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_udpstream", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
