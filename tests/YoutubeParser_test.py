import unittest
import mock
from libnntsc.parsers.amp_youtube import AmpYoutubeParser

class TestYoutubeParser(unittest.TestCase):
    testdata = [{
        # failed measurements
        "video": "abcde12345",
        "requested_quality": 0,
        "dscp": "Default",
        "useragent": "AMP YouTube test agent (Chromium 71.0.3578.98)",
        "max_runtime": 0,
        "title": "example live video that was missing codecs",
        "actual_quality": 0,
        "initial_buffering": 900,
        "playing_time": 0,
        "stall_time": 0,
        "stall_count": 0,
        "total_time": 1000,
        "pre_time": 100,
        "reported_duration": 0,
        "timeline": [],
    }, {
        # good measurements
        "video": "abcde12345",
        "requested_quality": 0,
        "dscp": "Default",
        "useragent": "AMP YouTube test agent (Chromium 71.0.3578.98)",
        "max_runtime": 0,
        "title": "example video that worked",
        "actual_quality": 0,
        "initial_buffering": 900,
        "playing_time": 3990,
        "stall_time": 10,
        "stall_count": 1,
        "total_time": 5000,
        "pre_time": 100,
        "reported_duration": 3990,
        "timeline": [], # ignored by libnntsc
    }]

    expected = [{
        # doesnotexist.invalid
        "total_time": 1000,
        "stall_count": 0,
        "playing_time": 0,
        "stall_time": 0,
        "pre_time": 100,
        "initial_buffering": 900,
    }, {
        # good.example.com
        "total_time": 5000,
        "stall_count": 1,
        "playing_time": 3990,
        "stall_time": 10,
        "pre_time": 100,
        "initial_buffering": 900,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))

        # create the parser under test
        parser = AmpYoutubeParser(database, influx)

        # run all the data through the parser
        for data in self.testdata:
            parser.process_data(0, data, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_youtube", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
