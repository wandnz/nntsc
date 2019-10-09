import unittest
import mock
from libnntsc.parsers.amp_external import AmpExternalParser

class TestExternalParser(unittest.TestCase):
    testdata = [{
        "command": "foo",
        "results": [{
            # lost measurements
            "destination": "lost.example.com",
            "value": 0
        }]
    }, {
        "command": "foo",
        "results": [{
            # good measurements
            "destination": "good.example.com",
            "value": 12345
        }]
    }, {
        "command": "foo",
        "results": [{
            # failed to perform measurement
            "destination": "doesnotexist.invalid",
            "value": None
        }]
    }]

    expected = [{
        # lost.example.com
        "value": 0,
    }, {
        # foo.example.com
        "value": 12345,
    }, {
        # doesnotexist.invalid
        "value": None,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))
        database.custom_insert.side_effect = [(0, 0)] * len(self.testdata)

        # create the parser under test
        parser = AmpExternalParser(database, influx)

        # run all the data through the parser
        for data in self.testdata:
            parser.process_data(0, data, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_external", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
