import unittest
import mock
from libnntsc.parsers.amp_http import AmpHttpParser

class TestHttpParser(unittest.TestCase):
    testdata = [{
        # failed measurements
        "url": "http://doesnotexist.invalid/",
        "duration": None,
        "bytes": None,
        "server_count": 1,
        "object_count": 0,
        "keep_alive": True,
        "max_connections": 24,
        "max_connections_per_server": 8,
        "max_persistent_connections_per_server": 2,
        "pipelining": False,
        "pipelining_maxrequests": 4,
        "caching": False,
        "dscp": "Default",
        "useragent": "AMP HTTP test agent",
        "proxy": None,
        "failed_object_count": 0,
        "servers": []
    }, {
        # good measurements
        "url": "http://good.example.com/",
        "duration": 200,
        "bytes": 606,
        "server_count": 1,
        "object_count": 1,
        "keep_alive": True,
        "max_connections": 24,
        "max_connections_per_server": 8,
        "max_persistent_connections_per_server": 2,
        "pipelining": False,
        "pipelining_maxrequests": 4,
        "caching": False,
        "dscp": "Default",
        "useragent": "AMP HTTP test agent",
        "proxy": None,
        "failed_object_count": 0,
        "servers": [], # ignored
    }]

    expected = [{
        # doesnotexist.invalid
        "bytes": None,
        "duration": None,
        "server_count": 1,
        "object_count": 0,
    }, {
        # good.example.com
        "bytes": 606,
        "duration": 200,
        "server_count": 1,
        "object_count": 1,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))

        # create the parser under test
        parser = AmpHttpParser(database, influx)

        # run all the data through the parser
        for data in self.testdata:
            parser.process_data(0, data, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_http", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
