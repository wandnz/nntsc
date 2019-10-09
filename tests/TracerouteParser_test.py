import unittest
import mock
from libnntsc.parsers.amp_traceroute import AmpTracerouteParser

class TestTracerouteParser(unittest.TestCase):
    testdata = {
        "asn": {
            "table": "data_amp_astraceroute",
            "collection": "amp-astraceroute",
            "aggregation": {"aspath": "varchar[]"},
            "data": [{
                # lost measurements - ASN
                "target": "lost.example.com",
                "address": "192.0.2.1",
                "length": 5,
                "error_type": None,
                "error_code": None,
                "packet_size": 84,
                "random": False,
                "ip": False,
                "as": True,
                "dscp": "Default",
                "hops": [{"rtt": None, "address": None, "as": None}] * 5,
            }, {
                # good measurements - ASN
                "target": "good.example.com",
                "address": "198.51.100.254",
                "length": 2,
                "error_type": None,
                "error_code": None,
                "packet_size": 84,
                "random": False,
                "ip": False,
                "as": True,
                "dscp": "Default",
                "hops": [{"rtt": 5000, "address": "203.0.113.254", "as": 64496},
                {"rtt": 10000, "address": "198.51.100.254", "as": 64497}],
            }, {
                # failed to perform measurement - ASN
                "target": "doesnotexist.invalid",
                "address": "0.0.0.0",
                "length": None,
                "error_type": None,
                "error_code": None,
                "packet_size": 84,
                "random": False,
                "ip": False,
                "as": True,
                "dscp": "Default",
                "hops": [],
            }],
            "expected": [{
                # lost.example.com - ASN
                "errors": 0,
                "aspath_id": None,
                "packet_size": 84,
                "addresses": 1,
            }, {
                # foo.example.com - ASN
                "errors": 0,
                "aspath_id": 0,
                "packet_size": 84,
                "addresses": 1,
            }, {
                # doesnotexist.invalid - ASN
                "errors": None,
                "aspath_id": None,
                "packet_size": 84,
                "addresses": None,
            }]
        },

        "ip": {
            "table": "data_amp_traceroute",
            "collection": "amp_traceroute",
            "aggregation": {"hop_rtt": "integer[]"},
            "data": [{
                # lost measurements - IP
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
                # good measurements - IP
                "target": "good.example.com",
                "address": "198.51.100.254",
                "length": 2,
                "error_type": None,
                "error_code": None,
                "packet_size": 84,
                "random": False,
                "ip": True,
                "as": False,
                "dscp": "Default",
                "hops": [{"rtt": 5000, "address": "203.0.113.254", "as": None},
                {"rtt": 10000, "address": "198.51.100.254", "as": None}],
            }, {
                # failed to perform measurement - IP
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
            }],
            "expected": [{
                # lost.example.com - IP
                "aspath": None,
                "responses": None,
                "error_type": None,
                "error_code": None,
                "aspath_id": None,
                "path_id": 0,
                "packet_size": 84,
                "aspath_length": None,
                "length": 5,
                "hop_rtt": [None] * 5,
                "path": [None] * 5,
                "uniqueas": None,
            }, {
                # foo.example.com - IP
                "aspath": None,
                "responses": None,
                "error_type": None,
                "error_code": None,
                "aspath_id": None,
                "path_id": 0,
                "packet_size": 84,
                "aspath_length": None,
                "length": 2,
                "hop_rtt": [5000, 10000],
                "path": ["203.0.113.254", "198.51.100.254"],
                "uniqueas": None,
            }, {
                # doesnotexist.invalid - IP
                "aspath": None,
                "responses": None,
                "error_type": None,
                "error_code": None,
                "aspath_id": None,
                "path_id": 0, # an empty path still gets created in the database
                "packet_size": 84,
                "aspath_length": None,
                "length": None,
                "hop_rtt": [],
                "path": [],
                "uniqueas": None,
            }]
        }
    }

    def test_process_data(self):
        for flag, testdata in self.testdata.iteritems():
            # set up required postgresql database mock
            database = mock.Mock()
            database.insert_stream.side_effect = range(0, len(testdata["data"]))
            database.custom_insert.side_effect = [(0, 0)] * len(testdata["data"])

            # create the parser under test
            parser = AmpTracerouteParser(database)

            # run all the data through the parser
            parser.process_data(0, testdata["data"], "source")

            # check what was written to the database
            # TODO check the paths that got added too
            calls = [mock.call(testdata["table"], testdata["collection"],
                               mock.ANY, 0, x, testdata["aggregation"])
                     for x in testdata["expected"]]
            database.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
