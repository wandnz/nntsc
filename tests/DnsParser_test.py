import unittest
import mock
from libnntsc.parsers.amp_dns import AmpDnsParser

class TestDnsParser(unittest.TestCase):
    testdata = {
        "query": "example.com",
        "query_type": "A",
        "query_class": "IN",
        "udp_payload_size": 4096,
        "recurse": True,
        "dnssec": False,
        "nsid": False,
        "dscp": "Default",
        "results": [{
            # lost measurement
            "destination": "lost.example.com",
            "instance": "lost.example.com",
            "address": "192.0.2.1",
            "rtt": None,
            "query_len": 40,
            "response_size": None,
            "total_answer": None,
            "total_authority": None,
            "total_additional": None,
            "flags": {},
            "ttl": None,
            "nsid_bytes": None,
            "rrsig": False,
        }, {
            # good measurement
            "destination": "good.example.com",
            "instance": "good.example.com",
            "address": "198.51.100.254",
            "rtt": 1000,
            "query_len": 40,
            "response_size": 56,
            "total_answer": 1,
            "total_authority": 0,
            "total_additional": 1,
            "flags": {"rd": True, "tc": False, "aa": False, "opcode": 0,
                "qr": True, "rcode": 0, "cd": False, "ad": False, "ra": True},
            "ttl": 0,
            "nsid_bytes": None,
            "rrsig": False,
        }, {
            # failed to perform measurement
            "destination": "doesnotexist.invalid",
            "instance": "doesnotexist.invalid",
            "address": "0.0.0.0",
            "rtt": None,
            "query_len": None,
            "response_size": None,
            "total_answer": None,
            "total_authority": None,
            "total_additional": None,
            "flags": {},
            "ttl": None,
            "nsid_bytes": None,
            "rrsig": False,
        }]
    }

    expected = [{
        # lost.example.com
        #"nsid_bytes": None,
        "query_len": 40,
        "total_answer": None,
        "total_authority": None,
        "total_additional": None,
        "rtt": None,
        "response_size": None,
        "ttl": None,
        "requests": 1,
        "lossrate": 1.0,
        #"rrsig": False,
        "flag_aa": None,
        "flag_qr": None,
        "flag_ra": None,
        "flag_rd": None,
        "flag_tc": None,
        "flag_ad": None,
        "flag_cd": None,
        "rcode": None,
        "opcode": None,
    }, {
        # good.example.com
        #"nsid_bytes": None,
        "query_len": 40,
        "total_answer": 1,
        "total_authority": 0,
        "total_additional": 1,
        "rtt": 1000,
        "response_size": 56,
        "ttl": 0,
        "requests": 1,
        "lossrate": 0.0,
        #"rrsig": False,
        "flag_aa": False,
        "flag_qr": True,
        "flag_ra": True,
        "flag_rd": True,
        "flag_tc": False,
        "flag_ad": False,
        "flag_cd": False,
        "rcode": 0,
        "opcode": 0,
    }, {
        # doesnotexist.invalid
        #"nsid_bytes": None,
        "query_len": None,
        "total_answer": None,
        "total_authority": None,
        "total_additional": None,
        "rtt": None,
        "response_size": None,
        "ttl": None,
        "requests": 0,
        "lossrate": None,
        #"rrsig": False,
        "flag_aa": None,
        "flag_qr": None,
        "flag_ra": None,
        "flag_rd": None,
        "flag_tc": None,
        "flag_ad": None,
        "flag_cd": None,
        "rcode": None,
        "opcode": None,
    }]

    def test_process_data(self):
        # set up required postgresql and influx database mocks
        influx = mock.Mock()
        database = mock.Mock()
        database.insert_stream.side_effect = range(0, len(self.testdata))

        # create the parser under test
        parser = AmpDnsParser(database, influx)

        # run all the data through the parser
        parser.process_data(0, self.testdata, "source")

        # check what was written to the database
        calls = [mock.call("data_amp_dns", mock.ANY, 0, x, {})
                 for x in self.expected]
        influx.insert_data.assert_has_calls(calls, any_order=True)

if __name__ == "__main__":
    unittest.main()
