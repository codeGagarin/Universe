import json

import unittest

from reg_api import adapt_json, api_request, get_sub_domains, _extract_subdomains


class TestRegAPI(unittest.TestCase):
    def test_adapt(self):
        test_input = {'p1': 1, 'p2': 'hello'}
        test_result = adapt_json(**test_input)
        ref_result = '{"p1": 1, "p2": "hello"}'
        self.assertEqual(test_result, ref_result)
        print(test_result)

    def test_api(self):
        response = api_request('nop')
        print(response)

    def test_get_sub_domains(self):
        print(get_sub_domains())

    def test_extract_subdomain(self):
        test_response = """
            {
               "answer" : {
                  "domains" : [
                     {
                        "dname" : "test.ru",
                        "result" : "success",
                        "rrs" : [
                           {
                              "content" : "111.222.111.222",
                              "prio" : "0",
                              "rectype" : "AAAA",
                              "state" : "A",
                              "subname" : "www"
                           }
                        ],
                        "service_id" : "12345",
                        "soa" : {
                           "minimum_ttl" : "12h",
                           "ttl" : "1d"
                        }
                     },
                     {
                        "dname" : "test.com",
                        "result" : "success",
                        "rrs" : [
                           {
                              "content" : "111.222.111.222",
                              "prio" : "0",
                              "rectype" : "A",
                              "state" : "A",
                              "subname" : "www"
                           }
                        ],
                        "service_id" : "12346",
                        "soa" : {
                           "minimum_ttl" : "12h",
                           "ttl" : "1d"
                        }
                     }
                  ]
               },
               "result" : "success"
            }        
        """
        test_result = _extract_subdomains(json.loads(test_response))
        reference_result = {'www': {'domain': 'test.com', 'subname': '111.222.111.222'}}

        self.assertEqual(test_result, reference_result)
        print(test_result)

    @staticmethod
    def tmp2(s: str):

        return ' '.join(sorted(s.split(' '), key=lambda word: sum(map(int, word))+int(word)/(10**(len(word)))))

    def test_temp(self):
        queue_time = self.tmp2
        pair = "103 123 4444 99 2000", "2000 103 123 4444 99"
        self.assertEqual(pair[1], queue_time(pair[0]))

class TestHackAPI(unittest.TestCase):
    def test_main(self):

        import json
        import http.client as httplib
        import time
        import socket

        def header(url, path='/', method='HEAD'):
            headers = {}
            response = {}
            user_agent = "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:35.0) Gecko/20100101 Firefox/35.0"
            try:
                conn = httplib.HTTPConnection(url)
                conn.putrequest(method, path)
                conn.putheader("User-Agent", user_agent)
                conn.endheaders()
                res = conn.getresponse()
                conn.close()
                for item in res.getheaders():
                    headers.update({item[0]: item[1]})

                response = {'status': {'code': res.status, 'reason': res.reason}, 'http_headers': headers}
                response = json.dumps(response, indent=4, separators=(',', ': '))
            except:
                response = {}
            return response

        def resolve(target):
            hostname = ''
            aliaslist = []
            ipaddrlist = []
            code = ''
            header_response = {}
            iplist = []
            response = {}
            # zonetransfer_json = {}

            time_start, time_end = time.time()

            try:
                soc = socket.gethostbyname_ex(target)
                # zonetransfer_json = json.loads(zonetransfer.zonetransfer(target))

                if soc:
                    hostname = soc[0]
                    aliaslist = soc[1]
                    ipaddrlist = soc[2]

                    '''
                    check for http headers
                    '''
                    try:
                        header_response = json.loads(header(target))
                        code = header_response['status']['code']
                    except:
                        header_response = {}
                        code = ''

                    if hostname != target:
                        header_response = json.loads(header(hostname))

                    time_end = time.time()
            except:
                time_end = time.time()

            response_time = str(time_end - time_start)

            response = {'target': target, 'hostname': hostname,
                        'alias': aliaslist, 'ipaddress': ipaddrlist,
                        'status': code, 'response_time': response_time,
                        'http_response': header_response}  # , 'zonetransfer': zonetransfer_json}

            response = json.dumps(response, indent=4, separators=(',', ': '))
            return response

        if hasattr(socket, 'setdefaulttimeout'):
            socket.setdefaulttimeout(5)

        import dns.resolver, dns.query, dns.zone

        def zonetransfer(target):
            zonetransfer_list = []
            my_resolver = dns.resolver.Resolver()
            my_resolver.timeout = 2.0
            my_resolver.lifetime = 2.0
            try:
                answers = my_resolver.query(target, 'NS')
            except:
                response = {'enabled': False, 'list': []}
                response = json.dumps(response, indent=4, separators=(',', ': '))
                return response

            ip_from_nslist = []
            for name_server in answers:
                name_server = str(name_server).rstrip('.')
                try:
                    ip_from_nslist.append(socket.gethostbyname(name_server))
                except socket.gaierror:  # skip non resolvable name server
                    pass

            for ip_from_ns in ip_from_nslist:
                zone = False

                try:
                    zone = dns.zone.from_xfr(dns.query.xfr(ip_from_ns, target, timeout=1))
                except:
                    pass

                if zone:
                    for name, node in zone.nodes.items():
                        rdataset = node.rdatasets
                        for record in rdataset:
                            name = str(name)
                            if name != '@' and name != '*':
                                zonetransfer_list.append(name + '.' + target)

            if zonetransfer_list:
                zonetransfer_list = [item.lower() for item in zonetransfer_list]
                zonetransfer_list = list(set(zonetransfer_list))
                response = {'enabled': True, 'list': zonetransfer_list}
                response = json.dumps(response, indent=4, separators=(',', ': '))
                return response
            else:
                response = {'enabled': False, 'list': []}
                response = json.dumps(response, indent=4, separators=(',', ': '))
                return response

        print(zonetransfer('station-hotels.ru'))


if __name__ == 'main':
    unittest.main()