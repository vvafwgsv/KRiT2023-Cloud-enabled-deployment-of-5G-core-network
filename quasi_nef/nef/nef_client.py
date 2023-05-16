import pycurl
import json
from nef_proxy_aux import *
import urllib.parse
from typing import Union


class BaseNfClient:
    @classmethod
    def send_query(cls, http_proxy: PycurlClient, url) -> Union[dict, None]:
        http_proxy.setopt(pycurl.URL, url)
        try:
            http_proxy.send_get_event()
        except InvalidQueryException as e:
            print(e)
            return None

        _response = http_proxy.get_http_response()

        if not _response:
            print(f'didnt receive any response from sbi: {http_proxy.get_targeted_nf()}')
        elif isinstance(_response, str):
            try:
                _response = json.loads(_response)
            except json.decoder.JSONDecodeError:
                _message = f'Failed to compile response into json: {_response}'
                return None

        return _response


class NrfClient(BaseNfClient):
    """"""
    __INSTANCE = None

    def __new__(cls, *args, **kwargs):
        if not cls.__INSTANCE:
            cls.__INSTANCE = super(NrfClient, cls).__new__(cls)
        return cls.__INSTANCE

    def __init__(self):
        """"""
        super().__init__()
        self.http_proxy = PycurlClient()

    def nrf_get_all_registered_nf(self):
        _nrf_sbi = 'http://192.168.58.110:16000/nnrf-nfm/v1/nf-instances'
        _nf_data: Dict[str, []] = dict()

        _response = self.send_query(self.http_proxy, _nrf_sbi)

        if not _response:
            print('Failed to acquire response')
            return

        self.http_proxy.get_targeted_nf()
        _nf_instances = list(map(lambda x: [value for _, value in x.items()], _response['_links']['items']))
        _nf_instances = list(map(lambda x: x[0], _nf_instances))
        print(_nf_instances)

        for link in _nf_instances:
            self.http_proxy.setopt(pycurl.URL, link)
            self.http_proxy.send_get_event()
            _data = self.http_proxy.get_http_response()
            if isinstance(_data, str):
                _data = json.loads(_data)
            if _data['nfType'] not in _nf_data.keys():
                _nf_data.update({_data['nfType']: {_data['nfInstanceId']: _data}})
            else:
                _nf_data[_data['nfType']].update({_data['nfInstanceId']: _data})

        return _nf_data

    def nrf_get_registered_nf_id(self, nf_type):
        _nf_instances = self.nrf_get_all_registered_nf()
        _nf_ids: Dict[str, dict] = {nf_type: {}}
        nf_type = nf_type.upper()
        try:
            _nf_ids.update({nf_type: [id for id in _nf_instances.get(nf_type).keys()]})
        except AttributeError:
            _nf_ids.update({nf_type: 'UNREGISTERED'})
            print(f'no registered NF: {nf_type} in queried: {self.http_proxy.get_targeted_nf()}')

        return _nf_ids


class AmfClient(BaseNfClient):
    __INSTANCE = None

    def __new__(cls, *args, **kwargs):
        if not cls.__INSTANCE:
            cls.__INSTANCE = super(AmfClient, cls).__new__(cls)
        return cls.__INSTANCE

    def __init__(self):
        """"""
        super().__init__()
        self.http_proxy = PycurlClient()

    def nsf_get_network_slice_information(self):
        """Gets NSI from NSSF for PDU Session presence"""
        # CANNOT BE ACHIEVED


class UdrClient(BaseNfClient):
    __INSTANCE = None

    def __new__(cls, *args, **kwargs):
        if not cls.__INSTANCE:
            cls.__INSTANCE = super(UdrClient, cls).__new__(cls)
        return cls.__INSTANCE

    def __init__(self):
        """"""
        super().__init__()
        self.http_proxy = PycurlClient()

    def udr_get_am_data(self, imsi: str = '666010000000001'):
        """
            This resource represents the subscribed AccessAndMobilitySubscriptionData for a SUPI for use in a serving PLMN.
            It is queried by the AMF via the UDM after registering.
            This resource is modelled with the Document resource archetype (see subclause C.1 of 3GPP TS 29.501 [7]).
        """

        _nudr_sbi = f'http://192.168.58.110:16005/nudr-dr/v1/subscription-data/imsi-{imsi}/66601/provisioned-data/am-data'

        _response = self.send_query(self.http_proxy, _nudr_sbi)

        if not _response:
            print('failed to acquire response')
            return ''
        else:
            return _response

    def udr_get_sm_data(self, sst: int, sd: str, dnn: str = '', imsi: str = '666010000000001', plmn: str = '66601'):
        """
            This resource represents the subscribed SessionManagementSubscriptionData for a SUPI. It is queried by the UDM
            triggered by the SMF during session setup, using one or both of query parameters representing the selected network
            slice and the DNN.
            This resource is modelled with the Document resource archetype (see subclause C.1 of 3GPP TS 29.501 [7])
        """

        _snssai_query = f'single-nssai=%7B%0A%09%22sst%22%3A%09{sst}%2C%0A%09%22sd%22%3A%09%22{sd}%22%0A%7D'

        if dnn:
            _snssai_query = f'{_snssai_query}&dnn={dnn}'

        _nudr_sbi = f'http://192.168.58.110:16005/nudr-dr/v1/subscription-data/imsi-{imsi}/{plmn}/provisioned-data/sm-data'\
                   f'?{_snssai_query}'

        _response = self.send_query(self.http_proxy, _nudr_sbi)

        if not _response:
            print('failed to acquire response')
            return ''
        else:
            return _response
