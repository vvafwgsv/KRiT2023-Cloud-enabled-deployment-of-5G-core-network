import re
import pycurl
from io import BytesIO
from typing import Dict
import pymongo


class InvalidQueryException(Exception):
    def __init__(self, message, errors):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.errors = errors


def override(f):
    """decorator for override hint"""
    return f


class SbiMapping:
    """"""
    sbi_mapping = {'NRF': 16000, 'AMF': 16001, 'SMF': 16002,
                   'PCF': 16003, 'UDM': 16004, 'UDR': 16005,
                   'NSSF': 16006, 'AUSF': 16007, 'BSF': 16008}
    inv_sbi_mapping = {16000: 'NRF', 16001: 'AMF', 16002: 'SMF',
                       16003: 'PCF', 16004: 'UDM', 16005: 'UDR',
                       16006: 'NSSF', 16007: 'AUSF', 16008: 'BSF'}

    @classmethod
    def nf_to_sbi_port(cls, nf):
        try:
            return cls.sbi_mapping.get(nf.upper())
        except KeyError:
            print(f'Invalid key: {nf}')

    @classmethod
    def sbi_port_to_nf(cls, port):
        try:
            return cls.inv_sbi_mapping.get(port)
        except KeyError:
            print(f'Invalid key: {port}')


class MongoDbHandle:
    """
    Singleton class that acts as a proxy between mongoDB 5GS and quasi-NEF

    Attributes
    ----------
    __instance: PycurlClient
        a handle to PycurlClient object

    Methods
    -------

    """
    __instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(MongoDbHandle, cls).__new__(cls)
        return cls.__instance

    def __init__(self, server, port):
        super().__init__()
        self.server = server
        self.port = port

    def get_all_imsi(self):
        myclient = pymongo.MongoClient("mongodb://" + str(self.server) + ":" + str(self.port) + "/")
        mydb = myclient["open5gs"]
        mycol = mydb["subscribers"]
        subs_list = []
        for x in mycol.find({}, {'imsi': 1, '_id': 0}):
            print(x)
            subs_list.append(x)

        return subs_list


class PycurlClient(pycurl.Curl):
    """
    Singleton class that acts as a proxy between SBI interfaces of NFs in 5GS and quasi-NEF

    Attributes
    ----------
    __instance: PycurlClient
        a handle to PycurlClient object
    __libcurl_acceptable: dict
        a mapping of integer codes onto libcurl options for verification of pycurl attributes

    Methods
    -------

    """

    __instance = None
    __libcurl_acceptable: Dict[int, str] = {v: k for k, v in pycurl.__dict__.items()
                                            if not k.startswith('__') and k.isupper()}

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(PycurlClient, cls).__new__(cls)
        return cls.__instance

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._curl_options: Dict[str, str] = dict()
        self._buffer: BytesIO = BytesIO()
        if not 'FORCE_HTTP' in kwargs:
            self.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE)

    def send_post_event(self):
        """Issues POST http query towards NF"""
        self.setopt(pycurl.POST, 1)
        self.perform()

    def send_get_event(self):
        """Issues GET http query towards NF"""
        self.setopt(pycurl.POST, 0)
        try:
            self.perform()
        except InvalidQueryException:
            raise

    @override
    def perform(self):
        """Method to execute http/2 query towards NF SBI servers"""
        self._buffer = BytesIO()
        assert 'URL' in self._curl_options, f'url unset'
        self.setopt(pycurl.WRITEDATA, self._buffer)
        try:
            super().perform()
        except pycurl.error as err:
            _message = f'Unable to perform http/2 request to {self._curl_options.get("URL")}\n\tError: {err}'
            raise InvalidQueryException(message=_message, errors=err)

    def get_http_response(self, encoding: str = 'utf8'):
        """Method to acquire http/2 response from NF SBI servers"""

        _response: bytes = self._buffer.getvalue()
        _response_decoded = _response.decode(encoding)
        return _response_decoded

    @override
    def setopt(self, option, value):
        """Sets an option of Curl client"""

        assert option in self.__libcurl_acceptable, f'invalid option to setopt: {option}'
        try:
            super().setopt(option, value)
            self._curl_options.update({self.__libcurl_acceptable.get(option): value})
        except TypeError as err:
            print(f'invalid value: {value} to option: {option}; {err}')
        except pycurl.error as err:
            print(f'libcurl rejected option || value: {err}')

    @override
    def delopt(self, option):
        """Unsets an option of Curl client"""

        assert option in self.__libcurl_acceptable, f'invalid option to unsetopt: {option}'
        try:
            self._curl_options.pop(option)
            super().unsetopt(option)
        except KeyError:
            print(f'option valid, but unset')

    def get_targeted_nf(self):
        """Returns the NF name that the currently set URL of proxy points to"""
        _url = self.get_url()
        _port = re.match(r'.+:(\d+)/.+', _url).groups()[0]
        try:
            return SbiMapping.sbi_port_to_nf(int(_port))
        except:
            print(f'cannot map current port to NF SBI instance')

    def get_targeted_resource(self):
        """Returns the NF resource that the currently set URL of proxy points to"""
        _url = self.get_url()
        _resources = re.match(r'.+:(\d+)\/(?P<SERVICE>[\w-]+)\/(?P<VERSION>v[1-2]{1})\/(?P<RESOURCE>[\w-]+).*', _url).groupdict()
        return _resources

    def get_url(self):
        try:
            return self._curl_options.get('URL')
        except KeyError:
            print('URl unset')

