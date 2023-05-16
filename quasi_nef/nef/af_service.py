from nef_client import *
from typing import List
from functools import reduce


class AfService:
    """
    Singleton class that acts as a feature exposure agent, providing summary of subscriber related data.

    Attributes
    ----------
    __instance: AfService
        a handle to AfService object

    udr_proxy: UdmClient
        a handle to UdrClient object

    nrf_proxy: NrfClient
        a handle to NrfClient object

    db_proxy: MongoDbHandle
        a handle to MongoDB open5gs server

    Methods
    -------

    """
    __INSTANCE = None
    imsis = ['999707364000060', '666010000000001', '666010000000002', '666011', '666012', '666013']

    def __new__(cls, *args, **kwargs):
        if not cls.__INSTANCE:
            cls.__INSTANCE = super(AfService, cls).__new__(cls)
        return cls.__INSTANCE

    def __init__(self):
        self.db_proxy = MongoDbHandle(server='127.0.0.1', port='27017')
        self.udr_proxy = UdrClient()
        self.nrf_proxy = NrfClient()

    def get_aggregated_slice_data(self, sst: int, sd: str, imsis=None):
        """"""
        _aggregated_data: List[dict] = []
        if not imsis:
            imsis = self.imsis

        for imsi in imsis:
            _data = self.udr_proxy.udr_get_sm_data(sst=sst, sd=sd, imsi=imsi)
            if not isinstance(_data, list):
                _data = [_data]
            if 'status' not in _data[0].keys():
                _aggregated_data.append({imsi: _data[0]})

        if not _aggregated_data:
            _message = f'Unsupported slice: S_NSSAI [SST: {sst}, SD: {sd}]'
            raise InvalidQueryException(message=_message, errors='')
        return _aggregated_data

    def summarize_by_slice(self, sst, sd, imsis=None):
        """
           Summarized slice data by:
           total IMSIs
           total session Ambr UL/DL
        """
        if not imsis:
            imsis = self.imsis

        try:
            _data = self.get_aggregated_slice_data(sst=sst, sd=sd, imsis=imsis)
        except InvalidQueryException:
            raise

        _aggregated_bitrate = self._aggregate_bandwidth(self._get_all_ambr_items(_data))
        _per_dnn_bitrate = self._get_per_dnn_ambr_items(_data)
        _per_dnn_aggregated_bitrate = list(
            map(lambda x: {x: self._aggregate_bandwidth(_per_dnn_bitrate[x])}, _per_dnn_bitrate))
        _per_dnn_aggregated_bitrate = self._unpack_dicts(_per_dnn_aggregated_bitrate)

        _all_ssc_modes = self._get_all_ssc_modes(_data)

        _default_required_ssc_modes = list(map(lambda x: {x: set(_all_ssc_modes[x]['DEFAULT_SSCS'])}, _all_ssc_modes))
        _default_required_ssc_modes = dict(reduce((lambda x, y: {**x, **y}), _default_required_ssc_modes))

        _common_supported_ssc_modes = list(map(
            lambda x: {x: self._get_union_of_supported_ssc(_all_ssc_modes[x]['ALL_SSCS'])}, _all_ssc_modes))
        _common_supported_ssc_modes = dict(reduce((lambda x, y: {**x, **y}), _common_supported_ssc_modes))

        _master_key = f'S_NSSAI[SST:{sst} SD:{sd}]'
        _result: Dict[str, dict] = {_master_key: {}}
        _result[_master_key].update({
            'TOTAL_IMSIS': len(_data),
            'ALL_SUPPORTING_IMSIS': list(map(lambda x: next(iter(x)), _data)),
            'PER_DNN_DEFAULT_REQUIRED_SSC': _default_required_ssc_modes,
            'PER_DNN_COMMON_SUPPORTED_SSC': _common_supported_ssc_modes,
            'PER_DNN_AGGREGATED_BITRATE': _per_dnn_aggregated_bitrate,
        })
        _result[_master_key].update(_aggregated_bitrate)

        return _result

    def summarize_by_dnn(self, dnn):
        """
           Summarized slice per-dnn data by:
           total IMSIs
           total session Ambr UL/DL
        """

    def get_all_preemption_capable_ues(self, sst, sd, imsis=None):
        """
            Method to extract all preemptive UES per DNN in a given slice.

            UE that is preemptive can be subjected to preemption of resources if the 5G-AN experiences overload and
            UE of higher priority attempts to access the network.

            "The Pre-emption-Capability IE defines whether a bearer with a lower allocation and retention priority
             (ARP) level should be dropped to free up the required resources." Reference: 3GPP TS 36.444-e10
            "The Pre-emption-Vulnerability IE defines whether a bearer is applicable for such dropping
             by a pre-emption capable bearer with a higher allocation and retention priority (ARP) value.
             Reference: 3GPP TS 36.444-e10"
            "The Priority-Level IE is used for this decision to ensure that the request of the bearer
             with the higher priority level is preferred. In addition, the allocation and retention priority (ARP)
             can be used to decide which bearer(s) to drop during exceptional resource limitations.
             Reference: 3GPP TS 36.444-e10"
        """
        if not imsis:
            imsis = self.imsis

        try:
            _data = self.get_aggregated_slice_data(sst=sst, sd=sd, imsis=imsis)
        except InvalidQueryException:
            raise

        _result: Dict[str: dict] = {'PREEMPTIVE_UES': {}}

        for policy in _data:
            _aux = next(iter(policy.items()))
            _imsi = _aux[0]
            for dnn in _aux[1]['dnnConfigurations']:
                _dnn_data = _aux[1]['dnnConfigurations'][dnn]
                if dnn not in _result['PREEMPTIVE_UES'].keys():
                    _result['PREEMPTIVE_UES'].update({dnn: {}})
                if _dnn_data['5gQosProfile']['arp']['preemptCap'] == 'MAY_PREEMPT':
                    _result['PREEMPTIVE_UES'][dnn].update(
                        {_imsi: {'preemptCap': 'MAY_PREEMPT', 'arp': _dnn_data['5gQosProfile']['priorityLevel']}})
        return _result

    def get_all_preemption_vulnerable_ues(self, sst, sd, imsis=None):
        """
            Method to extract all preemption vulnerable UES per DNN in a given slice.

            UE that is preemptive can be subjected to preemption of resources if the 5G-AN experiences overload and
            UE of higher priority attempts to access the network.

            "The Pre-emption-Capability IE defines whether a bearer with a lower allocation and retention priority
             (ARP) level should be dropped to free up the required resources." Reference: 3GPP TS 36.444-e10
            "The Pre-emption-Vulnerability IE defines whether a bearer is applicable for such dropping
             by a pre-emption capable bearer with a higher allocation and retention priority (ARP) value.
             Reference: 3GPP TS 36.444-e10"
            "The Priority-Level IE is used for this decision to ensure that the request of the bearer
             with the higher priority level is preferred. In addition, the allocation and retention priority (ARP)
             can be used to decide which bearer(s) to drop during exceptional resource limitations.
             Reference: 3GPP TS 36.444-e10"
        """
        if not imsis:
            imsis = self.imsis

        _data = self.get_aggregated_slice_data(sst=sst, sd=sd, imsis=imsis)
        _result: Dict[str: dict] = {'PREEMPTABLE_UES': {}}

        for policy in _data:
            _aux = next(iter(policy.items()))
            _imsi = _aux[0]
            for dnn in _aux[1]['dnnConfigurations']:
                _dnn_data = _aux[1]['dnnConfigurations'][dnn]
                if dnn not in _result['PREEMPTABLE_UES'].keys():
                    _result['PREEMPTABLE_UES'].update({dnn: {}})
                if _dnn_data['5gQosProfile']['arp']['preemptVuln'] == 'PREEMPTABLE':
                    _result['PREEMPTABLE_UES'][dnn].update(
                        {_imsi: {'preemptVuln': 'PREEMPTABLE', 'arp': _dnn_data['5gQosProfile']['priorityLevel']}})
        return _result

    @staticmethod
    def _aggregate_bandwidth(bandwidths: list) -> dict:
        """Inner method to aggregate bitrate"""

        _ul = list(map(lambda x: x['uplink'].split()[0], bandwidths))
        _dl = list(map(lambda x: x['downlink'].split()[0], bandwidths))

        _ul_add = reduce((lambda x, y: int(x) + int(y)), _ul)
        _dl_add = reduce((lambda x, y: int(x) + int(y)), _dl)

        return {'TOTAL_AMBR_UL': str(_ul_add) + 'Kbps', 'TOTAL_AMBR_DL': str(_dl_add) + 'Kbps'}

    @staticmethod
    def _get_all_ambr_items(data):
        """Inner method to extract all sessionAmbr dicts"""

        _all_ambr_items = []
        for policy in data:
            _aux = dict(next(iter(policy.items()))[1])
            for dnn in _aux['dnnConfigurations']:
                _all_ambr_items.append(_aux['dnnConfigurations'][dnn]['sessionAmbr'])
        return _all_ambr_items

    @staticmethod
    def _get_per_dnn_ambr_items(data):
        """Inner method to extract per DNN sessionAmbr dicts"""
        _all_ambr_items: Dict[str, list] = dict()
        for policy in data:
            _aux = dict(next(iter(policy.items()))[1])
            for dnn in _aux['dnnConfigurations']:
                if dnn not in _all_ambr_items.keys():
                    _all_ambr_items.update({dnn: []})
                _all_ambr_items[dnn].append(_aux['dnnConfigurations'][dnn]['sessionAmbr'])
        return _all_ambr_items

    @staticmethod
    def _get_all_ssc_modes(data):
        """
            Inner method to extract all per-imsi/slice subscribed SSC modes

            The SSC mode associated with the application is either the SSC mode included in a non-default SSCMSP rule
            that matches the application or the SSC mode included in the default SSC mode selection policy rule, if present.
            If the SSCMSP does not include a default SSCMP rule and no other rule matches the application,
            then the UE requests the PDU Session without providing the SSC mode.
            In this case, the network determines the SSC mode of the PDU Session.

            The SMF receives from the UDM the list of supported SSC modes and the default SSC mode per DNN
            per S-NSSAI as part of the subscription information.
        """
        _ssc_modes: Dict[str, dict] = dict()
        for policy in data:
            _aux = dict(next(iter(policy.items()))[1])
            for dnn in _aux['dnnConfigurations']:
                if dnn not in _ssc_modes.keys():
                    _ssc_modes.update({dnn: {'DEFAULT_SSCS': [], 'ALL_SSCS': []}})
                _ssc_modes[dnn]['DEFAULT_SSCS'].append(_aux['dnnConfigurations'][dnn]['sscModes']['defaultSscMode'])
                _ssc_modes[dnn]['ALL_SSCS'].append(_aux['dnnConfigurations'][dnn]['sscModes']['allowedSscModes'])
        return _ssc_modes

    @staticmethod
    def _get_union_of_supported_ssc(all_ssc: list):
        """Inner method to return an union of per dnn supported SSCs"""
        return list(reduce((lambda x, y: set(x).union(set(y))), all_ssc))

    @staticmethod
    def _get_all_subscribed_dnns(data):
        """Inner method to extract all subscribed DNNs in a slice"""
        pass

    @staticmethod
    def _unpack_dicts(dicts_to_unpack: list):
        return dict(reduce((lambda x, y: {**x, **y}), dicts_to_unpack))
