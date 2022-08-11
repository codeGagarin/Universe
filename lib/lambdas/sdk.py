from yandex.cloud.serverless.functions.v1.function_pb2 import Function, Resources, Version

from yandex.cloud.serverless.functions.v1.function_service_pb2_grpc import FunctionServiceStub
from yandex.cloud.operation.operation_service_pb2_grpc import OperationServiceStub

from yandex.cloud.iam.v1.service_account_service_pb2_grpc import ServiceAccountServiceStub


from yandexcloud import SDK
from google.protobuf import duration_pb2

from keys import KeyChain


def _remove_intent(body: str) -> str:
    return '\n'.join([
            line.strip()
            for line in body.split('\n')
            if len(line) != 0
        ])


def _sanitize_key(key_dict: dict, convert_fields: str) -> dict:
    fields = convert_fields.split(' ')

    for field in fields:
        assert field in key_dict.keys(), f'<convert_fields> is absent in field names <{field}>'

    return {
        key: _remove_intent(value) if key in fields else value
        for key, value
        in key_dict.items()
    }


class Clients:

    _sdk = SDK(
        service_account_key=_sanitize_key(KeyChain.YC_DEPLOY_ACC, 'private_key')
    )

    _fc_client = None
    _sa_client = None
    _op_client = None

    @classmethod
    def function_service(cls) -> FunctionServiceStub:
        if not cls._fc_client:
            cls._fc_client = cls._sdk.client(FunctionServiceStub)
        return cls._fc_client

    @classmethod
    def service_account_service(cls) -> ServiceAccountServiceStub:
        if not cls._sa_client:
            cls._sa_client = cls._sdk.client(ServiceAccountServiceStub)
        return cls._sa_client

    @classmethod
    def operation_service(cls) -> OperationServiceStub:
        if not cls._op_client:
            cls._op_client = cls._sdk.client(OperationServiceStub)
        return cls._op_client

    @classmethod
    def wait_operation_and_get_result(cls, *args, **kvargs):
        return cls._sdk.wait_operation_and_get_result(*args, **kvargs)
