import json
from tempfile import mkstemp
from pathlib import Path
from zipfile import ZipFile

import yandex.cloud.access.access_pb2 as ac
import yandex.cloud.iam.v1.service_account_service_pb2 as sa
import yandex.cloud.serverless.functions.v1.function_service_pb2 as fs
from yandex.cloud.serverless.functions.v1.function_pb2 import Function, Resources, Version
from google.protobuf import duration_pb2


from config import Lambda
from sdk import Clients
from keys import KeyChain

def pretty_file_size(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def check_public_access(function_id) -> bool:
    access_bindings = Clients.function_service().ListAccessBindings(
            ac.ListAccessBindingsRequest(
                resource_id=function_id
            )
    ).access_bindings
    for binding in access_bindings:
        if binding.role_id == 'serverless.functions.invoker':
            subject = binding.subject
            return subject.id == "allUsers"
    return False


def set_function_access(function_id=None, is_public=True,):
    if check_public_access(function_id) == is_public:
        return False
    if is_public:
        access_bindings = [
            ac.AccessBinding(
                role_id='serverless.functions.invoker',
                subject=ac.Subject(
                    id="allUsers",
                    type="system",
                )
            )
        ]
    else:
        access_bindings = []
    op = Clients.function_service().SetAccessBindings(
        ac.SetAccessBindingsRequest(
            resource_id=function_id,
            access_bindings=access_bindings
        )
    )
    Clients.wait_operation_and_get_result(
        operation=op,
        meta_type=ac.SetAccessBindingsMetadata,
    )
    return True


def service_account_name_to_id(name: str) -> str:
    response = Clients.service_account_service().List(
        sa.ListServiceAccountsRequest(
            folder_id=KeyChain.YC_DEFAULT_CATALOG,
            filter=f'name="{name}"'
        )
    )
    assert len(response.service_accounts) == 1, \
        f'Service account name [{name}] not found.' \
        f'May be deploying account need role [iam.serviceAccounts.user] to access on service account information'
    return response.service_accounts[0].id


def get_function_id(lambda_name: str, fs_client):
    function_list = fs_client.List(
        fs.ListFunctionsRequest(
            folder_id=KeyChain.YC_DEFAULT_CATALOG,
            filter=f'name="{lambda_name}"'
        )
    ).functions

    return function_list[0].id if function_list else None


def create_function(lambda_params: Lambda, fs_client):
    op = fs_client.Create(
        fs.CreateFunctionRequest(
            folder_id=KeyChain.YC_DEFAULT_CATALOG,
            name=lambda_params.get_full_name(),
            description=lambda_params.description,
        )
    )

    return Clients.wait_operation_and_get_result(
        operation=op,
        response_type=Function,
    ).response.id


def update_function(function_id: str, lambda_params: Lambda, fs_client):
    op = fs_client.Update(
        fs.UpdateFunctionRequest(
            function_id=function_id,
            name=lambda_params.get_full_name(),
            description=lambda_params.description,
        )
    )

    return Clients.wait_operation_and_get_result(
        operation=op,
        response_type=Function,
    ).response.id


def codes_to_pack(lambda_params: Lambda):
    _, zip_path = mkstemp()
    manual_data = lambda_params.manual_data or {}

    """ Inject key engine """
    if lambda_params.secrets:
        with open(Path(__file__).parent.joinpath('key_chain.py'), mode='rt') as keys_engine:
            manual_data['keys.py'] = keys_engine.read()

    """ Inject requirements.txt """
    if lambda_params.req:
        manual_data['requirements.txt'] = '\n'.join(lambda_params.req)

    with ZipFile(zip_path, 'w') as _zip:
        for path in lambda_params.files or ():
            _zip.write(path)

        for path, data in (manual_data or {}).items():
            _zip.writestr(path, data)

    with open(zip_path, 'rb') as zip_file:
        return zip_file.read()


def create_version(function_id: str, lambda_params: Lambda, fs_client):
    env = lambda_params.env or {}
    op = fs_client.CreateVersion(
        fs.CreateFunctionVersionRequest(
            function_id=function_id,
            runtime=lambda_params.runtime,
            resources=Resources(
                memory=lambda_params.mem_usage
            ),
            entrypoint=lambda_params.handler,
            content=codes_to_pack(lambda_params),
            execution_timeout=duration_pb2.Duration(
                seconds=lambda_params.execute_duration
            ),
            environment=dict(
                **env,
                **{
                    f'SECRET_{secret_name}': KeyChain.dump(secret_name)
                    for secret_name in lambda_params.secrets or ()
                },
            ),
            service_account_id=service_account_name_to_id(
                lambda_params.role
            ),
        )
    )

    return Clients.wait_operation_and_get_result(
        operation=op,
        response_type=Version,
    ).response.id
