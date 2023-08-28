from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import BinaryIO, Dict, Optional, Iterator, Union, TypedDict

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

from azure_.azure_ad_auth import ADTokenRefresher, AzureCloudEnv
from util.http import create_session, raise_if_err

_5_minutes = 5 * 60
logger = logging.getLogger("eshare_analytics." + __loader__.name)


JSONPrimitive = Union[str, float, bool]
DatasetParams = Dict[str, JSONPrimitive]


class CloudEnv(Enum):
    Commercial = "Commercial"
    GCC = "GCC"
    GCCHigh = "GCCHigh"
    DoDCON = "DoDCON"
    DoD = "DoD"


class DatasetMeasure(TypedDict, total=False):
    description: str
    expression: str
    formatString: str
    isHidden: bool
    name: str


class DatasetColumn(TypedDict, total=False):
    dataCategory: str
    dataType: str
    formatString: str
    isHidden: bool
    name: str
    sortByColumn: str
    summarizeBy: str


class DatasetTable(TypedDict, total=False):
    columns: list[DatasetColumn]
    measures: list[DatasetMeasure]
    name: str
    description: str
    isHidden: bool


class UploadConflict(Enum):
    Abort = "Abort"
    CreateOrOverwrite = "CreateOrOverwrite"
    Overwrite = "Overwrite"


class WorkspaceUserPermission(Enum):
    # https://docs.microsoft.com/en-us/rest/api/power-bi/groups/add-group-user#groupuseraccessright
    Admin = "Admin"
    Contributor = "Contributor"
    Member = "Member"
    None_ = "None"
    Viewer = "Viewer"


@dataclass
class DatasetRefreshSchedule:
    # the timezone is a string value from
    # https://github.com/unicode-org/icu-data/blob/main/tzdata/icunew/2021a4/44/windowsZones.txt
    # as documented at https://docs.microsoft.com/en-us/dotnet/api/system.timezoneinfo.id
    timezone: str
    times: list[str]
    days: Optional[list[str]] = None


class PowerBIApiError(Exception):
    message = "PowerBI error"

    def __init__(self, description=None):
        self.description = description
        super().__init__(str(self))

    def __str__(self):
        return (
            type(self).__name__
            + ": "
            + self.message
            + (": " + self.description if self.description else "")
        )


class AlreadyExists(PowerBIApiError):
    message = "The requested item already exists"


class IncompatibleDatasetParameters(PowerBIApiError):
    message = "The requested parameters are not compatible with the existing dataset parameters"


class CannotDetermineOverwrite(PowerBIApiError):
    message = "Cannot determine which artifact to overwrite"


def raise_if_already_exists_error(r: requests.Response):
    # already exists error: 400 {"error":{"code":"PowerBIEntityAlreadyExists","pbi.error":{"code":"PowerBIEntityAlreadyExists","parameters":{},"details":[]}}}
    if (
        r.status_code == 400
        and r.json().get("error", {}).get("code") == "PowerBIEntityAlreadyExists"
    ):
        raise AlreadyExists


class PowerBIApi:
    # https://docs.microsoft.com/en-us/power-bi/developer/automation/
    # https://docs.microsoft.com/en-us/rest/api/power-bi/
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        concurrency: int,
        cloud_env: CloudEnv = CloudEnv.Commercial,
        retry: int = 10,
    ):
        self.client_id = client_id
        self.session = create_session(
            num_workers=concurrency,
            num_hosts=2,  # the powerbi api service url and the authorization url
            retry=retry,
            retry_allowed_methods=None,  # allow all methods to be retried
        )
        # urls found at
        # https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-sample-for-customers-national-clouds
        if cloud_env is CloudEnv.Commercial:
            resource = "https://analysis.windows.net/powerbi/api"
            api_root_url = "https://api.powerbi.com"
            azure_cloud = AzureCloudEnv.AzureCloud
        elif cloud_env is CloudEnv.GCC:
            resource = "https://analysis.usgovcloudapi.net/powerbi/api"
            api_root_url = "https://api.powerbigov.us"
            azure_cloud = AzureCloudEnv.AzureCloud
        elif cloud_env is CloudEnv.GCCHigh or cloud_env is CloudEnv.DoDCON:
            resource = "https://high.analysis.usgovcloudapi.net/powerbi/api"
            api_root_url = "https://api.high.powerbigov.us"
            azure_cloud = AzureCloudEnv.AzureUSGovernment
        elif cloud_env is CloudEnv.DoD:
            resource = "https://mil.analysis.usgovcloudapi.net/powerbi/api"
            api_root_url = "https://api.mil.powerbigov.us"
            azure_cloud = AzureCloudEnv.AzureUSGovernment
        else:
            raise Exception(f"Unsupported cloud env {cloud_env}")

        self.base_url = api_root_url + "/v1.0/myorg"
        self._azure_ad_token_refresher = ADTokenRefresher(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            resource=resource,
            session=self.session,
            azure_cloud=azure_cloud,
        )

    def _request(self, *args, raise_=True, **kwargs):
        self._azure_ad_token_refresher.refresh_token_if_expired()
        r = self.session.request(*args, **kwargs)
        if raise_:
            raise_if_err(r)
        return r

    def list_workspaces(self) -> list[dict]:
        url = self.base_url + "/groups"
        r = self._request("GET", url)
        return r.json()["value"]

    def list_datasets(self, workspace_id: str) -> list[dict]:
        url = self.base_url + "/groups/{}/datasets".format(workspace_id)
        r = self._request("GET", url)
        return r.json()["value"]

    def list_reports(self, workspace_id: str) -> list[dict]:
        url = self.base_url + "/groups/{}/reports".format(workspace_id)
        r = self._request("GET", url)
        return r.json()["value"]

    def list_workspace_users(self, workspace_id: str) -> list[dict]:
        url = self.base_url + "/groups/{}/users".format(workspace_id)
        r = self._request("GET", url)
        return r.json()["value"]

    def get_report(self, workspace_id: str, report_id: str) -> dict:
        url = self.base_url + "/groups/{}/reports/{}".format(workspace_id, report_id)
        r = self._request("GET", url)
        return r.json()

    def get_report_by_name(self, workspace_id: str, name: str) -> dict:
        reports = self.list_reports(workspace_id)
        for report in reports:
            if report["name"] == name:
                return report

    def get_dataset_by_name(self, workspace_id: str, name: str) -> Optional[dict]:
        datasets = self.list_datasets(workspace_id)
        for dataset in datasets:
            if dataset["name"] == name:
                return dataset

    def upload_import_ovewrite_existing(
        self, f: BinaryIO, workspace_id: str, name: str
    ) -> tuple[list[dict], list[dict]]:
        try:
            return self.upload_import(
                f,
                workspace_id,
                name,
                conflict=UploadConflict.CreateOrOverwrite,
            )
        except CannotDetermineOverwrite:
            # when an artifact already exists more than once, we get back a MoreThanOneDuplicatePackageFoundError
            # because the service does not know which one to overwrite
            # it's not clear how multiple dupes can happen, but they have happened
            # we catch the error, delete all dupes, and retry
            logger.warning(
                "Found more than one artifact called '%s', will delete and retry",
                name,
            )
            # delete all dupes and try again
            ds = self.list_datasets(workspace_id)
            for d in ds:
                if d["name"] == name:
                    logger.debug("Deleting dataset '%s' '%s'", d["id"], d["name"])
                    self.delete_dataset(workspace_id, d["id"])
            rs = self.list_reports(workspace_id)
            for r in rs:
                if r["name"] == name:
                    logger.debug("Deleting report '%s' '%s'", r["id"], r["name"])
                    self.delete_report(workspace_id, r["id"])
            # when trying to reupload after the deletions, the request always fails with the following error
            # {"error":{"code":"UnknownError","pbi.error":{"code":"UnknownError","parameters":{},"details":[],"exceptionCulprit":1}}}
            # the error shows even if we wait for a while. it looks like it needs a new connection to succeed
            return self.upload_import(
                f,
                workspace_id,
                name,
                conflict=UploadConflict.CreateOrOverwrite,
            )

    def upload_import(
        self,
        f: BinaryIO,
        workspace_id: str,
        name: str,
        conflict=UploadConflict.Abort,
        dataset_only: bool = False,
        wait: bool = True,
    ) -> Optional[tuple[list[dict], list[dict]]]:
        url = self.base_url + "/groups/{}/imports".format(workspace_id)
        query_params = {"datasetDisplayName": name + ".pbix"}
        if conflict is not None:
            query_params["nameConflict"] = conflict.value
        if dataset_only:
            query_params["skipReport"] = "true"

        # sending a multipart/form-data
        # the key of the file entry seems to be irrelevant. I am setting it to be equal to the name
        # using requests_toolbelt because plain requests loads the whole file in memory, which blows
        # up for big datasets
        m = MultipartEncoder(fields={name: (name, f)})
        r = self._request(
            "POST",
            url,
            params=query_params,
            data=m,
            headers={"Content-Type": m.content_type},
            raise_=False,
        )
        if (
            r.status_code == 409
            and conflict is UploadConflict.CreateOrOverwrite
            and r.json().get("error", {}).get("code")
            in (
                "MoreThanOneDuplicatePackageFoundError",
                "MoreThanOneDuplicateReportFoundError",
            )
        ):
            # when a dataset already exists more than once, we get back a MoreThanOneDuplicatePackageFoundError
            # because the service does not know which one to overwrite
            raise CannotDetermineOverwrite
        raise_if_err(r)

        waited = 0
        wait_seconds = 5
        import_item = r.json()
        while wait:
            import_item = self.get_import(workspace_id, import_item["id"])
            if import_item.get("importState") == "Succeeded":
                return import_item["datasets"], import_item["reports"]
            elif import_item.get("importState") == "Failed":
                raise Exception(f"Publishing of import failed. {import_item}")
            else:
                if waited != 0 and waited % 300 == 0:
                    logger.debug(
                        "Waiting for publish of 'groups/%s/import/%s', state: %s",
                        workspace_id,
                        import_item["id"],
                        import_item,
                    )
                time.sleep(wait_seconds)
                waited += wait_seconds

    def get_import(self, workspace_id: str, import_id: str) -> dict:
        url = self.base_url + "/groups/{}/imports/{}".format(workspace_id, import_id)
        r = self._request("GET", url)
        return r.json()

    def update_report_content(
        self,
        workspace_id: str,
        report_id: str,
        source_workspace_id: str,
        source_report_id: str,
    ) -> None:
        url = self.base_url + "/groups/{}/reports/{}/UpdateReportContent".format(
            workspace_id, report_id
        )
        r = self._request(
            "POST",
            url,
            json={
                "sourceReport": {
                    "sourceReportId": source_report_id,
                    "sourceWorkspaceId": source_workspace_id,
                },
                "sourceType": "ExistingReport",
            },
        )

    def get_dataset_parameters(
        self,
        workspace_id: str,
        dataset_id: str,
    ) -> DatasetParams:
        url = self.base_url + "/groups/{}/datasets/{}/parameters".format(
            workspace_id, dataset_id
        )
        r = self._request("GET", url)
        return {
            e["name"]: e.get("currentValue") == "TRUE"
            if e["type"] == "Logical"
            else e.get("currentValue")
            for e in r.json()["value"]
        }

    def update_dataset_parameters(
        self,
        workspace_id: str,
        dataset_id: str,
        params: DatasetParams,
        take_over_on_err=True,
    ) -> None:
        url = self.base_url + "/groups/{}/datasets/{}/Default.UpdateParameters".format(
            workspace_id, dataset_id
        )
        r = self._request(
            "POST",
            url,
            json={
                "updateDetails": [{"name": k, "newValue": v} for k, v in params.items()]
            },
            raise_=False,
        )
        if r.status_code == 403 and take_over_on_err:
            self.take_dataset_ownership(workspace_id, dataset_id)
            return self.update_dataset_parameters(
                workspace_id, dataset_id, params, take_over_on_err=False
            )
        if (
            r.status_code == 404
            and "Dataset parameters" in r.json()["error"]["message"]
        ):
            # there's a scenario where params of the existing dataset are incompatible with the new
            # ones and impossible to update. this is a bug with the dataset, i.e. the
            # dataset we chose to deploy has params X, but we are trying to set params Y
            # this appears like this
            # status=404, headers={'Cache-Control': 'no-store, must-revalidate, no-cache', 'Pragma': 'no-cache', 'Transfer-Encoding': 'chunked', 'Content-Type': 'application/json; charset=utf-8', 'Strict-Transport-Security': 'max-age=31536000; includeSubDomains', 'X-Frame-Options': 'deny', 'X-Content-Type-Options': 'nosniff', 'RequestId': '2ff6e21d-5dfb-492d-8cd2-a79421a25b7e', 'Access-Control-Expose-Headers': 'RequestId', 'Date': 'Wed, 28 Dec 2022 21:57:01 GMT'} content=b'{"error":{"code":"ItemNotFound","message":"Dataset parameters at positions 1 - 4 cannot be found in da9ee43e-6933-495a-9bbf-abf8ddc58424","target":"da9ee43e-6933-495a-9bbf-abf8ddc58424"}}'
            raise IncompatibleDatasetParameters(r.json()["error"]["message"])
        raise_if_err(r)

    def take_dataset_ownership(self, workspace_id: str, dataset_id: str):
        url = self.base_url + "/groups/{}/datasets/{}/Default.TakeOver".format(
            workspace_id, dataset_id
        )
        r = self._request("POST", url)

    def delete_dataset(self, workspace_id: str, dataset_id: str) -> None:
        url = self.base_url + "/groups/{}/datasets/{}".format(workspace_id, dataset_id)
        r = self._request("DELETE", url)

    def delete_report(self, workspace_id: str, report_id: str) -> None:
        url = self.base_url + "/groups/{}/reports/{}".format(workspace_id, report_id)
        r = self._request("DELETE", url)

    def refresh_dataset(
        self, workspace_id: str, dataset_id: str, wait=False
    ) -> Optional[tuple[str, dict]]:
        url = self.base_url + "/groups/{}/datasets/{}/refreshes".format(
            workspace_id, dataset_id
        )
        r = self._request(
            "POST",
            url,
            json={"notifyOption": "NoNotification"},
        )
        refresh_id = r.headers["RequestId"].lower()
        waited = 0
        wait_seconds = 15
        while wait:
            refreshes = self.get_dataset_refresh_history(workspace_id, dataset_id)
            if not refreshes and waited > 60:
                raise Exception("Dataset refresh failed to start")
            for refresh in refreshes:
                if refresh["requestId"].lower() == refresh_id:
                    if refresh["status"] != "Unknown":
                        return refresh["status"], json.loads(
                            refresh.get("serviceExceptionJson", "null")
                        )
                    break
            if waited != 0 and waited % 300 == 0:
                logger.debug(
                    "Waiting for refresh of 'groups/%s/datasets/%s'",
                    workspace_id,
                    dataset_id,
                )
            time.sleep(wait_seconds)
            waited += wait_seconds

    def get_dataset_refresh_history(
        self, workspace_id: str, dataset_id: str, limit: int = None
    ) -> list[dict]:
        url = self.base_url + "/groups/{}/datasets/{}/refreshes".format(
            workspace_id, dataset_id
        )
        r = self._request(
            "GET",
            url,
            params={"$top": limit} if limit is not None else None,
        )
        return r.json()["value"]

    def get_dataset_latest_refresh(self, workspace_id: str, dataset_id: str) -> dict:
        refreshes = self.get_dataset_refresh_history(workspace_id, dataset_id, limit=1)
        if refreshes:
            return refreshes[0]

    def get_dataset_refresh_schedule(self, workspace_id: str, dataset_id: str):
        url = (
            self.base_url
            + f"/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
        )
        r = self._request("GET", url)
        return r.json()

    def _update_dataset_refresh_schedule(
        self, workspace_id: str, dataset_id: str, data: dict
    ):
        url = (
            self.base_url
            + f"/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
        )
        self._request(
            "PATCH",
            url,
            json={"value": data},
        )

    def set_dataset_refresh_schedule(
        self, workspace_id: str, dataset_id: str, schedule: DatasetRefreshSchedule
    ):
        self._update_dataset_refresh_schedule(
            workspace_id,
            dataset_id,
            data={
                "enabled": True,
                "localTimeZoneId": schedule.timezone,
                "times": schedule.times,
                **({"days": schedule.days} if schedule.days else {}),
                "NotifyOption": "NoNotification",
                # not only is it not possible to add specific email addresses to be notified, but
                # it is not possible to specify mail notification at all, call returns a
                # 400 Invalid NotifyOption value 'MailOnFailure' for app only owner requests
                # more details:
                # https://community.powerbi.com/t5/Service/NotifyOption-with-Service-Principal/m-p/1321456
                # https://stackoverflow.com/questions/62340439/power-bi-c-sharp-api-how-to-update-refreshschedule-of-the-datasets
                # https://github.com/microsoft/PowerBI-CSharp/blob/baaffdefa45424ce369ff4ff8a974efbe49c9b7f/sdk/PowerBI.Api/Source/V2/Models/ScheduleNotifyOption.cs#L50-L59
            },
        )

    def disable_dataset_refresh_schedule(self, workspace_id: str, dataset_id: str):
        self._update_dataset_refresh_schedule(
            workspace_id,
            dataset_id,
            data={
                "enabled": False,
            },
        )

    def enable_dataset_refresh_schedule(self, workspace_id: str, dataset_id: str):
        self._update_dataset_refresh_schedule(
            workspace_id,
            dataset_id,
            data={
                "enabled": True,
            },
        )

    def export_report(self, workspace_id: str, report_id: str) -> Iterator[bytes]:
        # the powerbi UI uses a different undocumented endpoint
        # GET https://wabi-us-north-central-b-redirect.analysis.windows.net/export/v201606/reports/{report id}/pbix
        # the export REST API fails if some common headers are present. I have not identified exactly
        # which headers cause this, but experiments point to Accept and Accept-Encoding
        # The powershell cmdlet to export the report calls the export REST API endpoint with minimal
        # headers and it succeeds, so make sure to get rid of any unnecessary headers
        url = self.base_url + "/groups/{}/reports/{}/Export".format(
            workspace_id, report_id
        )
        r = self._request(
            "GET", url, headers={"Accept": None, "Accept-Encoding": None}, stream=True
        )
        for chunk in r.raw.stream(1024, decode_content=False):
            if chunk:
                yield chunk

    def get_dataset_datasources(self, workspace_id: str, dataset_id: str) -> list[dict]:
        url = self.base_url + "/groups/{}/datasets/{}/datasources".format(
            workspace_id, dataset_id
        )
        r = self._request(
            "GET",
            url,
        )
        return r.json()["value"]

    def create_datasource(self, gateway_id: str):
        url = self.base_url + "/gateways/{}/datasources".format(
            gateway_id, datasource_id
        )
        r = self._request(
            "POST",
            url,
            json={
                "credentialDetails": {
                    "credentialType": "Basic",
                    "credentials": json.dumps(creds),
                    "encryptedConnection": "Encrypted",
                    "encryptionAlgorithm": "None",
                    "privacyLevel": "None",
                    "useEndUserOAuth2Credentials": "False",
                }
            },
        )

    def update_datasource_credentials(
        self, gateway_id: str, datasource_id: str, username: str, password: str
    ):
        url = self.base_url + "/gateways/{}/datasources/{}".format(
            gateway_id, datasource_id
        )
        creds = {
            "credentialData": [
                {"name": "username", "value": username},
                {"name": "password", "value": password},
            ]
        }
        r = self._request(
            "PATCH",
            url,
            json={
                "credentialDetails": {
                    "credentialType": "Basic",
                    "credentials": json.dumps(creds),
                    "encryptedConnection": "Encrypted",
                    "encryptionAlgorithm": "None",
                    "privacyLevel": "None",
                    "useEndUserOAuth2Credentials": "False",
                }
            },
        )

    def update_dataset_sql_credentials(
        self,
        workspace_id: str,
        dataset_id: str,
        server: str,
        database: str,
        username: str,
        password: str,
    ) -> bool:
        datasources = self.get_dataset_datasources(workspace_id, dataset_id)
        # looking for this
        # {
        #     "datasourceType": "Sql",
        #     "connectionDetails": {
        #         "server": server,
        #         "database": database
        #     },
        #     "datasourceId": "b9527252-8965-49f3-90dd-889678cd4cd6",
        #     "gatewayId": "64a6f5d8-ce80-4e82-ae89-4805ffd36e75"
        # }
        for datasource in datasources:
            if (
                datasource["datasourceType"] == "Sql"
                and datasource["connectionDetails"]["server"] == server
                and datasource["connectionDetails"]["database"] == database
            ):
                break
            else:
                return False
        self.update_datasource_credentials(
            datasource["gatewayId"],
            datasource["datasourceId"],
            username,
            password,
        )
        return True

    def get_workspace_by_name(self, name: str) -> dict:
        url = self.base_url + "/groups"
        # workspace names are case-sensitive, and there can only be 1 workspace with the same name,
        # so this query is guaranteed to return 0 or 1 result
        r = self._request("GET", url, params={"$filter": f"name eq '{name}'"})
        results = r.json()["value"]
        if results:
            return results[0]

    def create_workspace(self, name: str) -> dict:
        """
        Create a workspace (case sensitive)

        Reference:
        https://docs.microsoft.com/en-us/rest/api/power-bi/groups/create-group#create-a-workspace-in-the-new-workspace-experience-example

        :param name: (str) - name of the workspace to be created

        :return workspace: (dict)
        """
        url = self.base_url + "/groups"
        r = self._request(
            "POST",
            url,
            params={
                "workspaceV2": "true"
            },  # we always use the "new experience" workspaces
            json={"name": name},
            raise_=False,
        )
        raise_if_already_exists_error(r)
        raise_if_err(r)
        return r.json()

    def set_service_principal_permission_to_workspace(
        self,
        workspace_id: str,
        service_principal_id: str,
        permission: WorkspaceUserPermission,
    ) -> None:
        request_data = {
            "identifier": service_principal_id,
            "groupUserAccessRight": permission.value,
            "principalType": "App",
        }
        self._set_user_permission_to_workspace(workspace_id, request_data)

    def set_user_permission_to_workspace(
        self,
        workspace_id: str,
        email: str,
        permission: WorkspaceUserPermission,
    ) -> None:
        request_data = {
            "identifier": email,
            "groupUserAccessRight": permission.value,
            "principalType": "User",
        }
        self._set_user_permission_to_workspace(workspace_id, request_data)

    def _set_user_permission_to_workspace(
        self, workspace_id, request_data: dict
    ) -> None:
        url = self.base_url + f"/groups/{workspace_id}/users"
        r = self._request("PUT", url, json=request_data, raise_=False)
        if r.status_code == 404:
            self._request("POST", url, json=request_data)
        else:
            raise_if_err(r)

    def rebind_report_to_dataset(
        self,
        workspace_id: str,
        report_id: str,
        dataset_id: str,
    ):
        url = self.base_url + "/groups/{}/reports/{}/Rebind".format(
            workspace_id, report_id
        )
        r = self._request(
            "POST",
            url,
            json={"datasetId": dataset_id},
        )

    def create_push_dataset(
        self,
        workspace_id: str,
        name: str,
        table: DatasetTable,
        fifo_retention_policy=True,
    ) -> dict:
        url = self.base_url + "/groups/{}/datasets".format(workspace_id)
        query_params = {}
        if fifo_retention_policy:
            query_params["defaultRetentionPolicy"] = "basicFIFO"
        r = self._request(
            "POST",
            url,
            params=query_params,
            json={
                "name": name,
                "tables": [table],
                # "datasources": [],
                "defaultMode": "Push",
                # "relationships": [],
            },
        )
        return r.json()

    def update_push_dataset_table_schema(
        self, workspace_id: str, dataset_id: str, table: DatasetTable
    ):
        url = self.base_url + "/groups/{}/datasets/{}/tables/{}".format(
            workspace_id, dataset_id, table["name"]
        )
        self._request(
            "PUT",
            url,
            json=table,
        )

    def truncate_push_dataset_table(
        self, workspace_id: str, dataset_id: str, table_name: str
    ):
        url = self.base_url + "/groups/{}/datasets/{}/tables/{}/rows".format(
            workspace_id, dataset_id, table_name
        )
        self._request(
            "DELETE",
            url,
        )

    def push_rows(
        self, workspace_id: str, dataset_id: str, table_name: str, rows: list[dict]
    ):
        url = self.base_url + "/groups/{}/datasets/{}/tables/{}/rows".format(
            workspace_id, dataset_id, table_name
        )

        r = self._request(
            "POST",
            url,
            json={"rows": rows},
        )

    def delete_user(self, workspace_id: str, user_email: str):
        url = self.base_url + "/groups/{}/users/{}".format(
            workspace_id, user_email
        )

        r = self._request("DELETE", url)
