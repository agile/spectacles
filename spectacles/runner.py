from typing import List
from spectacles.client import LookerClient
from spectacles.validators import SqlValidator, DataTestValidator
from spectacles.utils import log_duration


class Runner:
    """Runs validations and returns JSON-style dictionaries with validation results.

    Args:
        base_url: Base URL for the Looker instance, e.g. https://mycompany.looker.com.
        project: Name of the Looker project to use.
        branch: Name of the Git branch to check out.
        client_id: Looker API client ID.
        client_secret: Looker API client secret.
        port: Desired API port to use for requests.
        api_version: Desired API version to use for requests.
        remote_reset: reset state of branch in Looker to match remote. (reset --hard)
        ref: The Git ref to check out. (this will use a reset --hard in Looker)

    Attributes:
        client: Looker API client used for making requests.

    """

    def __init__(
        self,
        base_url: str,
        project: str,
        branch: str,
        client_id: str,
        client_secret: str,
        port: int = 19999,
        api_version: float = 3.1,
        remote_reset: bool = False,
        ref: str = None,
    ):
        self.project = project
        self.client = LookerClient(
            base_url, client_id, client_secret, port, api_version
        )
        self.client.update_session(project, branch)
        self.client.git_branch(project, branch, ref)
        if remote_reset:
            self.client.reset_to_remote(project, branch)

    @log_duration
    def validate_sql(
        self, selectors: List[str], mode: str = "batch", concurrency: int = 10
    ) -> List[dict]:
        sql_validator = SqlValidator(self.client, self.project, concurrency)
        sql_validator.build_project(selectors)
        errors = sql_validator.validate(mode)
        return [vars(error) for error in errors]

    @log_duration
    def validate_data_tests(self):
        data_test_validator = DataTestValidator(self.client, self.project)
        errors = data_test_validator.validate()
        return [vars(error) for error in errors]
