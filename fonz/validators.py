from typing import List, Optional, Sequence, DefaultDict
import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
import aiohttp
from fonz.client import LookerClient
from fonz.lookml import Project, Model, Explore, Dimension
from fonz.logger import GLOBAL_LOGGER as logger
from fonz.exceptions import SqlError, FonzException


class Validator(ABC):  # pragma: no cover
    """Defines abstract base interface for validators.

    Not intended to be used directly, only inherited.

    Attributes:
        client: Looker API client.

    """

    def __init__(self, client: LookerClient):
        self.client = client

    @abstractmethod
    def validate(self):
        raise NotImplementedError


class SqlValidator(Validator):
    """Runs and validates the SQL for each selected LookML dimension.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    Attributes:
        timeout: aiohttp object to limit duration of running requests.
        project: LookML project object representation.

    """

    timeout = aiohttp.ClientTimeout(total=300)

    def __init__(self, client: LookerClient, project: str):
        super().__init__(client)
        self.project = Project(project, models=[])

    @staticmethod
    def parse_selectors(selectors: List[str]) -> DefaultDict[str, set]:
        """Parses explore selectors with the format 'model_name.explore_name'.

        Args:
            selectors: List of selector strings in 'model_name.explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name.*' would select all explores in the 'model_name' model.

        Returns:
            DefaultDict[str, set]: A hierarchy of selected model names (keys) and
                explore names (values).

        """
        selection: DefaultDict = defaultdict(set)
        for selector in selectors:
            try:
                model, explore = selector.split(".")
            except ValueError:
                raise FonzException(
                    f"Explore selector '{selector}' is not valid.\n"
                    "Instead, use the format 'model_name.explore_name'. "
                    f"Use 'model_name.*' to select all explores in a model."
                )
            else:
                selection[model].add(explore)
        return selection

    # TODO: Refactor this so it's more obvious how selection works
    def _select(self, choices: Sequence[str], select_from: Sequence) -> Sequence:
        unique_choices = set(choices)
        select_from_names = set(each.name for each in select_from)
        difference = unique_choices.difference(select_from_names)
        if difference:
            raise FonzException(
                f"{select_from[0].__class__.__name__}"
                f'{"" if len(difference) == 1 else "s"} '
                + ", ".join(difference)
                + f" not found in LookML for project '{self.project.name}'."
            )
        return [each for each in select_from if each.name in unique_choices]

    def build_project(self, selectors: List[str]) -> None:
        """Creates an object representation of the project's LookML.

        Args:
            selectors: List of selector strings in 'model_name.explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name.*' would select all explores in the 'model_name' model.

        """
        selection = self.parse_selectors(selectors)
        logger.info(
            f"Building LookML project hierarchy for project {self.project.name}."
        )

        all_models = [Model.from_json(model) for model in self.client.get_models()]
        project_models = [
            model for model in all_models if model.project == self.project.name
        ]

        # Expand wildcard operator to include all specified or discovered models
        selected_model_names = selection.keys()
        if "*" in selected_model_names:
            explore_names = selection.pop("*")
            for model in project_models:
                selection[model.name].update(explore_names)

        selected_models = self._select(
            choices=tuple(selection.keys()), select_from=project_models
        )

        for model in selected_models:
            # Expand wildcard operator to include all specified or discovered explores
            selected_explore_names = selection[model.name]
            if "*" in selected_explore_names:
                selected_explore_names.remove("*")
                selected_explore_names.update(
                    set(explore.name for explore in model.explores)
                )

            selected_explores = self._select(
                choices=tuple(selected_explore_names), select_from=model.explores
            )

            for explore in selected_explores:
                dimensions_json = self.client.get_dimensions(model.name, explore.name)
                for dimension_json in dimensions_json:
                    dimension = Dimension.from_json(dimension_json)
                    dimension.url = self.client.base_url + dimension.url
                    if not dimension.ignore:
                        explore.add_dimension(dimension)

            model.explores = selected_explores

        self.project.models = selected_models

    def _validate_explore(
        self, model: Model, explore: Explore, batch: bool = False
    ) -> List[SqlError]:
        """Queries selected dimensions in an explore and returns any errors.

        Args:
            model: Object representation of LookML model.
            explore: Object representation of LookML explore.
            batch: When true, runs one query per explore (using all dimensions). When
                false, runs one query per dimension. Batch mode increases query speed
                but can only return the first error encountered for each dimension.

        Returns:
            List[SqlError]: SqlErrors encountered while querying the explore.

        """
        loop = asyncio.get_event_loop()
        tasks = []

        if batch:
            task = loop.create_task(self._query_explore(model, explore))
            tasks.append(task)
        else:
            for dimension in explore.dimensions:
                task = loop.create_task(
                    self._query_dimension(model, explore, dimension)
                )
                tasks.append(task)

        validation_errors = loop.run_until_complete(asyncio.gather(*tasks))
        return [error for error in validation_errors if error]

    def validate(self, batch: bool = False) -> List[SqlError]:
        """Queries selected explores and returns any errors.

        Args:
            batch: When true, runs one query per explore (using all dimensions). When
                false, runs one query per dimension. Batch mode increases query speed
                but can only return the first error encountered for each dimension.

        Returns:
            List[SqlError]: SqlErrors encountered while querying the explore.

        """
        explore_count = self._count_explores()
        logger.info(
            f"Begin testing {explore_count} "
            f"{'explore' if explore_count == 1 else 'explores'} "
            f"({'batch' if batch else 'single-dimension'} mode)."
        )
        index = 0

        validation_errors = []
        for model in self.project.models:
            for explore in model.explores:
                index += 1
                logger.info(f"Starting SQL validation for explore '{explore.name}'.")
                validation_errors.extend(self._validate_explore(model, explore, batch))
                if explore.errored:
                    logger.info(f"Explore '{explore.name}' failed SQL validation.")
                else:
                    logger.info(f"Explore '{explore.name}' passed SQL validation.")

        return validation_errors

    def _get_error_from_api_result(self, result: dict) -> dict:
        """Extracts the relevant error parameters from an API result.

        Args:
            result: JSON dictionary query result returned by the API.

        Returns:
            dict: Relevant error parameters extracted from the API result.

        """
        error = result["errors"][0]
        message = error["message_details"]
        # Subtract one to account for the comment Looker appends to queries
        if error.get("sql_error_loc"):
            line_number = error["sql_error_loc"]["line"] - 1
        else:
            line_number = None
        return {"sql": result["sql"], "line_number": line_number, "message": message}

    async def _run_async_query(self, model: str, explore: str, dimensions: List[str]):
        """Executes an asynchronous Looker query and returns the result.

        Args:
            model: LookML model name.
            explore: LookML explore name.
            dimensions: List of LookML dimension names.

        Returns:
            Union[list, dict]: API query result. At time of writing, the Looker Async
                Query endpoint returns a list for a successful query and a dict for an
                unsuccessful one.

        """
        async with aiohttp.ClientSession(
            headers=self.client.session.headers,
            raise_for_status=True,
            timeout=self.timeout,
        ) as async_session:
            query_id = await self.client.create_query(
                async_session, model, explore, dimensions
            )
            query_task_id = await self.client.run_query(async_session, query_id)
            result = await self.client.get_query_results(async_session, query_task_id)
            return result

    async def _query_explore(
        self, model: Model, explore: Explore
    ) -> Optional[SqlError]:
        """Creates and executes a query with a single explore.

        Args:
            model: Object representation of LookML model.
            explore: Object representation of LookML explore.

        Returns:
            Optional[SqlError]: SqlError encountered while querying the explore. If no
                error occurs, returns None.

        """
        dimensions = [dimension.name for dimension in explore.dimensions]
        result = await self._run_async_query(model.name, explore.name, dimensions)

        if isinstance(result, list):
            return None
        elif isinstance(result, dict) and result.get("errors"):
            error_params = self._get_error_from_api_result(result)
            path = f"{model.name}/{explore.name}"
            error = SqlError(path=path, **error_params)

            explore.error = error
            explore.errored = True
            model.errored = True

            return error
        else:
            raise FonzException(
                f"Unexpected API result for explore '{model.name}/{explore.name}'. "
                f"API result obtained: {result}"
            )

    async def _query_dimension(
        self, model: Model, explore: Explore, dimension: Dimension
    ) -> Optional[SqlError]:
        """Creates and executes a query with a single dimension.

        Args:
            model: Object representation of LookML model.
            explore: Object representation of LookML explore.
            dimension: Object representation of LookML dimension.

        Returns:
            Optional[SqlError]: SqlError encountered while querying the explore. If no
                error occurs, returns None.

        """
        result = await self._run_async_query(model.name, explore.name, [dimension.name])

        if isinstance(result, list):
            return None
        elif isinstance(result, dict) and result.get("errors"):
            error_params = self._get_error_from_api_result(result)
            path = f"{model.name}/{dimension.name}"
            error = SqlError(path=path, url=dimension.url, **error_params)

            dimension.error = error
            dimension.errored = True
            explore.errored = True
            model.errored = True

            return error
        else:
            raise FonzException(
                f"Unexpected API result for dimension '{model.name}/{dimension.name}'. "
                f"API result obtained: {result}"
            )

    def _count_explores(self) -> int:
        """Counts the explores in the LookML project hierarchy.

        Returns:
            int: The number of explores in the LookML project.

        """
        explore_count = 0
        for model in self.project.models:
            explore_count += len(model.explores)
        return explore_count
