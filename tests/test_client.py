from unittest.mock import Mock, patch
import pytest
import requests
import asynctest
import aiohttp
from spectacles.client import LookerClient
from spectacles.exceptions import ApiConnectionError

TEST_BASE_URL = "https://test.looker.com"
TEST_CLIENT_ID = "test_client_id"
TEST_CLIENT_SECRET = "test_client_secret"


@pytest.fixture
def client(monkeypatch):
    mock_authenticate = Mock(spec=LookerClient.authenticate)
    monkeypatch.setattr(LookerClient, "authenticate", mock_authenticate)
    return LookerClient(TEST_BASE_URL, TEST_CLIENT_ID, TEST_CLIENT_SECRET)


@pytest.fixture
def mock_response(code=404):
    mock = Mock(spec=requests.Response)
    mock.status_code = code
    if code in [404]:
        mock.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "An HTTP error occurred."
        )
    return mock


@patch("spectacles.client.requests.Session.post")
def test_bad_authenticate_raises_connection_error(mock_post, mock_response):
    mock_post.return_value = mock_response
    with pytest.raises(ApiConnectionError):
        LookerClient(TEST_BASE_URL, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    mock_response.raise_for_status.assert_called_once()


@patch("spectacles.client.requests.Session.post")
def test_authenticate_sets_session_headers(mock_post, monkeypatch):
    mock_looker_version = Mock(spec=LookerClient.get_looker_release_version)
    mock_looker_version.return_value("1.2.3")
    monkeypatch.setattr(LookerClient, "get_looker_release_version", mock_looker_version)

    mock_post_response = Mock(spec=requests.Response)
    mock_post_response.json.return_value = {"access_token": "test_access_token"}
    mock_post.return_value = mock_post_response
    client = LookerClient(TEST_BASE_URL, TEST_CLIENT_ID, TEST_CLIENT_SECRET)
    assert client.session.headers == {"Authorization": f"token test_access_token"}


@patch("spectacles.client.requests.Session.patch")
def test_update_session_uses_prod_for_master(mock_patch, client, mock_response):
    mock_patch.return_value = mock_response(200)

    client.update_session(project="test_project", branch="master")
    mock_patch.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/session",
        json={"workspace_id": "production"},
    )


@patch("spectacles.client.requests.Session.patch")
def test_update_session_uses_dev_for_others(mock_patch, client, mock_response):
    mock_patch.return_value = mock_response(200)

    client.update_session(project="test_project", branch="notmaster")
    mock_patch.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/session",
        json={"workspace_id": "dev"},
    )


@patch("spectacles.client.requests.Session.patch")
def test_bad_update_session_patch_raises_connection_error(
    mock_patch, client, mock_response
):
    mock_patch.return_value = mock_response
    with pytest.raises(ApiConnectionError):
        client.update_session(project="test_project", branch="test_branch")
    mock_response.raise_for_status.assert_called_once()


@patch("spectacles.client.requests.Session.put")
def test_git_branch_master_does_nothing(mock_put, client, mock_response):
    mock_put.return_value = mock_response(200)
    client.git_branch(project="test_project", branch="master")

    mock_put.assert_not_called()


@patch("spectacles.client.requests.Session.put")
def test_git_branch_checks_out_named_branch(mock_put, client, mock_response):
    mock_put.return_value = mock_response(200)
    client.git_branch(project="test_project", branch="notmaster")

    mock_put.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/projects/test_project/git_branch",
        json={"name": "notmaster"},
    )


@patch("spectacles.client.requests.Session.put")
def test_git_branch_passes_ref(mock_put, client, mock_response):
    mock_put.return_value = mock_response(200)
    client.git_branch(project="test_project", branch="notmaster", ref="123abc")

    mock_put.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/projects/test_project/git_branch",
        json={"name": "notmaster", "ref": "123abc"},
    )


@patch("spectacles.client.requests.Session.post")
def test_reset_to_remote_master_does_nothing(mock_post, client, mock_response):
    mock_post.return_value = mock_response(200)
    client.reset_to_remote(project="test_project", branch="master")

    mock_post.assert_not_called()


@patch("spectacles.client.requests.Session.post")
def test_reset_to_remote(mock_post, client, mock_response):
    mock_post.return_value = mock_response(200)
    client.reset_to_remote(project="test_project", branch="notmaster")

    url = "https://test.looker.com:19999/api/3.1/projects/test_project/reset_to_remote"
    mock_post.assert_called_once_with(url=url)


@patch("spectacles.client.requests.Session.get")
def test_bad_get_lookml_models_raises_connection_error(mock_get, client, mock_response):
    mock_get.return_value = mock_response
    with pytest.raises(ApiConnectionError):
        client.get_lookml_models()
    mock_response.raise_for_status.assert_called_once()


@patch("spectacles.client.requests.Session.get")
def test_bad_get_lookml_dimensions_raises_connection_error(
    mock_get, client, mock_response
):
    mock_get.return_value = mock_response
    with pytest.raises(ApiConnectionError):
        client.get_lookml_dimensions(model="test_model", explore="test_explore")
    mock_response.raise_for_status.assert_called_once()


@pytest.mark.asyncio
@asynctest.patch("aiohttp.ClientSession.post")
async def test_create_query(mock_post, client):
    QUERY_ID = 124950204921
    mock_post.return_value.__aenter__.return_value.json = asynctest.CoroutineMock(
        return_value={"id": QUERY_ID}
    )
    async with aiohttp.ClientSession() as session:
        query_id = await client.create_query(
            session,
            "test_model",
            "test_explore_one",
            ["dimension_one", "dimension_two"],
        )
    assert query_id == QUERY_ID
    mock_post.assert_called_once_with(
        url="https://test.looker.com:19999/api/3.1/queries",
        json={
            "model": "test_model",
            "view": "test_explore_one",
            "fields": ["dimension_one", "dimension_two"],
            "limit": 0,
            "filter_expression": "1=2",
        },
    )
