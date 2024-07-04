from unittest import mock
import daily_weather.main as daily_weather


@mock.patch("requests.get")
def test_get_coordinates_by_zip(mock_get):
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "zip": "85374",
        "name": "Surprise",
        "lat": 33.63,
        "lon": -112.3314,
        "country": "US",
    }
    mock_get.return_value = mock_response
    data = daily_weather.get_coordinates_by_zip("85374")
    assert data == (33.63, -112.3314)
