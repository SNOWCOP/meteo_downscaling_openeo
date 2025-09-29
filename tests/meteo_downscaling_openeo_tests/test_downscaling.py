from pathlib import Path

import openeo
import pytest
import xarray


def test_run():

    from snowcop.downscaling.run_agera_downscaler import run
    run(spatial_extent)

@pytest.fixture
def openeoplatform_connection() -> openeo.Connection:
    return openeo.connect("openeo-dev.vito.be").authenticate_oidc()

spatial_extent = {
        "south": 5816500,
        "north": 5816500 + 1024*30,
        "west": 271000,
        "east": 271000 + 1024*30,
        "crs": "EPSG:32719"
}

temporal_extent = "2025-07"

@pytest.fixture
def local_agera(openeoplatform_connection):


    #dem = c.load_collection("COPERNICUS_30", spatial_extent=spatial_extent).max_time()
    out = Path(__file__).parent / "testdata" / "agera_temperature.nc"
    if not out.exists():

        openeoplatform_connection.load_collection("AGERA5", spatial_extent=spatial_extent, temporal_extent=temporal_extent,
                              bands=["temperature-mean"]).download(str(out))
    return out

@pytest.fixture
def local_shortwave_radiation(openeoplatform_connection):


    #dem = c.load_collection("COPERNICUS_30", spatial_extent=spatial_extent).max_time()
    out = Path(__file__).parent / "testdata" / "agera_shortwave_radiation.zarr.zip"
    if not out.exists() and not (out.parent / "agera_shortwave_radiation.zarr").exists():

        openeoplatform_connection.load_collection("AGERA5", spatial_extent=spatial_extent, temporal_extent=temporal_extent,
                              bands=["solar-radiation-flux"]).execute_batch(str(out), format="ZARR")
    return out.parent / "agera_shortwave_radiation.zarr"

@pytest.fixture
def local_shortwave_radiation_netcdf(openeoplatform_connection):


    #dem = c.load_collection("COPERNICUS_30", spatial_extent=spatial_extent).max_time()
    out = Path(__file__).parent / "testdata" / "agera_shortwave_radiation.nc"
    if not out.exists() :

        openeoplatform_connection.load_collection("AGERA5", spatial_extent=spatial_extent, temporal_extent=temporal_extent,
                              bands=["solar-radiation-flux"]).download(str(out), format="netCDF", options = dict(strict_cropping=False))
    return out



@pytest.fixture
def local_dem(openeoplatform_connection):


    out = Path(__file__).parent / "testdata" / "copernicus_dem.nc"
    if not out.exists():
        dem = openeoplatform_connection.load_collection("COPERNICUS_30", spatial_extent=spatial_extent).max_time()
        dem.download(str(out))
    return out

def test_local_run(local_agera, local_dem):
    from openeo.local import LocalConnection
    local_conn = LocalConnection("./")

    agera = local_conn.load_collection(str(local_agera))
    dem = local_conn.load_collection(str(local_dem))
    from snowcop.downscaling.downscale_variables import downscale_temperature_humidity
    downscale_temperature_humidity(agera, dem, None).execute()

def test_local_shortwave_radiation(local_shortwave_radiation_job):
    from openeo.local import LocalConnection
    local_conn = LocalConnection("./")

    agera = local_conn.load_collection(str(local_shortwave_radiation_job))

    from snowcop.downscaling.downscale_variables import downscale_shortwave_radiation
    downscale_shortwave_radiation(agera,  None).execute()

@pytest.fixture
def local_shortwave_radiation_job(openeoplatform_connection):


    #dem = c.load_collection("COPERNICUS_30", spatial_extent=spatial_extent).max_time()
    out = Path(__file__).parent / "testdata" / "agera_shortwave_radiation.nc"


    return openeoplatform_connection.load_collection("AGERA5", spatial_extent=spatial_extent, temporal_extent=temporal_extent,
                              bands=["solar-radiation-flux"]).execute_batch(str(out),title="agera_shortwave_netcdf", format="netCDF", options = dict(strict_cropping=False))


def test_localservice_shortwave_radiation():
    from openeo.local import LocalConnection
    local_conn = openeo.connect("http://localhost:8080/").authenticate_basic("openeo", "openeo")

    agera = local_conn.load_stac("https://openeo-dev.vito.be/openeo/1.2/jobs/j-25091807142441ddbd47f78a41d1656e/results/ZGZhNjc4Y2I5YWIxN2Y2NWQ0ZjAyNWUzMGZhYzVlMGQ5MDExNjE3NmU0NGZkMTdkNzAzNDE5MzIyNzQ3Y2JiZEBlZ2kuZXU=/c6cc84e74bfddbf296d72a782970b229?expires=1758784773")

    from snowcop.downscaling.downscale_variables import downscale_shortwave_radiation
    downscale_shortwave_radiation(agera,  None).download("local_result.nc")