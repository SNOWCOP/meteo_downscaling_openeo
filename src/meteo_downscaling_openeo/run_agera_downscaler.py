import openeo

from .downscale_variables import downscale_shortwave_radiation, downscale_temperature_humidity


def run(spatial_extent):

    c = openeo.connect("openeo-dev.vito.be").authenticate_oidc()

    temporal_extent = ["2024-07-01","2024-07-05"]
    dem = c.load_collection("COPERNICUS_30", spatial_extent=spatial_extent)
    agera = c.load_collection("AGERA5", spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=["temperature-mean","dewpoint-temperature","solar-radiation-flux"])
    agera.result_node().update_arguments(featureflags={"tilesize": 1})
    geopotential = c.load_stac("https://artifactory.vgt.vito.be/artifactory/auxdata-public/geopotential.json", spatial_extent=spatial_extent, bands=["geopotential"])
    geopotential.result_node().update_arguments(featureflags={"tilesize": 1})
    geopotential.metadata = geopotential.metadata.add_dimension("t", label="2025-09-29", type="temporal")

    downscale_temperature_humidity(agera, dem, geopotential.max_time()).execute_batch( job_options={"executor-memory": "6G", "load_stac_apply_lcfm_improvements":True})

    #shortwave_rad_cube = downscale_shortwave_radiation(agera, None)
    #shortwave_rad_cube.execute_batch(format="netCDF",filename_prefix="shortwave_radiation_")

