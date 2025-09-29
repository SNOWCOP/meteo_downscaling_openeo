import openeo

from snowcop.downscaling.downscale_variables import downscale_shortwave_radiation


def run(spatial_extent):

    c = openeo.connect("openeo-dev.vito.be").authenticate_oidc()



    temporal_extent = ["2025-07-01","2025-07-03"]
    dem = c.load_collection("COPERNICUS_30", spatial_extent=spatial_extent)
    agera = c.load_collection("AGERA5", spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=["temperature-mean","solar-radiation-flux"])
    #agera.result_node().update_arguments(featureflags={"tilesize": 1})
    geopotential = c.load_stac("https://artifactory.vgt.vito.be/artifactory/auxdata-public/geopotential.json", spatial_extent=spatial_extent, bands=["geopotential"])

    from snowcop.downscaling.downscale_variables import downscale_temperature_humidity
    downscale_temperature_humidity(agera, dem, geopotential).execute_batch(format="netCDF", job_options={"executor-memory": "8G"})

    shortwave_rad_cube = downscale_shortwave_radiation(agera, None)
    #shortwave_rad_cube.execute_batch(format="netCDF",filename_prefix="shortwave_radiation_")

