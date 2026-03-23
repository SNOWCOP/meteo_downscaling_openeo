import openeo

from meteo_downscaling_openeo.downscale_variables import downscale_shortwave_radiation, downscale_temperature_humidity


def run(spatial_extent):

    c = openeo.connect("openeo-dev.vito.be").authenticate_oidc()

    shortwave_rad_cube = downscaled_temperature_humidity_radiation_cube(c, spatial_extent)
    shortwave_rad_cube.execute_batch(title="SNOWCOP Downscaling radiation", format="netCDF",filename_prefix="shortwave_radiation_",
                                                                                      job_options={
                                                                                          "executor-memory": "8G"})


def downscaled_temperature_humidity_radiation_cube(c, spatial_extent, temporal_extent = ["2024-07-01", "2024-07-10"]):

    dem = c.load_collection("COPERNICUS_30", spatial_extent=spatial_extent)
    agera = c.load_collection("AGERA5", spatial_extent=spatial_extent, temporal_extent=temporal_extent,
                              bands=["temperature-mean", "dewpoint-temperature", "solar-radiation-flux"])
    #agera.result_node().update_arguments(featureflags={"tilesize": 64})
    geopotential = c.load_stac("https://artifactory.vgt.vito.be/artifactory/auxdata-public/geopotential_bboxfixed.json",
                               spatial_extent=spatial_extent, bands=["geopotential"])
    #geopotential.result_node().update_arguments(featureflags={"tilesize": 64})
    geopotential.metadata = geopotential.metadata.add_dimension("t", label="2025-09-29", type="temporal")
    temperature_humidity = downscale_temperature_humidity(agera, dem, geopotential.max_time())

    dem_notime = dem.max_time()
    slope_aspect = dem_notime.aspect().merge_cubes(dem_notime.slope()).rename_labels(dimension="bands", target=["aspect", "slope"])
    shortwave_rad_cube = downscale_shortwave_radiation(agera, slope_aspect)
    return temperature_humidity.merge_cubes(shortwave_rad_cube)

