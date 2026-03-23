import json
from pathlib import Path

import openeo
from openeo.api.process import Parameter
from openeo.rest.udp import build_process_dict

from meteo_downscaling_openeo.run_agera_downscaler import run, downscaled_temperature_humidity_radiation_cube

if __name__ == '__main__':
    spatial_extent = Parameter.spatial_extent()
    temporal_extent = Parameter.temporal_interval()
    c = openeo.connect("openeo.vito.be").authenticate_oidc()
    cube = downscaled_temperature_humidity_radiation_cube(c, spatial_extent,temporal_extent)

    returns = {
        "description": "A data cube with the newly computed values.\n\nAll dimensions stay the same, except for the dimensions specified in corresponding parameters. There are three cases how the dimensions can change:\n\n1. The source dimension is the target dimension:\n   - The (number of) dimensions remain unchanged as the source dimension is the target dimension.\n   - The source dimension properties name and type remain unchanged.\n   - The dimension labels, the reference system and the resolution are preserved only if the number of values in the source dimension is equal to the number of values computed by the process. Otherwise, all other dimension properties change as defined in the list below.\n2. The source dimension is not the target dimension. The target dimension exists with a single label only:\n   - The number of dimensions decreases by one as the source dimension is 'dropped' and the target dimension is filled with the processed data that originates from the source dimension.\n   - The target dimension properties name and type remain unchanged. All other dimension properties change as defined in the list below.\n3. The source dimension is not the target dimension and the latter does not exist:\n   - The number of dimensions remain unchanged, but the source dimension is replaced with the target dimension.\n   - The target dimension has the specified name and the type other. All other dimension properties are set as defined in the list below.\n\nUnless otherwise stated above, for the given (target) dimension the following applies:\n\n- the number of dimension labels is equal to the number of values computed by the process,\n- the dimension labels are incrementing integers starting from zero,\n- the resolution changes, and\n- the reference system is undefined.",
        "schema": {
            "type": "object",
            "subtype": "datacube"
        }
    }
    udp = build_process_dict(cube, "agera_meteo_mountain_downscaling",
                             "Computes meteo variables (temperature, humidity, shortwave radiation) downscaled to mountain areas based on AGERA5 and DEM data.",
                             description="Computes meteo variables (temperature, humidity, shortwave radiation) downscaled to mountain areas based on AGERA5 and DEM data. A simply physical model is used. Ported from original: [https://github.com/bare92/micropyzzotmet/](micropyzzotmet)",
                             links=[{"rel": "about", "href": "https://github.com/bare92/micropyzzotmet/](micropyzzotmet"}],
                             categories=["meteo"],
                             parameters=[spatial_extent, temporal_extent], returns=returns,
                             default_job_options={"executor-memory": "10G"})

    with open(Path(__file__).parent / "agera_meteo_mountain_downscaling.json", "w+") as f:
        json.dump(udp, f, indent=2)