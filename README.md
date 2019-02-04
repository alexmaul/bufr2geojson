# bufr2geojson

Read file given at command-line and convert all contained BUFR messages to
GeoJSON format.

## GeoJSON structure

All feature properties representing a descriptor value are named with the
prefix `data_`, followed by a sequence number.

To assure the descriptor values are in the right order, sort all keys for
iteration.

    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude, altitude]
                },
                "properties": {
                    "abbreviated_heading": "...",
                    "data_00": { "name": "...",
                        "value": ...,
                        "unit": "..."
                    },
                    "data_01": { "name": "...",
                        "value": ...,
                        "unit": "..."
                    },
                    ...
                }
            },
        ]
    }
