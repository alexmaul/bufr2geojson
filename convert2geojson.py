#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Read file given at command-line and convert all contained BUFR messages to
GeoJSON format.

(C) 2019 DWD/amaul

GeoJSON structure
-----------------
All feature properties representing a descriptor value are named with the
prefix "data_", followed by a sequence number.
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
"""
import sys
import os
from json import dump
from datetime import datetime
from gzip import open as gzip_open
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from trollbufr.bufr import Bufr
from trollbufr.coder.bufr_types import TabBType
import trollbufr.load_file

import logging
logger = logging.getLogger()


def parse_args():
    # Setup command-line argument parser
    parser = ArgumentParser(description=__import__("__main__").__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="count",
                        help="Log-Level [default: 0]"
                        )
    parser.add_argument("--amtl",
                          action="store_true",
                          help="Only station with WMO-ID (IIiii)"
                          )
    parser.add_argument("--jsonp",
                          action="store_true",
                          help="Return JSON-P"
                          )
    parser.add_argument(dest="filename",
                        help="File with BUFR messages",
                        metavar="file",
                        nargs=1
                        )
    args = parser.parse_args()
    # Setup logging
    log_formater_line = "[%(asctime)s %(levelname)s] %(message)s"
    if not args.verbose:
        loglevel = logging.WARN
    else:
        if args.verbose == 1:
            loglevel = logging.INFO
        elif args.verbose >= 2:
            loglevel = logging.DEBUG
            log_formater_line = "[%(asctime)s %(levelname)s %(module)s:%(lineno)d] %(message)s".format
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(log_formater_line, "%Y-%m-%d %H:%M:%S"))
    handler.setLevel(loglevel)
    logging.getLogger("").setLevel(loglevel)
    logging.getLogger("").addHandler(handler)
    return args


def runner(args):
    bufr = Bufr(os.environ["BUFR_TABLES_TYPE"], os.environ["BUFR_TABLES"])
    with open(args.filename[0], "rb") as fh_in:
        bufr_data = fh_in.read()
    if args.amtl:
        station_descr = (1002,)
    else:
        station_descr = (1002, 1018)
    try:
        with gzip_open("%s.geojson.gz" % args.filename[0], "wt") as fh_out:
            i = 0
            if args.jsonp:
                fh_out.write('appendData( ')
            fh_out.write('{ "type" : "FeatureCollection",\n')
            fh_out.write('"datetime_current" : "%s",\n' % (
                datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            ))
            fh_out.write('"features" : [')
            for blob, size, header in trollbufr.load_file.next_bufr(bin_data=bufr_data):
                bufr.decode_meta(blob)
                tabl = bufr.get_tables()
                for report in bufr.next_subset():
                    station_accepted = False
                    feature_set = {"type": "Feature",

                                    "geometry": {"type": "Point", "coordinates": []},
                                    "properties": {}
                                    }
                    feature_coordinates = [0, 0, 0]
                    feature_properties = {"abbreviated_heading": header}
                    try:
                        j = 0
                        for descr_entry in report.next_data():
                            if descr_entry.mark is not None:
                                continue
                            if descr_entry.descr in (5001, 5002, 27001, 27002):
                                feature_coordinates[1] = descr_entry.value
                                continue
                            if descr_entry.descr in (6001, 6002, 28001, 28002):
                                feature_coordinates[0] = descr_entry.value
                                continue
                            if descr_entry.descr in (7001, 7002, 7007, 7030, 10007) and descr_entry.value:
                                feature_coordinates[2] = descr_entry.value
                                continue
                            if descr_entry.descr in station_descr and descr_entry.value is not None:
                                station_accepted = True
                            # d_name, d_unit, d_typ
                            d_info = tabl.lookup_elem(descr_entry.descr)
                            if d_info.unit.upper() in ("CCITT IA5", "NUMERIC", "CODE TABLE", "FLAG TABLE"):
                                d_unit = None
                            else:
                                d_unit = d_info.unit
                            if descr_entry.value is None or d_info.type in (TabBType.NUMERIC, TabBType.LONG, TabBType.DOUBLE):
                                d_value = descr_entry.value
                            elif d_info.type in (TabBType.CODE, TabBType.FLAG) and descr_entry.value is not None:
                                d_value = tabl.lookup_codeflag(descr_entry.descr,
                                                               descr_entry.value)
                            else:
                                d_value = str(descr_entry.value).decode("latin1")
                            feature_properties["data_%03d" % (j)] = {"name": d_info.name, "value": d_value}
                            if d_info.shortname is not None:
                                feature_properties["data_%03d" % (j)]["shortname"] = d_info.shortname
                            if d_unit is not None:
                                feature_properties["data_%03d" % (j)]["unit"] = str(d_unit)
                            j += 1
                    except Exception as e:
                        station_accepted = False
                        if "Unknown descriptor" not in str(e):
                            raise e
                    if station_accepted:
                        if i:
                            fh_out.write(",\n")
                        i += 1
                        feature_set["geometry"]["coordinates"] = feature_coordinates
                        feature_set["properties"] = feature_properties
                        dump(feature_set, fh_out, indent=3, separators=(',', ': '))
            fh_out.write(']\n}\n')
            if args.jsonp:
                fh_out.write(');\n')
    except Exception as e:
        logger.info(e, exc_info=1)
    return 0


if __name__ == "__main__":
    args = parse_args()
    sys.exit(runner(args))
