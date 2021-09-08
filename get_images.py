import logging
import math
import shutil
import os

import ee
import geetools

logger = logging.getLogger(__name__)

ee.Initialize()

def bounding_box_from_point(
      lat: float,
      lon: float,
      dist: int=1000):
    """
    Arguments:
        point: coordinate point (lat, long)
        dist: radial distance from the point in meters
    Returns a bounding box (south, west, north, east) centered on that point
        with side length dist formula from:
        http://www.movable-type.co.uk/scripts/latlong.html#rhumblines
    """

    earth_radius = 6371000  # meters
    angular_distance = math.degrees(0.5 * (dist / earth_radius))

    delta_lat = angular_distance
    delta_lon = angular_distance / math.cos(math.radians(lat))

    s, n = lat - delta_lat, lat + delta_lat
    w, e = lon - delta_lon, lon + delta_lon
    return ee.Geometry.Rectangle([w, s, e, n], "EPSG:4326", False)

def get_image(bounding_box, year, bands=["B8"]):
    collection = (
        ee.ImageCollection("LANDSAT/LE07/C01/T1")
        .filterBounds(bounding_box)
        .filterDate(f"{year}-01-01", f"{year+1}-01-01")
        .select(bands)
        .filterMetadata('CLOUD_COVER', 'less_than', 33))

    return ee.Image(collection.mosaic()).clip(bounding_box)

def image_to_disk(
        image: ee.image.Image,
        filename: str,
        scale: float=1):
    geetools.batch.image.toLocal(
        image,
        name=filename,
        scale=scale)

def download_image(
        lat, lon, year,
        filename, destination_folder,
        index,
        dist=2000,
        bands=["B8"],
        force=False):

    destination_path = os.path.join(destination_folder, f"{filename}.tif")

    if not force:
        if os.path.isfile(destination_path):
            logger.info(f"file {destination_path} already exists, skipping")
            return

    logger.info(f"downloading image {index}...")

    try:
        box = bounding_box_from_point(lat, lon, dist)
        image = get_image(box, year, bands)
        image_to_disk(image, filename)

        # cleanup
        # copy the file to the destination folder
        shutil.copy(
            os.path.join(filename, "download.B8.tif"),
            destination_path)
        # delete the zip and folder that were created
        os.remove(f"{filename}.zip")
        shutil.rmtree(filename)
    except Exception as e:
        logger.info(f"could not download image @ {lat} {lon}")
        logger.debug(e)
