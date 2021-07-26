import hashlib
import logging

import geopandas as gpd

logger = logging.getLogger(__name__)

TRANSMISSION_LINES_DATA = "./data/HIFLD/Transmission_Lines.shp"
US_STATES_DATA = "./data/US_States/tl_2020_us_state.shp"

def get_linestring_coords(x):
  """
  Get a list of coordinates from a geographic point. Note that the lat/lon
  is flipped in the output - makes it easier to pull up the coordinates on
  google maps. Intended to be used by pandas.apply.
  """
  try:
      lons,lats = x.coords.xy
      coords = [(lat,lon) for lon,lat in zip(lons, lats)]
  except NotImplementedError:
        # some geometries are made up of several disconnected line strings. We
        # get a NotImplementedError Exception when we try to iterate over these,
        # so we'll just catch that here and handle it in a loop.
        coords = []
        for part in x:
            lons,lats = part.coords.xy
            coords.extend([(lat,lon) for lon,lat in zip(lons, lats)])
  return coords

def get_transmission_lines_data(states=None):
    """
    Load the transmission lines data and return it as a geopandas dataframe.

    Args:
        states (str/list[str]):
            A single or list of state names or state abbreviations to filter
            the dataset with. Useful for debugging.

    Returns:
        gpd.DataFrame:
            A Dataframe with the transmission line information
    """
    logger.info(f"loading {TRANSMISSION_LINES_DATA}...")
    translines_shp = gpd.read_file(TRANSMISSION_LINES_DATA)

    # for ease of working with the data visually, let's change the coordinates from
    # EPSG:3857 (Meters offset from 0N 0E) to  EPSG:4326 (geodetic lat/lon)
    translines_shp = translines_shp.to_crs("epsg:4326")

    logger.info(f"Loading state data from {US_STATES_DATA}")
    # Merge in the state data
    us_states_shp = gpd.read_file(US_STATES_DATA)
    # ensure the crs of the geometries match the transmission dataset
    us_states_shp = us_states_shp.to_crs(translines_shp.crs)

    # If we asked to filter by states, do that here
    if states:
        # Check if states is a list, and if not, enclose it in a list
        if type(states) is not list:
            states = [states]

        logger.info(f"trimming dataset to states {', '.join(states)}")
        # redefine translines_shp with this filter applied
        us_states_shp = us_states_shp[
                (us_states_shp.NAME.isin(states))
                | (us_states_shp.STUSPS.isin(states))]

    # do the merge
    translines_shp = gpd.overlay(
        us_states_shp,
        translines_shp,
        how="intersection",
        keep_geom_type=False)

    logger.debug(translines_shp.crs)
    logger.debug(translines_shp.info())
    logger.debug(translines_shp.sample(5))

    return translines_shp

def process_transmission_data(states=None):
    """
    Process the transmission data.

    Args:
        states (str/list[str]):
            A single or list of state names or state abbreviations to filter
            the dataset with. Useful for debugging.

    Returns:
        gpd.DataFrame:
            A Dataframe of transmission infrastructure data. Contains the
            following columns:
                STUSPS - The state abbreviation
                NAME - The name of the state
                ID - the ID of this infrastructure element from the original data
                LAT - the latitude for the above
                LON - the longitude for the above
                hash - the MD5 Hash of (ID+LAT+LON). Used for identifying files.
    """
    # load the transmission line data
    translines_shp = get_transmission_lines_data(states)

    # Trim the data down to just the index and the geometry
    translines_shp = translines_shp[["STUSPS", "NAME", "ID", "geometry"]]

    # set the index to the state,
    translines_shp.set_index(["STUSPS", "NAME", "ID"], inplace=True)

    # Extract the coordinates from each record
    translines_shp["coords"] =\
        translines_shp.geometry.apply(get_linestring_coords)

    # Explode the dataset on the coords
    translines_shp = translines_shp.explode("coords")

    # Grab the lat/lon from each coordinate pair and turn them into
    # individual columns
    translines_shp["LAT"] = translines_shp.coords.apply(lambda x: x[0])
    translines_shp["LON"] = translines_shp.coords.apply(lambda x: x[1])

    # trim the dataset down to just the index and the lat/lon
    translines_shp = translines_shp[["LAT", "LON"]]

    # reset the index so I don't have to mess with a multiindex
    translines_shp.reset_index(inplace=True)

    # create a column based off of the hash of ID, LAT and LON
    # we can use the hash as part of the filename, since we know how it's built
    translines_shp["hash"] = translines_shp.apply(
        lambda x: hashlib.md5(
            (str(x.ID) + str(x.LAT) + str(x.LON)).encode()).hexdigest(), axis=1)

    return translines_shp
