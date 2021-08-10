import logging
import multiprocessing

import tqdm

from get_images import download_image, bounding_box_from_point
from load_datasets import process_transmission_data

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # load the transmission line data
    translines_shp = process_transmission_data(["MI"])

    print(translines_shp.iloc[0].LAT, translines_shp.iloc[0].LON)
    print(bounding_box_from_point(translines_shp.iloc[0].LAT, translines_shp.iloc[0].LON))

    work = [
        (row.LAT, row.LON, 2020, f"{row.hash}_2020", "images", i)
        for i,row
        in translines_shp.sample(1000, random_state=42).iterrows()]

    with multiprocessing.Pool(8) as p:
        p.starmap(download_image, work)
