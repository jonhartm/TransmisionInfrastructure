import logging
import multiprocessing
import random

import pandas as pd
import tqdm

from get_images import download_image, bounding_box_from_point
from load_datasets import process_transmission_data

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # load the transmission line data
    translines_shp = process_transmission_data(["MI"])

    print(translines_shp.iloc[0].LAT, translines_shp.iloc[0].LON)
    print(bounding_box_from_point(translines_shp.iloc[0].LAT, translines_shp.iloc[0].LON))

    sample_df = translines_shp.sample(1000, random_state=42)

    # Download 1000 images from the 2020 map that contain infrastructure
    work = [
        (row.LAT, row.LON, 2020, f"{row.hash}_2020", "images", i)
        for i,row
        in sample_df.iterrows()]

    # log these images so we can look them up later
    image_list = [
        {
            "lat": w[0],
            "lon": w[1],
            "image_name": w[3],
            "target": 1
        }
        for w in work]

    with multiprocessing.Pool(8) as p:
        p.starmap(download_image, work)

    # Download 1000 images from the same sample, but offset each of the
    # images randomly N/S/E/W
    offset_dist = 0.01
    random.seed(42)

    work = [
        (
            row.LAT + random.choice([1,-1]) * offset_dist,
            row.LON + random.choice([1,-1]) * offset_dist,
            2020,
            f"{row.hash}_2020_neg",
            "images",
            i)
        for i,row
        in sample_df.iterrows()]

    with multiprocessing.Pool(8) as p:
        p.starmap(download_image, work)

    # log these images so we can look them up later
    image_list.extend([
        {
            "lat": w[0],
            "lon": w[1],
            "image_name": w[3],
            "target": 0
        }
        for w in work])

    pd.DataFrame(image_list).to_csv("downloaded_images.csv", index=None)
