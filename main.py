import logging

from load_datasets import process_transmission_data

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # load the transmission line data
    translines_shp = process_transmission_data(["MI", "ME", "FL", "UT", "AZ"])

    print(translines_shp.sample(10))
