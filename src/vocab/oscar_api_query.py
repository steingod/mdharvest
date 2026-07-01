#!/usr/bin/env python3
#
# This fetches OSCAR satellites and instrument info and caches into OSCARvoc.py
# As remote sensing data is often made of many files with the same satellites/instruments
# caching avoids to repeat the same queries to oscar api when extracing nc attributes
# Runs as: ./oscar_api_query.py
#
import json
import datetime
import urllib.request as ul


def query_oscar_api(base_url, data_key):
    #query OSCAR api to get satellites or instruments info
    # looking for acronym -> short_name, fullname -> long_name and slug to create the resource
    data_dict = {}
    page = 1

    while True:
        try:
            url = f"{base_url}?page={page}"
            print("query: ", url)

            with ul.urlopen(url) as response:
                data = json.load(response)

                items = data.get("_embedded", {}).get(data_key, [])
                if not items:
                    break

                for item in items:
                    acronym = item.get("acronym")
                    slug = item.get("slug")
                    full_name = item.get("fullname")

                    if acronym:
                        data_dict[acronym] = {"slug": slug, "fullname": full_name}

            # loop pages
            page += 1

        except Exception as e:
            print(f"An error occurred while fetching data from {base_url}: {e}")
            break

    return data_dict

def fetch_satellite_data():
    #get satellites
    satellite_url = "https://space.oscar.wmo.int/api/v1/satellites"
    return query_oscar_api(satellite_url, "satellites")

def fetch_instrument_data():
    #get instruments
    instrument_url = "https://space.oscar.wmo.int/api/v1/instruments"
    return query_oscar_api(instrument_url, "instruments")

def save_to_file(output_file, satellite_data, instrument_data):
    #create dictionary file
    update = datetime.datetime.now()
    with open(output_file, "w") as file:
        file.write(f"# This file is generated through oscar_api_query.py\n")
        file.write('#last fetch: '+str(update)+"\n")
        file.write(f"satellites = {json.dumps(satellite_data)}\n")
        file.write(f"instruments = {json.dumps(instrument_data)}\n")


if __name__ == "__main__":
    satellite_data = fetch_satellite_data()
    instrument_data = fetch_instrument_data()

    if satellite_data and instrument_data:
        save_to_file("OSCARvoc.py", satellite_data, instrument_data)
    else:
        print('OSCARvoc.py could not be updated. Use cached version')

