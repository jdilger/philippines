# exploring data.py
import glob
import re
import json
import subprocess
from datetime import datetime
from pprint import pprint


BASE_ASSET_PATH = r"projects/earthengine-legacy/assets/" r"projects/sig-ee/Philippines"
BUCKET = "gee-upload"
unique_ics = []


def remove_year_band_name(bandnamepath: str) -> str:

    tmp = bandnamepath.split(".")[0]
    tmp = tmp.split("/")[-1].split("_")
    del tmp[1]

    return "_".join(tmp)


def make_asset_name(j: str, base_asset_path: str, search_key: str = "maps") -> str:

    regex = search_key + r"[\\/](\w{2,})[\\/](\w{2,})[\\/](\d{4})"
    groups = re.search(regex, j)
    asset_name = f"{base_asset_path}/{groups[1]}_{groups[2]}/{groups[3]}"
    unique_ics.append(f"{groups[1]}_{groups[2]}")
    return asset_name


def make_source_ids(localpath: str, bucket: str, split_folder: str = "maps") -> str:

    gcp_id = localpath.replace("\\", r"/").split(split_folder)[1]

    return f"gs://{bucket}/{split_folder}{gcp_id}"


def get_band_dict(bands: list, bucket: str) -> dict:

    bands_gcp_formatted = [make_source_ids(i, bucket) for i in bands]
    band_names = list(map(lambda i: remove_year_band_name(i), bands_gcp_formatted))
    band_dict = dict(zip(band_names, bands_gcp_formatted))

    return band_dict


def get_metadata_dict(band_path: str, **kwags) -> dict:

    parse_path = re.search(r"[\\/](\d{4})[\\/]", band_path)[1]
    start_time = f"{datetime.strptime(parse_path,'%Y').isoformat()}Z"

    metadata_dict = dict(start_time=start_time)
    if kwags:
        metadata_dict["properties"] = kwags
    return metadata_dict


def add_manifest_element(asset: dict, func, **kwags) -> None:

    func(asset, kwags)


def make_tilesets(asset: dict, band_dict: dict) -> None:

    band_dict = band_dict.get("band_dict")

    for k, v in band_dict.items():
        asset["tilesets"].append({"id": k, "sources": [{"uris": [v]}]})


def make_bands(asset: dict, band_dict: dict) -> None:

    band_dict = band_dict.get("band_dict")
    for k, v in band_dict.items():
        asset["bands"].append({"id": k, "tileset_id": k})


def make_metadata(asset: dict, metadata: dict) -> None:

    metadata = metadata.get("metadata")
    start_time = metadata.get("start_time", None)
    properties = metadata.get("properties", None)

    if start_time:
        asset["start_time"] = start_time
    if properties:
        asset["properties"] = properties


def make_manifest(asset_name: str, band_dict: dict, metadata: dict) -> dict:
    # todo, grab start time from j path -its the last entry
    asset = {
        "name": asset_name,
        "tilesets": [],
        "bands": [],
        "start_time": "",
    }
    add_manifest_element(asset, make_tilesets, band_dict=band_dict)
    add_manifest_element(asset, make_bands, band_dict=band_dict)
    add_manifest_element(asset, make_metadata, metadata=metadata)
    pprint(asset)

    return asset


def check_json_name(file_name: str) -> str:
    search = re.search(".json$", file_name)
    if search is None:
        return f"{file_name}.json"
    else:
        return file_name


def upload(manifest: str = None):

    result = subprocess.run(
        ["earthengine", "upload", "image", "--manifest", manifest], capture_output=True
    )
    task_id = re.search(
        r"(Started upload task with ID:) ([\w{1:24}\d{1:24}]*)", str(result)
    )

    return f"{task_id.groups()[1]}"


def save(
    manifest: dict, location: str, file_name: str = None, test: bool = False
) -> None:
    """saves your manifest to a local location."""
    assert manifest is not None, "Manifest has not been created."
    import os

    print(os.getcwd())
    if file_name is None:
        file_name = "manifest.json"
    else:
        file_name = check_json_name(file_name)

    full_path = f"{location}/{file_name}"

    with open(full_path, "w") as f:
        json.dump(manifest, f, indent=2)

    if test is False:
        upload(full_path)


base_ics = [i for i in glob.glob("data/20201001/maps/*")]
print("base_ics", base_ics)
# note, needs to be ran from C:\Users\johnj\Documents\SIG\43.phi atm
# fix this later
count = 0
for i in base_ics:
    for j in glob.glob(f"{i}/*/*"):

        band_paths = glob.glob(f"{j}/*.tif")
        asset_name = make_asset_name(j, BASE_ASSET_PATH)
        band_dict = get_band_dict(band_paths, BUCKET)
        metadata = get_metadata_dict(band_paths[0])
        manifest = make_manifest(asset_name, band_dict, metadata)
        save(manifest, "./scripts/manifests", f"manifest_{count}", test=True)
        count += 1


print("total imgs:", count)
print(set(unique_ics))
