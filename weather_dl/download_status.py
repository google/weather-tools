# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import itertools
import sys
import typing as t
from collections import Counter
from urllib.parse import urlparse

try:
    from .download_pipeline.manifest import Location, MANIFESTS, Manifest
except ModuleNotFoundError:
    print('\033[93mTool requires `ecmwf-pipelines` to be installed!`\033[0m')
    sys.exit(1)

# Cycle of characters used in display progress
PROGRESS = itertools.cycle(''.join([c * 100 for c in '|/–-\\']))


def main(args: t.List[str], *, manifests: t.Mapping[str, Manifest.__class__] = MANIFESTS) -> Counter:
    """Counts frequency of statuses within a Firestore path."""
    parser = argparse.ArgumentParser(description='Check statuses of `weather-dl` downloads.')
    parser.add_argument('prefix',
                        help='Prefix of the location string (e.g. a cloud bucket); used to filter which statuses to '
                             'check.')
    parser.add_argument('-m', '--manifest-location', type=Location, default='fs://downloader-manifest',
                        help="Location of the manifest, only FirestoreManifests are supported. These are formatted as "
                             "such: 'fs://<my-collection>?projectId=<my-project-id>'")
    args = parser.parse_args(args)

    if urlparse(args.manifest_location).scheme != 'fs':
        raise NotImplementedError('Only Firestore Manifests are supported!')

    counter = Counter()

    location = Location(args.manifest_location)

    manifest_type = manifests[urlparse(location).scheme]
    fs_manifest = manifest_type(location)

    store_scheme = urlparse(args.prefix).scheme

    for collection in fs_manifest.root_document_for_store(store_scheme).collections():
        for doc in collection.stream():
            print(next(PROGRESS), end='\r')
            data = doc.to_dict()
            if data.get('location', '').startswith(args.prefix):
                counter.update([data['status']])

    print(f"\033[1m\033[92mThe current download statuses for '{args.prefix}' are: {counter}.\033[0m")
    return counter
