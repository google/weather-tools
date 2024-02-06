#!/usr/bin/env python3
# Copyright 2024 Google LLC
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

SUPPORTED_CUSTOM_COORDS = ['city', 'country']

COUNTRIES_BOUNDING_BOXES = {
    'india': (6.5546079, 35.4940095078, 68.1766451354, 97.4025614766),
    'canada': (41.6751050889, 83.23324, -140.99778, -52.6480987209),
    'japan': (31.0295791692, 45.5514834662, 129.408463169, 145.543137242),
    'united kingdom': (49.959999905, 58.6350001085, -7.57216793459, 1.68153079591),
    'south africa': (-34.8191663551, -22.0913127581, 16.3449768409, 32.830120477),
    'australia': (-44, -10, 113, 154),
    'united states': (24.396308, 49.384358, -125.0, -66.93457)
}

CITIES_BOUNDING_BOXES = {
    'delhi': (28.404, 28.883, 76.838, 77.348),
    'new york': (40.4774, 40.9176, -74.2591, -73.7002),
    'san francisco': (37.6398, 37.9298, -122.5975, -122.3210),
    'los angeles': (33.7036, 34.3373, -118.6682, -118.1553),
    'london': (51.3849, 51.6724, -0.3515, 0.1482)
}
