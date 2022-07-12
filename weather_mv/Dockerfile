
# Copyright 2022 Google LLC
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
# ==============================================================================
ARG py_version=3.8
FROM apache/beam_python${py_version}_sdk:2.40.0 as beam_sdk
FROM continuumio/miniconda3:4.12.0
ARG py_version

# Update miniconda
RUN conda update conda -y

# Install desired python version
RUN conda install python=${py_version} -y

# Install SDK.
RUN pip install --no-cache-dir apache-beam[gcp]==2.40.0

# Verify that the image does not have conflicting dependencies.
RUN pip check

# Copy files from official SDK image, including script/dependencies.
COPY --from=beam_sdk /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]