
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
FROM ecmwf/jupyter-notebook as build
FROM apache/beam_python3.8_sdk:2.32.0
USER root
# Ensure ECMWF's HTTP proxy does not get in the way (thanks to bentorey.hernandez@ecmwf.int).
RUN set -ex \
	&& echo 'Acquire::http::Pipeline-Depth "0";' > /etc/apt/apt.conf.d/99fixbadproxy \
	&& echo 'Acquire::http::No-Cache=True;' >> /etc/apt/apt.conf.d/99fixbadproxy \
	&& echo 'Acquire::BrokenProxy=true;' >> /etc/apt/apt.conf.d/99fixbadproxy
# Install run-time dependencies.
RUN set -ex \
	&& apt-get update \
	&& apt-get install --yes --no-install-suggests --no-install-recommends \
	libgfortran4 \
	ghostscript \
	imagemagick \
	ksh \
	libarmadillo-dev \
	libbz2-1.0 \
	libcairo-gobject2 \
	libcairo-script-interpreter2 \
	libcairo2 \
	libcroco3 \
	libcurl4 \
	libexif12 \
	libexpat1 \
	libfftw3-double3 \
	libfftw3-long3 \
	libfftw3-quad3 \
	libfftw3-single3 \
	libfontconfig1 \
	libfreetype6 \
	libfribidi0 \
	libgdal20 \
	libgeoip1 \
	libgeos-c1v5 \
	libgif7 \
	libgomp1 \
	libgssrpc4 \
	libharfbuzz0b \
	libhdf5-103 \
	libicu-dev \
	libilmbase-dev \
	libjbig0 \
	libjpeg62-turbo-dev \
	libjs-jquery \
	liblcms2-2 \
	liblqr-1-0 \
	libnetcdf-c++4 \
	libnetcdf13 \
	libopenexr-dev \
	libpangocairo-1.0-0 \
	libpangoxft-1.0-0 \
	libpcrecpp0v5 \
	libpng16-16 \
	libproj-dev \
	libreadline7 \
	libsqlite3-0 \
	libtiff5 \
	libtiffxx5 \
	libwebp6 \
	libxft2 \
	libxml2 \
	libxslt1.1 \
	poppler-utils \
	rsync
COPY --from=build /usr/local/share/eccodes/ /usr/local/share/eccodes/
COPY --from=build /usr/local/share/libemos/ /usr/local/share/libemos/
COPY --from=build /usr/local/share/magics/ /usr/local/share/magics/
COPY --from=build /usr/local/share/metview/ /usr/local/share/metview/
COPY --from=build /usr/local/bin/ /usr/local/bin/
COPY --from=build /usr/local/lib/ /usr/local/lib/
# Ensure shared libs installed by the previous step are available.
RUN set -ex \
	&& /sbin/ldconfig
USER $NB_UID
# Configure Python runtime.
ENV \
	PYTHONDONTWRITEBYTECODE=1 \
	PYTHONPATH=/usr/local/lib/python3.8/site-packages \
	PYTHONUNBUFFERED=1
# Install Python modules.
RUN set -ex \
	&& pip install \
	cdsapi==0.1.1 \
	cfgrib==0.9.9.1 \
	ecmwf-api-client==1.4.2 \
	metview==0.8.6