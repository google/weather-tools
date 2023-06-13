FROM continuumio/miniconda3:latest

COPY . .

# Create conda env using environment.yml
RUN conda update conda -y
RUN conda env create --name weather-dl-v2-cli --file=environment.yml

# Activate the conda env and update the PATH
ARG CONDA_ENV_NAME=weather-dl-v2-cli
RUN echo "source activate ${CONDA_ENV_NAME}" >> ~/.bashrc
ENV PATH /opt/conda/envs/${CONDA_ENV_NAME}/bin:$PATH

ENV BASE_URI=http://<pod-ip>:8080
