FROM continuumio/miniconda3:latest

COPY . .

# Add the mamba solver for faster builds
RUN conda install -n base conda-libmamba-solver
RUN conda config --set solver libmamba

# Create conda env using environment.yml
RUN conda update conda -y
RUN conda env create --name weather-dl-v2-cli --file=environment.yml

# Activate the conda env and update the PATH
ARG CONDA_ENV_NAME=weather-dl-v2-cli
RUN echo "source activate ${CONDA_ENV_NAME}" >> ~/.bashrc
ENV PATH /opt/conda/envs/${CONDA_ENV_NAME}/bin:$PATH

RUN apt-get update -y
RUN apt-get install nano -y
RUN apt-get install vim -y
RUN apt-get install curl -y

# Install gsutil 
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-443.0.0-linux-arm.tar.gz
RUN tar -xf google-cloud-cli-443.0.0-linux-arm.tar.gz
RUN ./google-cloud-sdk/install.sh --quiet
RUN echo "if [ -f '/google-cloud-sdk/path.bash.inc' ]; then . '/google-cloud-sdk/path.bash.inc'; fi" >> /root/.bashrc
RUN echo "if [ -f '/google-cloud-sdk/completion.bash.inc' ]; then . '/google-cloud-sdk/completion.bash.inc'; fi" >> /root/.bashrc
