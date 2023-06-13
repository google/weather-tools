FROM continuumio/miniconda3:latest

COPY . .

RUN conda update conda -y
RUN conda env create -f environment.yml

ENV BASE_URI=http://<pod-ip>:8080