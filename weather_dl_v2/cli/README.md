# dl-cli
This is a command line interface for talking to the weather-dl-v2 FastAPI server.

You need to set up the [weather-dl-v2 server](https://github.com/google/weather-tools/tree/dl-v2/weather_dl_v2/fastapi-server).

After you have set up your server, replace the FastAPI server pod's IP in Dockerfile.
```
ENV BASE_URI=http://<pod-ip>:8080
```

## Installation 
Clone
```
git clone https://github.com/google/weather-tools
cd weather-tools/weather_dl_v2/cli
```
You can either use the cli locally or in a docker container.

Local
```
conda env create --name weather-dl-v2-cli --file=environment.yml
conda activate weather-dl-v2-cli
export BASE_URI=<fast-api server ip>
```
Docker
```
docker build -t dl-cli .
docker run -it --net <network name> dl-cli
conda activate weather-dl-v2-cli
```
After activating the conda enviroment, cli can be used. To see baisc subcommands use -
## Usage
```
weather-dl-cli --help
```