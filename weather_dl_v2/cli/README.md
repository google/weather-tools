# dl-cli
This is a command line interface for talking to the weather-dl-v2 FastAPI server.

You need to set up the [weather-dl-v2 server](https://github.com/google/weather-tools/tree/dl-v2/weather_dl_v2/fastapi-server)

After you have set up your server, replace the FastAPI server pod's IP in Dockerfile
```
ENV BASE_URI=http://<pod-ip>:8080
```

## Installation 
Local
```
git clone https://github.com/aniketinfocusp/weather-dl-cli
cd weather-dl-cli

conda env create --name dl-cli-env --file=environment.yml
conda activate dl-cli-env
export BASE_URI=<fast-api server ip>
```
Docker
```
docker build -t dl-cli .
docker run -it --net <network name> dl-cli
conda activate dl-cli-env
```
After activating the conda enviroment, cli can be used. To see baisc subcommands use -
## Usage
```
dl-cli --help
```