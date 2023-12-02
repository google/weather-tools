# Deployment Instructions & General Notes

### How to create environment
```
conda env create --name weather-dl-v2-license-dep --file=environment.yml

conda activate weather-dl-v2-license-dep
```

### Make changes in weather_dl_v2/config.json, if required [for running locally]
```
export CONFIG_PATH=/path/to/weather_dl_v2/config.json
```

### Create docker image for license deployment
Refer instructions in weather_dl_v2/README.md