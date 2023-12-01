#! /bin/bash

command="docker exec -it \\\$(docker ps -qf name=weather-dl-v2-cli) /bin/bash"
sudo sh -c "echo \"$command\" >> /etc/profile"