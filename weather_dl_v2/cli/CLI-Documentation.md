# CLI Documentation
The following doc provides cli commands and their various arguments and options.

Base Command:
```
weather-dl-v2
```

## Ping
Ping the FastAPI server and check if it’s live and reachable.

<details>
 <summary><code>weather-dl-v2 ping</code></summary>

##### Usage
```
weather-dl-v2 ping
```

</details>

<br>

## Download
Manage download configs.

<details>
 <summary><code>weather-dl-v2 download add</code> <br>
 Adds a new download config to specific licenses.  
 </summary>


##### Arguments
> `FILE_PATH` : Path to config file.

##### Options
> `-l/--license` (Required): License ID to which this download has to be added to.
> `-f/--force-download` : Force redownload of partitions that were previously downloaded.

##### Usage
```
weather-dl-v2 download add /path/to/example.cfg –l L1 -l L2 [--force-download]
```
</details>


<details>
 <summary><code>weather-dl-v2 download list</code> <br>
 List all the active downloads.
 </summary>

The list can also be filtered out by client_names.

##### Options
> `--filter` : Filter the list by some key and value. Format of filter filter_key=filter_value

##### Usage
```
weather-dl-v2 download list
weather-dl-v2 download list --filter client_name=cds
```
</details>


<details>
 <summary><code>weather-dl-v2 download get</code> <br>
 Get a particular download by config name.
 </summary>

##### Arguments
> `CONFIG_NAME` : Name of the download config.

##### Usage
```
weather-dl-v2 download get example.cfg
```
</details>

<details>
 <summary><code>weather-dl-v2 download remove</code> <br>
 Remove a download by config name.
 </summary>

##### Arguments
> `CONFIG_NAME` : Name of the download config.

##### Usage
```
weather-dl-v2 download remove example.cfg
```
</details>

<details>
 <summary><code>weather-dl-v2 download refetch</code> <br>
 Refetch all non-successful partitions of a config.
 </summary>

##### Arguments
> `CONFIG_NAME` : Name of the download config.

##### Options
> `-l/--license` (Required): License ID to which this download has to be added to.

##### Usage
```
weather-dl-v2 download refetch example.cfg -l L1 -l L2
```
</details>

<br>

## License
Manage licenses.

<details>
 <summary><code>weather-dl-v2 license add</code> <br>
 Add a new license. New licenses are added using a json file.
 </summary>

The json file should be in this format:
```
{
	"client_name": <client_name>,
	"number_of_requests": <number_of_request>,
	"secret_id": <secret_manager_id>
}
```


##### Arguments
> `FILE_PATH` : Path to the license json.

##### Usage
```
weather-dl-v2 license add /path/to/new-license.json
```
</details>

<details>
 <summary><code>weather-dl-v2 license get</code> <br>
 Get a particular license by license ID.
 </summary>

##### Arguments
> `LICENSE` : License ID of the license to be fetched.

##### Usage
```
weather-dl-v2 license get L1
```
</details>

<details>
 <summary><code>weather-dl-v2 license remove</code> <br>
 Remove a particular license by license ID.
 </summary>

##### Arguments
> `LICENSE` : License ID of the license to be removed.

##### Usage
```
weather-dl-v2 license remove L1
```
</details>

<details>
 <summary><code>weather-dl-v2 license list</code> <br>
 List all the licenses available. 
 </summary>

 The list can also be filtered by client name.

##### Options
> `--filter` : Filter the list by some key and value. Format of filter filter_key=filter_value.

##### Usage
```
weather-dl-v2 license list
weather-dl-v2 license list --filter client_name=cds
```
</details>

<details>
 <summary><code>weather-dl-v2 license update</code> <br>
 Update an existing license using License ID and a license json.
 </summary>

 The json should be of the same format used to add a new license.

##### Arguments
> `LICENSE` : License ID of the license to be edited.  
> `FILE_PATH` : Path to the license json.

##### Usage
```
weather-dl-v2 license edit L1 /path/to/license.json
```
</details>

<br>

## Queue
Manage all the license queue.

<details>
 <summary><code>weather-dl-v2 queue list</code> <br>
 List all the queues.
 </summary>

 The list can also be filtered by client name.

##### Options
> `--filter` : Filter the list by some key and value. Format of filter filter_key=filter_value.

##### Usage
```
weather-dl-v2 queue list
weather-dl-v2 queue list --filter client_name=cds
```
</details>

<details>
 <summary><code>weather-dl-v2 queue get</code> <br>
 Get a queue by license ID.
 </summary>

 The list can also be filtered by client name.

##### Arguments
> `LICENSE` : License ID of the queue to be fetched.

##### Usage
```
weather-dl-v2 queue get L1
```
</details>

<details>
 <summary><code>weather-dl-v2 queue edit</code> <br>
 Edit the priority of configs inside queues using edit.
 </summary>

Priority can be edited in two ways:
1. The new priority queue is passed using a priority json file that should follow the following format:
```
{
	“priority”: [“c1.cfg”, “c3.cfg”, “c2.cfg”]
}
```
2. A config file name and its absolute priority can be passed and it updates the priority for that particular config file in the mentioned license queue.

##### Arguments
> `LICENSE` : License ID of queue to be edited.

##### Options
> `-f/--file` : Path of the new priority json file.  
> `-c/--config` : Config name for absolute priority.  
> `-p/--priority`: Absolute priority for the config in a license queue. Priority increases in ascending order with 0 having highest priority.

##### Usage
```
weather-dl-v2 queue edit L1 --file /path/to/priority.json
weather-dl-v2 queue edit L1 --config example.cfg --priority 0
```
</details>

<br>