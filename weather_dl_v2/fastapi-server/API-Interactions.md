# API Interactions
| Command | Type | Endpoint |
|---|---|---|
| `weather-dl-v2 ping` | `get` | `/`
| Download  |   |   |
| `weather-dl-v2 download add <path> –l <license_id> [--force-download]` | `post` | `/download?force_download={value}` |
| `weather-dl-v2 download list` | `get` | `/download/` |
| `weather-dl-v2 download list --filter client_name=<name>` | `get` | `/download?client_name={name}` |
| `weather-dl-v2 download get <config_name>` | `get` | `/download/{config_name}` |
| `weather-dl-v2 download remove <config_name>` | `delete` | `/download/{config_name}` |
| `weather-dl-v2 download refetch <config_name> -l <license_id>` | `post` | `/download/refetch/{config_name}` |
|  License |   |   |
| `weather-dl-v2 license add <path>` | `post` | `/license/` |
| `weather-dl-v2 license get <license_id>` | `get` | `/license/{license_id}` |
| `weather-dl-v2 license remove <license_id>` | `delete` | `/license/{license_id}` |
| `weather-dl-v2 license list` | `get` | `/license/` |
| `weather-dl-v2 license list --filter client_name=<name>` | `get` | `/license?client_name={name}` |
| `weather-dl-v2 license edit <license_id> <path>` | `put` | `/license/{license_id}` |
| Queue  |   |   |
| `weather-dl-v2 queue list` | `get` | `/queues/` |
| `weather-dl-v2 queue list --filter client_name=<name>` | `get` | `/queues?client_name={name}` |
| `weather-dl-v2 queue get <license_id>` | `get` | `/queues/{license_id}` |
| `queue edit <license_id> --config <config_name> --priority <number>` | `post` | `/queues/{license_id}` |
| `queue edit <license_id> --file <path>` | `put` | `/queues/priority/{license_id}` |