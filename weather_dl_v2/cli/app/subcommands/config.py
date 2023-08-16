import typer
import json
from typing_extensions import Annotated
from app.cli_config import get_config
from app.utils import Validator

app = typer.Typer()

class ConfigValidator(Validator):
    pass

@app.command("show-ip", help="See the current server IP address.")
def show_server_ip():
    print(f"Current pod IP: {get_config().pod_ip}")

@app.command("set-ip", help="Update the server IP address.")
def update_server_ip(
    NEW_IP: Annotated[
        str, typer.Argument(help="New IP address. (Do not add port or protocol).")
    ],
):  
    file_path = "./cli_config.json"
    cli_config = {}
    with open(file_path, 'r') as file:
        cli_config = json.load(file)    

    old_ip = cli_config['pod_ip']
    cli_config['pod_ip'] = new_ip

    with open(file_path, 'w') as file:
        json.dump(cli_config, file)

    validator = ConfigValidator(valid_keys=["pod_ip", "port"])

    try:
        cli_config = validator.validate_json(file_path=file_path)
    except Exception as e:
        print(f"payload error: {e}")
        return
    
    print(f"Pod IP Updated {old_ip} -> {new_ip}")
    

