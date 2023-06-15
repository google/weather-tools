import typer
from typing_extensions import Annotated
from app.services.queue_service import queue_service
from app.utils import Validator

app = typer.Typer()

class QueueValidator(Validator):
    pass

@app.command("list", help="List all the license queues.")
def get_all_license_queue(
        filter: Annotated[str, typer.Option(help="Filter by some value. Format: filter_key=filter_value")] = None
    ):
    if filter:
        
        validator = QueueValidator(valid_keys=["client_name"])

        try:
            data = validator.validate(filters=[filter])
            client_name = data['client_name']
        except Exception as e:
            print(f"filter error: {e}")
            return 
        
        print(queue_service._get_license_queue_by_client_name(client_name))
        return
    
    print(queue_service._get_all_license_queues())

@app.command("get", help="Get queue of particular license.")
def get_license_queue(
        license: Annotated[str, typer.Argument(help="License ID")],
    ):
    print(queue_service._get_queue_by_license(license))

@app.command("edit", help="Edit existing license queue.")
def modify_license_queue(
        license: Annotated[str, typer.Argument(help="License ID.")],
        file_path: Annotated[str, typer.Argument(help='''File path of priority json file. Example json: {"priority": ["c1.cfg", "c2.cfg",...]}''')]
    ):
    validator = QueueValidator(valid_keys=["priority"])

    try:
        data = validator.validate_json(file_path=file_path)
        priority_list = data["priority"]
    except Exception as e:
        print(f"key error: {e}")
        return
    
    print(queue_service._edit_license_queue(license, priority_list))

