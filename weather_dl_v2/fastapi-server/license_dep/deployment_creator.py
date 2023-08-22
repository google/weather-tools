import logging
from os import path
import yaml
from kubernetes import client, config
from server_config import get_config

logger = logging.getLogger(__name__)


def create_license_deployment(license_id: str) -> str:
    """Creates a kubernetes workflow of type Job for downloading the data."""
    config.load_config()

    with open(path.join(path.dirname(__file__), "license_deployment.yaml")) as f:
        deployment_manifest = yaml.safe_load(f)
        deployment_name = f"weather-dl-v2-license-dep-{license_id}".lower()

        # Update the deployment name with a unique identifier
        deployment_manifest["metadata"]["name"] = deployment_name
        deployment_manifest["spec"]["template"]["spec"]["containers"][0]["args"] = [
            "--license",
            license_id,
        ]
        deployment_manifest["spec"]["template"]["spec"]["containers"][0][
            "image"
        ] = get_config().license_deployment_image

        # Create an instance of the Kubernetes API client
        api_instance = client.AppsV1Api()
        # Create the deployment in the specified namespace
        response = api_instance.create_namespaced_deployment(
            body=deployment_manifest, namespace="default"
        )

        logger.info(f"Deployment created successfully: {response.metadata.name}")
        return deployment_name


def terminate_license_deployment(license_id: str) -> None:
    # Load Kubernetes configuration
    config.load_config()

    # Create an instance of the Kubernetes API client
    api_instance = client.AppsV1Api()

    # Specify the name and namespace of the deployment to delete
    deployment_name = f"weather-dl-v2-license-dep-{license_id}".lower()

    # Delete the deployment
    api_instance.delete_namespaced_deployment(name=deployment_name, namespace="default")

    logger.info(f"Deployment '{deployment_name}' deleted successfully.")
