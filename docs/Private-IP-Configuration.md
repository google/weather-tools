# Private IP Configuration Guide for Dataflow Pipeline Execution

## Goals
In this document, we’ll describe how to use Private IP for the execution of a dataflow pipeline.

## Background
When we are running the dataflow pipeline, GCP decides to spawn one or more new VM-instances. By default, each VM-instance will have an External IP address. 

![VM Instance with External IP Address](_static/vm_instance_with_external_ip_address.png?raw=true "VM Instance with External IP Address")

Considering a billing account has a limited number of External IP addresses, we can skip this overhead by providing VPC-parameters as CLI-input of dataflow.

Following table<font color="red">*</font> summarizes the required input parameters.

| Field             | Description                                                                                                                                                              |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| network           | The Compute Engine network for launching Compute Engine instances to run your pipeline. If not set, Google Cloud assumes that you intend to use a network named default. |
| subnetwork        | The Compute Engine subnetwork for launching Compute Engine instances to run your pipeline.                                                                               |
| no_use_public_ips | Command-line flag that sets use_public_ips to False. If the option is not explicitly enabled or disabled, the Dataflow workers use public IP addresses.                  |

(<font color="red">*</font> excerpt from [GCP pipeline-options](https://cloud.google.com/dataflow/docs/reference/pipeline-options))

## Steps to Configure VPC
1. Configure VPC-Network & Subnetwork
2. Configure Firewall-Rule
3. Configure NAT & Router
4. Sample commands to trigger dataflow pipeline execution using above options

## Configure VPC-Network & Subnetwork
1. Open GCP’s Create VPC-Network page.
2. Provide name, description.
3. Select “Subnet creation mode” as Custom.
4. Provide name, description, region, IP address range & other
5. Select “Private Google Access” as On.
6. Complete VPC-Network creation by providing other required parameters. Refer to GCP’s [Create-VPC-Network](https://cloud.google.com/vpc/docs/create-modify-vpc-networks) documentation for more details.

![VPC Network Details](_static/vpc_network_details.png?raw=true "VPC Network Details")

## Configure Firewall-Rule
1. Open GCP’s Create a firewall rule page.
2. Provide name, description. You may set “Logs” as Off.
3. For the “Network” drop-down, select the network that we created in the previous step.
4. Select “Direction of traffic” as Ingress & “Action on match” as Allow.
5. Complete Firewall-Rule creation by providing other necessary information. Refer to GCP’s [Configuring-Firewall](https://cloud.google.com/filestore/docs/configuring-firewall) documentation for more details.

![Firewall Rule Details](_static/firewall_rule_details.png?raw=true "Firewall Rule Details")

## Configure NAT & Router
1. Open GCP’s Create a NAT gateway page.
2. Provide name, region.
3. For the “Network” drop-down, select the network that we created earlier.
4. For the “Router” drop-down, EITHER select pre-created router OR click on “create new router”.
<br> a. Complete router creation by providing name, description & region. Refer to GCP’s [Create-Router](https://cloud.google.com/network-connectivity/docs/router/how-to/create-router-vpc-on-premises-network) documentation for more details.
5. Complete NAT gateway creation by providing required details. Refer to GCP’s [Create-NAT-Gateway](https://cloud.google.com/nat/docs/set-up-manage-network-address-translation) documentation for more details.

![Router Details](_static/router_details.png?raw=true "Router Details")

![NAT Gateways](_static/nat_gateways.png?raw=true "NAT Gateways")

## Sample commands to trigger dataflow pipeline execution using above options

Following section showcases how VPC-parameters can be given as CLI inputs to weather-mv dataflow pipeline.

```bash
weather-mv --uris "gs://$STORAGE_BUCKET/*.nc"
           --output_table "$HOST_PROJECT_ID.$DATASET_ID.$TABLE_ID"
           --temp_location "gs://$STORAGE_BUCKET/tmp"
           --runner DataflowRunner
           --project $HOST_PROJECT_ID
           --region $REGION_NAME
           --no_use_public_ips 
           --network=$NETWORK_NAME
           --subnetwork=regions/$REGION_NAME/subnetworks/$SUBNETWORK_NAME
```

Replace the following:

- STORAGE_BUCKET: the storage bucket, e.g. bucket_58231
- HOST_PROJECT_ID: the host project ID, e.g. weather_tools
- DATASET_ID: the name of dataset, e.g. weather_mv_ds
- TABLE_ID: the name of table, e.g. tbl_2017_01 
- REGION_NAME: the regional endpoint of your Dataflow job, e.g. us-central1
- NETWORK_NAME: the name of your Compute Engine network, e.g. dataflow
  - Provide network_name same as what we created in Step-1.
- SUBNETWORK_NAME: the name of your Compute Engine subnetwork, e.g. private
  - Provide a subnetwork_name same as what we created in Step-1.

Alternatively, you may also execute following command,

```bash
weather-mv --uris "gs://$STORAGE_BUCKET/*.nc"
           --output_table "$HOST_PROJECT_ID.$DATASET_ID.$TABLE_ID"
           --temp_location "gs://$STORAGE_BUCKET/tmp"
           --runner DataflowRunner
           --project $HOST_PROJECT_ID
           --region $REGION_NAME
           --no_use_public_ips
           –-subnetwork=https://www.googleapis.com/compute/v1/projects/$HOST_PROJECT_ID/regions/$REGION_NAME/subnetworks/$SUBNETWORK_NAME
```
