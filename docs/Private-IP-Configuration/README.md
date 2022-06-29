# Private IP Configuration Guide for Dataflow Pipeline Execution

## Goals
In this document, we’ll describe how to use Private IP for the execution of a dataflow pipeline.

## Background
When we are running the dataflow pipeline, GCP decides to spawn one or more new VM-instances. By default, each VM-instance will have an External IP address. 

![VM Instance with External IP Address](https://user-images.githubusercontent.com/101806311/176452459-1f996a55-2604-4abc-a601-689442ba4d93.png?raw=true "VM Instance with External IP Address")

Considering a billing account has a limited number of External IP addresses, we can skip this <u>overhead</u> by providing VPC-parameters as CLI-input of dataflow.

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

![VPC Network Details](https://user-images.githubusercontent.com/101806311/176452593-a8a45ba0-f6d3-46b6-b335-f3f0c34b6c31.png?raw=true "VPC Network Details")

## Configure Firewall-Rule
1. Open GCP’s Create a firewall rule page.
2. Provide name, description. You may set “Logs” as Off.
3. For the “Network” drop-down, select the network that we created in the previous step.
4. Select “Direction of traffic” as Ingress & “Action on match” as Allow.
5. Complete Firewall-Rule creation by providing other necessary information. Refer to GCP’s [Configuring-Firewall](https://cloud.google.com/filestore/docs/configuring-firewall) documentation for more details.

![Firewall Rule Details](https://user-images.githubusercontent.com/101806311/176452752-1870458f-426c-4ae3-b249-2f987e79922a.png?raw=true "Firewall Rule Details")

## Configure NAT & Router
1. Open GCP’s Create a NAT gateway page.
2. Provide name, region.
3. For the “Network” drop-down, select the network that we created earlier.
4. For the “Router” drop-down, EITHER select pre-created router OR click on “create new router”.
<br> a. Complete router creation by providing name, description & region. Refer to GCP’s [Create-Router](https://cloud.google.com/network-connectivity/docs/router/how-to/create-router-vpc-on-premises-network) documentation for more details.
5. Complete NAT gateway creation by providing required details. Refer to GCP’s [Create-NAT-Gateway](https://cloud.google.com/nat/docs/set-up-manage-network-address-translation) documentation for more details.

![Router Details](https://user-images.githubusercontent.com/101806311/176452881-a8070ccc-0349-4459-a7ec-f33bff6b592e.png?raw=true "Router Details")

![NAT Gateways](https://user-images.githubusercontent.com/101806311/176453003-da5e8e80-7acc-425c-a527-34ae23a75b95.png?raw=true "NAT Gateways")

## Sample commands to trigger dataflow pipeline execution using above options

Following section showcases how VPC-parameters can be given as CLI inputs to weather-mv dataflow pipeline.

| Python                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| weather-mv --uris "gs://<font color="brown">$STORAGE_BUCKET</font>/*.nc"<br/>--output_table "<font color="brown">$HOST_PROJECT_ID</font>.<font color="brown">$DATASET_ID</font>.<font color="brown">$TABLE_ID</font>"<br/>--temp_location "gs://<font color="brown">$STORAGE_BUCKET</font>/tmp"<br/>--runner DataflowRunner<br/>--project <font color="brown">$HOST_PROJECT_ID</font><br/>--region <font color="brown">$REGION_NAME</font><br/>--no_use_public_ips <br/>--network=<font color="brown">$NETWORK_NAME</font><br/>--subnetwork=regions/<font color="brown">$REGION_NAME</font>/subnetworks/<font color="brown">$SUBNETWORK_NAME</font><br/> | 

Replace the following:<br/>
1. STORAGE_BUCKET: the storage bucket, e.g. bucket_58231<br/>
2. HOST_PROJECT_ID: the host project ID, e.g. weather_tools<br/>
3. DATASET_ID: the name of dataset, e.g. weather_mv_ds<br/>
4. TABLE_ID: the name of table, e.g. tbl_2017_01 <br/>
5. REGION_NAME: the regional endpoint of your Dataflow job, e.g. us-west1<br/>
6. NETWORK_NAME: the name of your Compute Engine network, e.g. dataflow<br/>
a. Provide network_name same as what we created in Step-1.<br/>
8. SUBNETWORK_NAME: the name of your Compute Engine subnetwork, e.g. private<br/>
a. Provide a subnetwork_name same as what we created in Step-1.<br/>

Alternatively, you may also execute following command,

| Python                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | 
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| weather-mv --uris "gs://<font color="brown">$STORAGE_BUCKET</font>/*.nc"<br/>--output_table "<font color="brown">$HOST_PROJECT_ID</font>.<font color="brown">$DATASET_ID</font>.<font color="brown">$TABLE_ID</font>"<br/>--temp_location "gs://<font color="brown">$STORAGE_BUCKET</font>/tmp"<br/>--runner DataflowRunner<br/>--project <font color="brown">$HOST_PROJECT_ID</font><br/>--region <font color="brown">$REGION_NAME</font><br/>--no_use_public_ips<br/>–subnetwork=https://www.googleapis.com/compute/v1/projects/$HOST_PROJECT_ID/regions/$REGION_NAME/subnetworks/$SUBNETWORK_NAME<br/> | 
