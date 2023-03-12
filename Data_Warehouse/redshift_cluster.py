"""
Creating/Deleting Redshift Cluster using the AWS python SDK
"""
import argparse
import configparser
import json
import sys
import time

import boto3
import pandas as pd
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

parser = argparse.ArgumentParser(
    description="Request update of configuration",
    #    formatter_class=formatter,
    conflict_handler="resolve",
)
required_args = parser.add_argument_group("required arguments")

required_args.add_argument("--create", "-c", action="store_true")
required_args.add_argument("--delete", "-d", action="store_true")


def create_iam_role(iam):
    """Create new IAM role."""
    try:
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description=(
                "Allows Redshift clusters to" " call AWS services on your behalf."
            ),
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)


def create_redshift_cluster(role_arn, redshift):
    """Create Redshift cluster.
    Args:
        role_arn (STR): Data Warehouse role ARN.
    """
    redshift.create_cluster(
        # HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        # Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        # Roles (for s3 access)
        IamRoles=[role_arn],
    )
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]

    print(prettyRedshiftProps(myClusterProps))

    while (
        redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
            "Clusters"
        ][0]["ClusterStatus"]
        != "available"
    ):
        time.sleep(2)
    print(f"Redshift cluster is available!")

    return redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
        "Clusters"
    ][0]


def open_tcp(myClusterProps, ec2):
    """
    Open an incoming  TCP port to access the cluster endpoint.
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
    except Exception as e:
        print(e)


def prettyRedshiftProps(props):
    pd.set_option("display.max_colwidth", -1)
    keysToShow = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def create_resources():
    # Create clients for EC2, S3, IAM, and Redshift
    ec2 = boto3.resource(
        "ec2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )

    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )

    redshift = boto3.client(
        "redshift",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )

    # Create a new IAM Role
    create_iam_role(iam)

    # Attaching Policy
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    # Get the IAM role ARN
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

    # Redshift Cluster
    myClusterProps = create_redshift_cluster(roleArn, redshift)

    DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]
    DWH_ROLE_ARN = myClusterProps["IamRoles"][0]["IamRoleArn"]
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    open_tcp(myClusterProps, ec2)


def delete_redshift_cluster(redshift):
    """Delete cluster as requested."""
    rds = boto3.client("rds", region_name="us-west-2")
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ClusterNotFound":
            print("Cluster is not found to delete!")
            return
        else:
            print("Unexpected error: %s" % e)

        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        print(prettyRedshiftProps(myClusterProps))

    try:
        while (
            redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
                "Clusters"
            ][0]["ClusterStatus"]
            == "deleting"
        ):
            time.sleep(2)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ClusterNotFound":
            print("Cluster deleted.")
        else:
            print("Unexpected error: %s" % e)


def delete_iam_role(iam):
    """Delete iam Role."""
    iam.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def clean_up_resources():
    redshift = boto3.client(
        "redshift",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )
    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )
    delete_redshift_cluster(redshift)
    delete_iam_role(iam)


def main(argv):
    args = parser.parse_args(argv)
    create = args.create
    delete = args.delete

    if create:
        create_resources()
    if delete:
        clean_up_resources()


if __name__ == "__main__":
    main(sys.argv[1:])
