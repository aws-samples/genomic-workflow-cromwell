import os
import aws_cdk as cdk

from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_batch as batch,
    aws_s3 as s3,
    aws_iam as iam,
    aws_s3_deployment as s3_deployment,
    aws_ecr as ecr,
    CfnOutput
)
from constructs import Construct
import time

class AwsCdkCromwellBatchStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1 Create a S3 bucket
        timestamp = int(time.time())
        cromwell_bucket_name = f"{construct_id}-cromwell-result-{cdk.Aws.REGION}-{timestamp}"
        result_bucket = s3.Bucket(
            self, "CromwellResultBucket",
            bucket_name=cromwell_bucket_name,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=False
        )

        script_bucket_name = f"{construct_id}-cromwell-script-{cdk.Aws.REGION}-{timestamp}"
        script_bucket = s3.Bucket(
            self, "CromwellScriptBucket",
            bucket_name=script_bucket_name,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=False
        )

        ref_bucket_name = f"{construct_id}-cromwell-ref-{cdk.Aws.REGION}-{timestamp}"
        ref_bucket = s3.Bucket(
            self, "CromwellRefBucket",
            bucket_name=ref_bucket_name,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=False
        )

        # Upload script to s3.
        current_dir = os.path.dirname(os.path.abspath(__file__))
        fetch_and_run_file_path = os.path.join(current_dir, "scripts")

        s3_deployment.BucketDeployment(self, "fetchAndRun",
            sources=[s3_deployment.Source.asset(fetch_and_run_file_path)],
            destination_bucket=script_bucket
        )
        
        test_path = os.path.join(current_dir, "scripts/genomicsdemo/demo-dataset/example_ref")
        s3_deployment.BucketDeployment(self, "testFastq",
            sources=[s3_deployment.Source.asset(test_path)],
            destination_bucket=ref_bucket
        )

        # 2. Create a new VPC with three public and three private subnets
        vpc = ec2.Vpc(
            self, f"{construct_id}-CromwellBatchVPC",
            max_azs=3,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=20
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=20
                )
            ]
        )

        # 3. Create a Batch Service Role
        batch_service_role_name = f"{construct_id}-CromwellBatchServiceRole-{cdk.Aws.REGION}"
        batch_service_role = iam.Role(
            self, "BatchServiceRole",
            role_name=batch_service_role_name,
            assumed_by=iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSBatchServiceRole")
            ]
        )

        # 4. Create a Batch Instance Role
        batch_instance_role_name = f"{construct_id}-CromwellBatchInstanceRole-{cdk.Aws.REGION}"
        batch_instance_role = iam.Role(
            self, "BatchInstanceRole",
            role_name=batch_instance_role_name,
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonElasticFileSystemClientReadWriteAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2ContainerRegistryReadOnly"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonECS_FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")
            ]
        )

        # 5. Create launch Template.
        user_data_script = """MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/x-shellscript; charset="us-ascii"

#!/bin/bash
yum update -y
yum install jq btrfs-progs sed git unzip lustre-client amazon-efs-utils wget -y
pip3 install botocore
curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
unzip -q /tmp/awscliv2.zip -d /tmp
/tmp/aws/install -b /usr/bin
mkdir -p /opt/aws-cli/bin
cp -a $(dirname $(find /usr/local/aws-cli -name 'aws' -type f))/. /opt/aws-cli/bin/mkdir -p /opt/ecs-additions/
aws configure set default.region %s
aws s3 cp s3://%s/fetch_and_run.sh /opt/ecs-additions/fetch_and_run.sh
chmod a+x /opt/ecs-additions/fetch_and_run.sh
cp /opt/ecs-additions/fetch_and_run.sh /usr/local/bin
aws s3 cp s3://%s/awscli-shim.sh /opt/ecs-additions/awscli-shim.sh
mv /opt/aws-cli/bin /opt/aws-cli/dist
chmod a+x /opt/ecs-additions/awscli-shim.sh
cp /opt/ecs-additions/awscli-shim.sh /opt/aws-cli/bin/aws
rm -f /usr/local/aws-cli/v2/current/bin/aws
cp /opt/ecs-additions/awscli-shim.sh /usr/local/aws-cli/v2/current/bin/aws
ln -sf /usr/local/aws-cli/v2/current/dist/aws /usr/bin/aws
wget https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm -O /tmp/mount-s3.rpm
yum install -y /tmp/mount-s3.rpm
mkdir -p /data/ref
mount-s3 --dir-mode 0555 --file-mode 0444 --allow-other %s /data/ref

--==MYBOUNDARY==--""" % (cdk.Aws.REGION, script_bucket.bucket_name, script_bucket.bucket_name, ref_bucket.bucket_name)

        user_data = ec2.UserData.for_linux()
        user_data.add_commands(user_data_script)
        launch_template = ec2.LaunchTemplate(
            self, f"{construct_id}-CromwellLaunchTemplate",
            user_data=user_data,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=500,
                    ),
                )
            ]
        )

        # 6. Create a Batch Compute Environment
        compute_env = batch.ManagedEc2EcsComputeEnvironment(
            self, "CromwellBatchComputeEnv",
            compute_environment_name=f"{construct_id}-CromwellBatchComputeEnv",
            launch_template=launch_template,
            instance_role=batch_instance_role,
            service_role=batch_service_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            instance_classes=[ec2.InstanceClass.M6I, ec2.InstanceClass.R6I, ec2.InstanceClass.C6I],
            use_optimal_instance_classes=False,
            minv_cpus=0,
            maxv_cpus=1024
        )

        # 7. Create a Batch Job Queue
        compute_queue = batch.JobQueue(self, "JobQueue", job_queue_name=f"{construct_id}-CromwellBatchQueue", priority=1)
        compute_queue.add_compute_environment(compute_env, 1)
        
        # Create Amazon ECR(Elastic Container Registry)
        ecr_repository = ecr.Repository(
            self, f"{construct_id}-EcrRepositoryUri",
            repository_name=f"{construct_id}-cromwell-repo"
        )

        # 8. Create a Cromwell Instance
        ## Create security group for the instance
        security_group = ec2.SecurityGroup(
            self, "CromwellSecurityGroup",
            vpc=vpc,
            allow_all_outbound=True,
            description="Allow SSH access",
            security_group_name=f"{construct_id}-CromwellSecurityGroup"
        )
        ## Allow SSH access from anywhere
        security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(22),
            description="Allow SSH access from anywhere"
        )
        cromwell_instance_role_name = f"{construct_id}-CromwellServerRole-{cdk.Aws.REGION}"
        cromwell_instance_role = iam.Role(
            self, "CromwellServerRole",
            role_name=cromwell_instance_role_name,
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSBatchFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")
            ]
        )

        aws_conf = """
include required(classpath("application"))

webservice {
  interface = localhost
  port = 8000
}

aws {
  application-name = "cromwell"
  auths = [{
      name = "default"
      scheme = "default"
  }]
  region = "%s"
}

call-caching {
  enabled = true
  invalidate-bad-cache-results = true
}

engine { filesystems { s3 { auth = "default" } } }

backend {
  default = "AWSBATCH"
  providers {
    AWSBATCH {
      actor-factory = "cromwell.backend.impl.aws.AwsBatchBackendLifecycleActorFactory"
      config {
        numSubmitAttempts = 2
        numCreateDefinitionAttempts = 2
        root = "s3://%s"
        auth = "default"
        default-runtime-attributes { queueArn = "%s" , scriptBucketName = "%s" , disks = ["/data/ref"] }
        filesystems {
          s3 {
            auth = "default"
            duplication-strategy: [
              "hard-link", "soft-link", "copy"
            ]
          }
        }
      }
    }
  }
}
        """ %(cdk.Aws.REGION, cromwell_bucket_name, compute_queue.job_queue_arn, script_bucket_name)

        demo_input="""
{
 "fastqtobam.fastq1" : "s3://%s/genomicsdemo/demo-dataset/demo-reads/H06JUADXX130110.1.ATCACGAT.20k_reads_1.fastq",
 "fastqtobam.fastq2" : "s3://%s/genomicsdemo/demo-dataset/demo-reads/H06JUADXX130110.1.ATCACGAT.20k_reads_2.fastq",
 "fastqtobam.Ref" : "Homo_sapiens_assembly19_part.fasta"
}
""" %(script_bucket_name, script_bucket_name)
        ## Create instance
        instance = ec2.Instance(
            self, "CromwellInstance",
            instance_name=f"{construct_id}-CromwellServer",
            instance_type=ec2.InstanceType("c6i.xlarge"),
            machine_image=ec2.MachineImage.latest_amazon_linux2(),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_group=security_group,
            role=cromwell_instance_role,
            user_data=ec2.UserData.for_linux(),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=100,
                    ),
                )
            ]
        )
        
        instance.user_data.add_commands("yum update -y")
        instance.user_data.add_commands("yum install java-11-amazon-corretto -y")
        instance.user_data.add_commands("wget https://github.com/broadinstitute/cromwell/releases/download/86/cromwell-86.jar -O /home/ec2-user/cromwell-86.jar")
        instance.user_data.add_commands("chown ec2-user:ec2-user /home/ec2-user/cromwell-86.jar")
        instance.user_data.add_commands("chmod a+x /home/ec2-user/cromwell-86.jar")
        instance.user_data.add_commands("aws configure set default.region %s" % cdk.Aws.REGION)
        instance.user_data.add_commands("aws s3 sync s3://%s/genomicsdemo /home/ec2-user/genomicsdemo" % script_bucket.bucket_name)
        instance.user_data.add_commands("chown -R ec2-user:ec2-user /home/ec2-user/genomicsdemo")
        instance.user_data.add_commands("cat > /home/ec2-user/aws.conf << EOF")
        instance.user_data.add_commands(aws_conf)
        instance.user_data.add_commands("EOF")
        instance.user_data.add_commands("cat > /home/ec2-user/genomicsdemo/demo.fastqtobam.inputs.json << EOF")
        instance.user_data.add_commands(demo_input)
        instance.user_data.add_commands("EOF")
        
        # 10. Output
        CfnOutput(self, "CromwellInstancePublicIP", value=instance.instance_public_ip)
        CfnOutput(self, "RefBucketName", value=ref_bucket.bucket_name)
        CfnOutput(self, "EcrRepositoryUri", value=ecr_repository.repository_uri)