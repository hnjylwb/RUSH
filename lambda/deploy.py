#!/usr/bin/env python3
"""
Deploy Lambda Function to AWS
"""

import os
import sys
import json
import zipfile
import tempfile
import shutil
import subprocess
import boto3
from pathlib import Path


def install_dependencies(target_dir):
    """Install DuckDB into target directory"""
    print("Installing dependencies")

    subprocess.run([
        sys.executable, '-m', 'pip', 'install',
        'duckdb',
        '-t', str(target_dir),
        '--platform', 'manylinux2014_x86_64',
        '--only-binary', ':all:',
        '--quiet'
    ], check=True)


def create_deployment_package():
    """Create ZIP file with code and dependencies"""
    print("Creating deployment package")

    lambda_dir = Path(__file__).parent

    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = Path(temp_dir) / 'package'
        package_dir.mkdir()

        # Install dependencies
        install_dependencies(package_dir)

        # Copy Lambda function
        shutil.copy(lambda_dir / 'lambda_function.py', package_dir / 'lambda_function.py')

        # Create ZIP
        zip_path = lambda_dir / 'function.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = Path(root) / file
                    arcname = file_path.relative_to(package_dir)
                    zipf.write(file_path, arcname)

        size_mb = zip_path.stat().st_size / (1024 * 1024)
        print(f"Package size: {size_mb:.1f} MB")

        return zip_path


def get_or_create_role(iam_client, role_name):
    """Get existing role or create new one"""

    # Check if exists
    try:
        response = iam_client.get_role(RoleName=role_name)
        print(f"Using existing role: {role_name}")
        return response['Role']['Arn']
    except iam_client.exceptions.NoSuchEntityException:
        pass

    # Create role
    print(f"Creating role: {role_name}")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    response = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy)
    )

    # Attach policies
    iam_client.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
    )
    iam_client.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    # Wait for role propagation
    import time
    time.sleep(10)

    return response['Role']['Arn']


def deploy_function(lambda_client, function_name, zip_path, role_arn):
    """Deploy or update Lambda function"""

    with open(zip_path, 'rb') as f:
        zip_content = f.read()

    # Check if function exists
    try:
        lambda_client.get_function(FunctionName=function_name)
        exists = True
    except lambda_client.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        print(f"Updating function: {function_name}")
        lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
    else:
        print(f"Creating function: {function_name}")
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.11',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_content},
            Timeout=900,
            MemorySize=3008,
            Environment={
                'Variables': {
                    'DATA_LOCATION': 's3://rush-data/tpch'
                }
            }
        )

    print("Deployment completed")


def main():
    function_name = 'rush-query-executor'
    role_name = f'{function_name}-role'

    print(f"Deploying Lambda function: {function_name}")

    # Initialize AWS clients (uses default profile/region)
    lambda_client = boto3.client('lambda')
    iam_client = boto3.client('iam')

    # Create deployment package
    zip_path = create_deployment_package()

    # Get or create IAM role
    role_arn = get_or_create_role(iam_client, role_name)

    # Deploy function
    deploy_function(lambda_client, function_name, zip_path, role_arn)

    # Cleanup
    zip_path.unlink()


if __name__ == '__main__':
    main()
