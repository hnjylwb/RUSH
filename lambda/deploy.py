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
import yaml
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


def deploy_function(lambda_client, function_name, zip_path, role_arn, memory_mb):
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
        print(f"Updating function: {function_name} ({memory_mb}MB)")
        lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
        # Update memory configuration
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            MemorySize=memory_mb,
            Timeout=900
        )
    else:
        print(f"Creating function: {function_name} ({memory_mb}MB)")
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.11',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_content},
            Timeout=900,
            MemorySize=memory_mb,
            Environment={
                'Variables': {
                    'DATA_LOCATION': 's3://ease-data/tpch'
                }
            }
        )

    print(f"Deployment completed: {function_name}")


def load_config():
    """Load configuration from services.yaml"""
    # Get config directory (relative to this script)
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / 'config' / 'services.yaml'

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Please ensure config/services.yaml exists"
        )

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Extract FaaS configuration
    faas_config = config.get('services', {}).get('faas', {})
    if not faas_config:
        raise ValueError("No FaaS configuration found in services.yaml")

    return faas_config


def main():
    print("Loading configuration from config/services.yaml")
    try:
        faas_config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)

    # Get memory sizes from configuration
    memory_sizes_gb = faas_config.get('memory_sizes_gb', [2, 4, 6, 8, 10])

    # Generate Lambda configurations from memory sizes
    lambda_configs = []
    for memory_gb in sorted(memory_sizes_gb):
        function_name = f'ease-query-executor-{memory_gb}gb'
        memory_mb = int(memory_gb * 1024)
        lambda_configs.append((function_name, memory_mb))

    role_name = 'ease-query-executor-role'

    print(f"\nDeploying {len(lambda_configs)} Lambda functions")
    print(f"Memory configurations: {memory_sizes_gb}")
    print("=" * 60)

    # Initialize AWS clients (uses default profile/region)
    lambda_client = boto3.client('lambda')
    iam_client = boto3.client('iam')

    # Create deployment package (same code for all functions)
    zip_path = create_deployment_package()

    # Get or create IAM role (shared by all functions)
    role_arn = get_or_create_role(iam_client, role_name)

    # Deploy each Lambda function
    for function_name, memory_mb in lambda_configs:
        print()
        deploy_function(lambda_client, function_name, zip_path, role_arn, memory_mb)

    # Cleanup
    zip_path.unlink()

    print()
    print("=" * 60)
    print(f"All {len(lambda_configs)} Lambda functions deployed successfully!")
    print()
    print("Functions:")
    for function_name, memory_mb in lambda_configs:
        print(f"  - {function_name}: {memory_mb}MB ({memory_mb / 1024:.0f}GB)")



if __name__ == '__main__':
    main()
