# Databricks notebook source
# Mount ADLS Gen2 Storage for CDC Pipeline

# Configuration
storage_account_name = "adfdatabricks456"
container_name = "datalake"

def mount_adls(storage_account_name, container_name):
    mount_point = f"/mnt/{container_name}"

    # Check if already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"Directory {mount_point} is already mounted.")
        return

    # Retrieve secrets from KeyVault backed secret scope
    try:
        storage_account_key = dbutils.secrets.get(scope="kv_secrets", key="storage-account-key")
    except Exception as e:
        print(f"WARNING: Secret scope 'kv_secrets' not found or key missing: {e}")
        return

    # Mount configuration
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
    extra_configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
    }

    print(f"Mounting {source} to {mount_point}...")

    try:
        dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs=extra_configs
        )
        print("Mount successful.")
    except Exception as e:
        print(f"Mount failed: {e}")

# Execute Mount
mount_adls(storage_account_name, container_name)

# Display Mounts
display(dbutils.fs.mounts())
