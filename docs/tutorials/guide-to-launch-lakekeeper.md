---
title: "Guide to launch Lakekeeper as the RESTCatalog Service for Texera's workflow result storage"
weight: 50
---

This guide goes through the process of setting up Lakekeeper, which can be used as the REST Catalog service for Texera's workflow result storage. 

For more information of why using RESTCatalog, see [Issue #4126](https://github.com/apache/texera/issues/4126).                                                               
                                                                                                                                             
  ## Prerequisites
                                                                                                                                             
  - **OS: macOS or Linux**
  - Already know how to setup Texera                                                                                                         
  - A running PostgreSQL instance                                                                        
  - An accessible S3 Bucket Endpoint
  - [awscli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) needs to be installed
                                                                                                                                             
  ## Step 1: Install Lakekeeper
                                                                                                                                             
  On macOS / Linux, run

  ```bash
  brew install lakekeeper
  ```

  Verify the installation by running:

  ```bash
  lakekeeper --version
  ```
                                                                                                                                             
  > Alternatively, you can download a pre-built binary from the https://github.com/lakekeeper/lakekeeper/releases and place it on your $PATH.        
                                                                                                                                             
  ## Step 2: Create a Database for Lakekeeper in Postgres                                                                                                  
  
  Create a database using the SQL script in Texera's repository:        
                   
  ```bash
  psql -f sql/texera_lakekeeper.sql
  ```
                                                                                                                                             
  ## Step 3: Configure the Bootstrap Script
                                                                                                                                             
  Edit the **User Configuration** section at the top of `bin/bootstrap-lakekeeper.sh`.

  First, set the PostgreSQL connection URLs used by Lakekeeper:
                                     
  ```diff                                                                                                      
  -LAKEKEEPER__PG_DATABASE_URL_READ=""
  -LAKEKEEPER__PG_DATABASE_URL_WRITE=""                                                                                                      
  +LAKEKEEPER__PG_DATABASE_URL_READ="postgres://<user>:<urlencoded_password>@<host>:5432/texera_lakekeeper"
  +LAKEKEEPER__PG_DATABASE_URL_WRITE="postgres://<user>:<urlencoded_password>@<host>:5432/texera_lakekeeper"
  ```           

  If you have customized storage-related values in `common/config/src/main/resources/storage.conf` (for example, the bucket name, S3 endpoint, or MinIO credentials), check the below environment variables in the script and modify their values accordingly:

```shell
  # Storage settings — must stay in sync with storage.conf
  # if needed, update the default values after `:-` to match storage.conf
STORAGE_ICEBERG_CATALOG_REST_URI="${STORAGE_ICEBERG_CATALOG_REST_URI:-http://localhost:8181/catalog}"
STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME="${STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME:-texera}"
STORAGE_ICEBERG_CATALOG_REST_REGION="${STORAGE_ICEBERG_CATALOG_REST_REGION:-us-west-2}"
STORAGE_ICEBERG_CATALOG_REST_S3_BUCKET="${STORAGE_ICEBERG_CATALOG_REST_S3_BUCKET:-texera-iceberg}"
STORAGE_S3_ENDPOINT="${STORAGE_S3_ENDPOINT:-http://localhost:9000}"
STORAGE_S3_AUTH_USERNAME="${STORAGE_S3_AUTH_USERNAME:-texera_minio}"
STORAGE_S3_AUTH_PASSWORD="${STORAGE_S3_AUTH_PASSWORD:-password}"
```                                                                                                                                                                                                                                                                                                                                                                                      
                  
  ## Step 4: Run the Bootstrap Script                                                                                                           
  
  Run the following script in Texera repo:
  ```bash  
  bash bin/bootstrap-lakekeeper.sh  
  ```
                                                                                                            
  The script will:

  1. Start Lakekeeper if it's not already running (on http://localhost:8181)                                                                 
  2. Bootstrap the Lakekeeper server (creates the default project)
  3. Create the texera-iceberg bucket in MinIO if it doesn't exist                                                                           
  4. Register the texera warehouse with Lakekeeper, pointing at that bucket                                                                                                            
                                                                                                      
                  
  ## Step 5: Verify

  Check that Lakekeeper is healthy by running:

  ```bash
  curl http://localhost:8181/health
  ```

  You should see a JSON response with `"health":"ok"`.                                                                                         
  
  Verify that the warehouse has been created by running:                                                                                                                          
  ```bash
  curl http://localhost:8181/management/v1/warehouse
  ```

  You should see a warehouse in the response.

  ## Step 6: Switch Texera to use the REST catalog 

  To make Texera actually use the Lakekeeper REST catalog you just set up, `edit common/config/src/main/resources/storage.conf`:
  
  ```diff                                                                                                                                 
    storage {                                                                                                                               
        iceberg {
            catalog {                                                                                                                       
  -             type = postgres
  +             type = rest
                ...                                                                                                                         
            }
        }                                                                                                                                   
    }            
  ```
  
  ## Done!                                                                                                                                      
  
  Lakekeeper is now your service of managing Iceberg RESTCatalog. Texera workflows that produce Iceberg results will write to the S3 bucket via the Iceberg RESTCatalog.   
