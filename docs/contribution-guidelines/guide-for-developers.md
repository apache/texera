---
title: "Guide for Developers"
weight: 20
---

## 0. Requirements

#### **Java 11 JDK**

Install `Java JDK 11 (Java Development Kit)` (recommend: `[adoptopenjdk](https://adoptium.net/installation/)`). To verify the installation, run:
```console
java -version
```

Next, set `JAVA_HOME`. On macOS you can run:
```
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```
On Windows, add a system environment variable called `JAVA_HOME` that points to the JDK directory.

#### Python@3.12/3.11/3.10

Install Python 3.12 (or 3.11/3.10) from the official site or your preferred package manager.

#### **Git**

On Windows, install the software from https://gitforwindows.org/. `Git Bash` is available after installing `Git`.

On Mac and Linux, see https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

Verify the installation by:
```console
git --version
```

#### **sbt (Scala Build Tool)**

Install `sbt` for building the project. Please refer to [sbt Reference Manual — Installing sbt](https://www.scala-sbt.org/1.x/docs/Setup.html). We recommend you to use [sdkman](https://sdkman.io/install) to install sbt. 

Verify the installation by:
```console
sbt --version
```

If the above command fails on Windows after installation, it is recommended to restart your computer.

#### **node LTS Version > 18.x**

Install an LTS version (not the latest) of `node`. Currently, we require LTS version > 18.x. 

On Windows, install from [https://nodejs.org/en/](https://nodejs.org/en/).

On Mac and Linux, [use NVM to install NodeJS](https://www.linode.com/docs/guides/how-to-install-use-node-version-manager-nvm/) as it avoids permission issues.

Verify the installation by:
```console
node -v
```

#### **Angular 16 Cli**

Install the angular 16 cli globally:
```console
npm install -g @angular/cli@16
```

Verify the installation by:
```console
ng version
```
</details>

<summary>

## 1. Setup Backend Development.

</summary>

### Clone and Configure Texera

In the terminal, clone the Texera repo:
```console
git clone git@github.com:Texera/texera.git
```

Do the following changes to the configuration files:
- Edit `common/config/src/main/resources/storage.conf` to use your Postgres credentials.
```diff
    jdbc {

-        username = "postgres"
+        username = <Postgres username you have>
        username = ${?STORAGE_JDBC_USERNAME}

-        password = "postgres"
+        password = <Postgres password you have>
        password = ${?STORAGE_JDBC_PASSWORD}
    }
```

- Edit `common/config/src/main/resources/udf.conf` to use the correct python executable path(can be obtained by command `which python` or `where python`):
```diff
python {
-   path = 
+   path = "/the/executable/path/of/python"
}
```

### Setup PostgreSQL locally

Texera uses [PostgreSQL](https://www.postgresql.org/) to manage the user data and system metadata. To install and configure it:
Install [Postgres](https://www.postgresql.org/download/). If you are using Mac, simply execute:
```console
brew install postgresql
```

Install [Pgroonga](https://pgroonga.github.io/install/) for enabling full-text search, if you are using Mac, simply execute:
```console
brew install pgroonga
```

Execute `sql/texera_ddl.sql`  to create `texera_db` database for storing user system data & metadata storage
```console
psql -U postgres -f "sql/texera_ddl.sql"
```
Execute `sql/iceberg_postgres_catalog.sql`  to create the database for storing Iceberg catalogs.
```console
psql -U postgres -f "sql/iceberg_postgres_catalog.sql"
```

### Setup the LakeFS+Minio locally

Texera requires [LakeFS](https://lakefs.io/) and S3([Minio](https://min.io/docs/minio/kubernetes/upstream/index.html) is one of the implementations) as the dataset storage. Setting up these two storage services locally are required to make Texera's dataset feature functioning.

Install [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) which contains both docker engine and docker compose. Make sure you launch the Docker after installing it.

In the terminal, enter the directory containing the docker-compose file:
```
cd file-service/src/main/resources
```

Edit `docker-compose.yml` by: search for `volumes` in the file and follow the instructions in the comment. This step is required otherwise your data will be lost if containers are deleted

Execute the following command to start LakeFS and Minio:
```
docker compose up
```

### Import the project into IntelliJ


Before you import the project, you need to have "Scala", and "SBT Executor" plugins installed in Intellij.
<img width="479" alt="Screenshot 2024-12-02 at 5 59 34 PM" src="/images/github-assets/415f2130-e885-499e-8f16-44ea48942406.png">


1. In Intellij, open `File -> New -> Project From Existing Source`, then choose the `texera` folder.
2. In the next window, select `Import Project from external model`, then select `sbt`. 
3. In the next window, make sure `Project JDK` is set. Click OK. 
4. IntelliJ should import and build this Scala project. In the terminal under `texera`, run:
```
sbt clean protocGenerate
```
This will generate proto-specified codes. And the IntelliJ indexing should start. Wait until the indexing and importing is completed. And on the right, you can open the sbt tab and check the loaded `texera` project and couple of sub projects: 

<img width="616" height="859" alt="image" src="/images/github-assets/4e916543-a991-4e64-aa4f-b7775eb6b106.png" />

5. When IntelliJ prompts "Scalafmt configuration detected in this project" in the bottom right corner, select "Enable".
If you missed the IntelliJ prompt, you can check the `Event Log` on the bottom right

6. In addition to the microservices, you need to run the JOOQ code generation using `sbt DAO/jooqGenerate`, make sure to provide Postgres credentials.

### Run the backend micro services in IntelliJ
The easiest way to run backend services is in IntelliJ. 
Currently we have couple of micro services for different purposes. If one microservice failed after running, it may have dependency to another microservice, so wait for other ones to start, also make sure to run LakeFS docker compose:

| **Component** | **File Path** | **Purpose / Functionality** |
|---|---|---|
| **ConfigService** | `config-service/src/main/scala/`<br>`org/apache/texera/service/`<br>`ConfigService.scala` | Hosts the system configurations to allow the frontend to retrieve configuration data. |
| **TexeraWebApplication** | `amber/src/main/scala/`<br>`org/apache/texera/web/`<br>`TexeraWebApplication.scala` | Provides user login, community resource read/write operations, and loads metadata for available operators. |
| **FileService** | `file-service/src/main/scala/`<br>`org/apache/texera/service/`<br>`FileService.scala` | Provides dataset-related endpoints including dataset management, access control, and read/write operations across datasets. |
| **WorkflowCompilingService** | `workflow-compiling-service/src/main/scala/`<br>`org/apache/texera/service/`<br>`WorkflowCompilingService.scala` | Propagates schema and checks for static errors during workflow construction. |
| **ComputingUnitMaster** | `amber/src/main/scala/`<br>`org/apache/texera/web/`<br>`ComputingUnitMaster.scala` | Manages workflow execution and acts as the master node of the computing cluster.<br>**Must start before `ComputingUnitWorker`.** |
| **ComputingUnitWorker** | `amber/src/main/scala/`<br>`org/apache/texera/web/`<br>`ComputingUnitWorker.scala` | A worker node in the computing cluster (not a web server). |
| **ComputingUnitManagingService** | `computing-unit-managing-service/src/main/scala/`<br>`org/apache/texera/service/`<br>`ComputingUnitManagingService.scala` | Manages the lifecycle of different types of computing units and their connections to users' frontends. |
| **AccessControlService** | `access-control-service/src/main/scala/`<br>`org/apache/texera/service/`<br>`AccessControlService.scala` | Authorize requests sent to computing unit, currently not needed to run for local development, it is only used in Kubernetes setup. |



To run each of the above web service, go to the corresponding scala file(i.e. for `TexeraWebApplication`, go find TexeraWebApplication.scala), then run the main function by pressing on the green run button and wait for the process to start up. 

For `TexeraWebApplication`, the following message indicates that it is successfully running:
```
[main] [akka.remote.Remoting] Remoting now listens on addresses:
org.eclipse.jetty.server.Server: Started
```
* If IntelliJ displays CreateProcess error=206, the filename or extension is too long : [add the -Didea.dynamic.classpath=true in Help | Edit Custom VM Options and restart the IDE](https://youtrack.jetbrains.com/issue/IDEA-285090)


For `ComputingUnitMaster`, the following prompt indicates that it is successfully running:

```
---------Now we have 1 node in the cluster---------
``` 

### Enable Python-based Operators

Texera has lots of Python-based operators like visualizations, and UDF operators. To enable them, install python dependencies by executing, you also need to install R in your system:
```console
cd texera
pip install -r amber/requirements.txt -r amber/operator-requirements.txt
```

</details>



<details>
<summary>

## 2. Launch Frontend
</summary>
This is for developers that work on the frontend part of the project. This step is NOT needed if you develop the backend only.

Before you start, make sure the backend services are all running.

### Install Angular CLI
```console
cd frontend
yarn install
```

Ignore those warnings (warnings are usually marked in yellow color or start with `WARN`).

### Launch Frontend in IntelliJ for local development

1. Click on the Green Run button next to the `start` in `frontend/package.json`.
2. Wait for some time and the server will get started. Open a browser and access `http://localhost:4200`. You should see the Texera UI with a canvas.\

<img width="1000" alt="image" src="/images/github-assets/9809ffad-6991-4e9f-855a-a081e7d684d7.png" />


Every time you save the changes to the frontend code, the browser will automatically refresh to show the latest UI.
You can also run frontend using command line:
```console
yarn start
```

### Launch Frontend in the production environment

Run the following command
```
yarn run build
```
This command will optimize the frontend code to make it run faster. This step will take a while. After that, start the backend engine in IntelliJ and use your browser to access `http://localhost:8080`.


</details>


<details>
<summary>

## 3. Email Notification (Optional)
</summary>

1. Set `smtp` in `config/src/main/resources/user-system.conf`. You need an App password if the account has 2FA.
2. Log in to Texera with an admin account.
3. Open the Gmail dashboard under the admin tab.
5. Send a test email.

</details>

<details>
<summary>

## 4. Misc

</summary>

This part is optional; you only need to do this if you are working on a specific task.

### To create a new database table and write queries using Java through Jooq
1. Create the needed new table in MySQL and update `sql/texera_ddl.sql` to include the new table.
2. Run `sbt DAO/jooqGenerate` to generate the classes for the new table.

Note: Jooq creates DAO for simple operations if the requested SQL query is complex, then the developer can use the generated Table classes to implement the operation

### Disable password login
Edit `config/src/main/resources/gui.conf`, change `local-login` to `false`.

### Enforce invite only
Edit `config/src/main/resources/user-system.conf`, change `invite-only` to `true`.

### Backend endpoints Role Annotation
There are two types of permissions for the backend endpoints:
1. @RolesAllowed(Array("Role"))
2. @PermitAll
Please don't leave the permission setting blank. If the permission is missing for an endpoint, it will be @PermitAll by default.

### **Windows: enable long paths**

Some workflows create deep directories (e.g., when writing `metadata.json` via Python/ICEBERG). On Windows, this can exceed the legacy `MAX_PATH` (~260 chars) and cause failures like:

```
[WinError 3] The system cannot find the path specified.
```

Enable long paths support (per machine) by running PowerShell **as Administrator**:

```powershell
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```

Verify the setting (expected value: `1`):

```powershell
Get-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled"
```

> If you cannot change this policy (e.g., on managed devices), keep your workspace path short (e.g., `C:\src\texera`) to reduce overall path length.

### **Windows: Fix `HADOOP_HOME` errors**

On Windows, if you encounter the following error when executing a workflow:

```
Caused by: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
```

here are the steps to solve this issue:

**Steps**

1. Obtain a `winutils.exe` matching your Hadoop line (Texera currently uses Hadoop 3.3.x).
    - Suggested source (use any equivalent source approved for your environment):
      https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin
2. Create the directory and place the binary:
   ```
   C:\hadoop\bin\winutils.exe
   ```
3. In IntelliJ, add this **VM option** to the **FileService** run configuration:
   ```
   -Dhadoop.home.dir="C:\hadoop"
   ```
4. (Optional) Also set a system environment variable and restart the IDE/terminal:
   ```
   HADOOP_HOME=C:\hadoop
   ```

**Notes**

- This issue may happen only on **Windows**; macOS/Linux do not need `winutils.exe`.
- Ensure the `winutils.exe` you use matches your Hadoop major/minor (e.g., 3.3.x).
- After configuring, the prior read/write and “unset” errors should disappear.


</details>