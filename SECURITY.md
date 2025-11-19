# Security Policy

This document outlines Apache Texera (Incubating)'s security model, deployment considerations, and procedures for
reporting security vulnerabilities.

## Table of Contents

- [Security Model Overview](#security-model-overview)
- [Resources in Texera](#resources-in-texera)
- [User Categories and Responsibilities](#user-categories-and-responsibilities)
- [UI User Roles and Privileges](#ui-user-roles-and-privileges)
- [Deployments and Computing Units](#deployments-and-computing-units)
- [What is NOT a Security Issue](#what-is-not-a-security-issue)
- [Reporting Security Vulnerabilities](#reporting-security-vulnerabilities)

## Security Model Overview

Texera's security architecture is built around:

1. **Authentication**: JWT-based token authentication with configurable expiration
2. **Authorization**: Role-based access control (RBAC) with four user roles
3. **Resource Access Control**: Fine-grained privileges for datasets, workflows, and computing units
4. **Deployment Isolation**: Separate security considerations for different deployment modes

## Resources in Texera

In Texera, a **resource** is any object within the system that can be created, accessed, modified, or shared by users
via the web application. Understanding resource types and how access to them is managed is critical to following
Texera’s security model.

### Resource Types

Texera supports the following resource types:

- **Datasets**: Input data imported or uploaded for workflow processing
- **Workflows**: Data analytics pipelines defined by users
- **Computing Units**: Execution environments for running workflows (e.g., Kubernates PODs)
- **Results**: Output from workflow executions, including but not limited to data, logs, metrics, and visualizations

### Resource Ownership and Access Control

Every resource is owned by a user. The owner controls the resource's visibility and can share it with other users by
granting access permissions:

- **READ**: View the resource and its contents
- **WRITE**: Modify, execute, delete, and share the resource
- **NONE**: No access to the resource

Resources can be shared with specific users or made public. Public resources are visible to all users. Resource owners
can modify access permissions at any time.

### Resource Visibility

- Users can only see resources for which they have at least READ access.
- Access changes (e.g., revoking WRITE or READ) take effect immediately for affected users.

## User Categories and Responsibilities

Texera's security model distinguishes between two categories of users with distinct responsibilities:

### Deployment Managers

They have the highest level of access and control. They install and configure Texera, and make decisions about
technologies, deployment modes, and permissions. They can potentially delete the entire installation and have access to
all credentials, including database passwords, JWT secrets, and API keys. Deployment managers have full access to:

- The underlying infrastructure (servers, Kubernetes clusters, cloud resources)
- Database administration (e.g., PostgreSQL)
- All configuration files, environment variables, and secrets
- Network and security settings
- Container orchestration and system logs

Deployment managers can also decide to keep audits, backups, and copies of information outside of Texera, which are not
covered by Texera's security model. They operate outside the Texera UI role system and may or may not have a UI user
account.

### UI Users

**Who They Are**: Individuals who interact with Texera through the web interface.

**Access Level**: Application-level access only. UI users work within the Texera platform but do not have access to:

- The underlying infrastructure (servers, Kubernetes cluster)
- Database administration
- System configuration files
- Network and firewall settings
- Container orchestration

**Roles**: UI users are assigned one of four roles (INACTIVE, RESTRICTED, REGULAR, ADMIN) that control their permissions
within the Texera application.

**Security Scope**: UI users are responsible for:

- Protecting their login credentials
- Managing access to their resources, e.g., datasets and workflows
- Following organizational data security policies

## UI User Roles and Privileges

Texera implements four UI user roles with increasing levels of privilege. These roles control what users can do **within
the Texera web application** and do not grant infrastructure-level access.

### 1. INACTIVE

Users with this role cannot log in to the system or access any resources. This is the default role for new registrations
awaiting approval in controlled environments.

### 2. RESTRICTED

Users with this role cannot log in to the system or access any resources. Unlike INACTIVE users, RESTRICTED accounts
typically represent users who previously used Texera but are now inactive and no longer use it. Any resources they
created in the past remain in the system but are inaccessible to them. This role is used to preserve historical data
while preventing further access.

### 3. REGULAR

Users with this role can create and manage their own resources (datasets, workflows, computing units). They have full
READ and WRITE access to resources they own, and their access to other users' resources is determined by granted
permissions (see Resources section above).

They cannot:

- Access other users' private resources without granted permissions
- Manage user accounts or change user roles
- Access system configuration, logs, or global settings

This is the standard role for data scientists, analysts, and researchers.
**Note**: REGULAR users can execute arbitrary code within workflows, so this role should only be granted to trusted
individuals.

### 4. ADMIN

Users with this role are application administrators who manage users and resources through the web interface.

They have all REGULAR privileges, plus:

- Manage all UI user accounts (create, modify, and delete users)
- Change user roles
- View user login information.
- Configure application settings available in the web interface

They cannot:

- Access the underlying servers or Kubernetes cluster
- Modify JWT secrets or database passwords
- Configure HTTPS/TLS or network settings
- Access system-level logs or SSH into servers

**Note**: ADMIN is an application-level role, not an infrastructure administrator. For infrastructure management,
deployment manager access is required.

## Deployments and Computing Units
Texera can be deployed in several configurations, such as local development, single-node setups, or distributed Kubernetes 
clusters. For details on supported deployment options and their operational differences, see the deployment guides in
our [wiki](https://github.com/apache/texera/wiki/How-to-run-Texera-on-local-Kubernetes).

### Computing Unit Types

Texera executes workflows on **computing units**. UI users (REGULAR and ADMIN) can execute arbitrary code (e.g., through
UDFs written in Python, R, Scala) within computing units as part of their workflows. This code is currently not
sandboxed or restricted by Texera. Deployment managers configure which types of computing units are available:

#### Local Computing Units

Local computing units run as processes on the same machine as the Texera services (single-node deployment).

**Security characteristics**:

- Suitable for development, testing, and small team use
- All computing units share the same host machine
- No infrastructure-level isolation between users' workflows
- Deployment managers control all computing resources

**Security considerations**:

- Users' workflow code executes on the host machine with limited isolation
- Deployment managers must trust all REGULAR and ADMIN users
- Resource exhaustion by one user can affect all users

#### Kubernetes Computing Units

Kubernetes computing units run as separate PODs in a Kubernetes cluster. Each computing unit is dynamically created when
a user needs it.

**Security characteristics**:

- Suitable for production environments and multi-tenant deployments
- Each computing unit runs in an isolated Kubernetes pod
- UI users configure resource limits (CPU, memory, GPU) per pod
- Pods can be scheduled across multiple nodes for better resource distribution

**Security considerations**:

- Better isolation between users compared to local computing units
- Kubernetes provides namespace and pod-level isolation
- Resource limits prevent individual users from consuming excessive resources
- Container security and image scanning should be implemented
- Deployment managers must secure the Kubernetes cluster infrastructure

### What is NOT Guaranteed

Texera's security model does NOT guarantee:

- Protection against malicious code in user workflows (users can execute arbitrary code)
- Strong isolation between workflows in local computing units
- Complete isolation between workflows in Kubernetes computing units within the same namespace
- Protection against infrastructure-level compromises
- Protection against deployment manager misconfigurations
- DDoS protection (requires external infrastructure)
- Compliance with specific regulatory requirements without additional configuration

## What are NOT Security Issues

The following are **NOT considered security vulnerabilities** in Texera:

### User Code Execution

REGULAR and ADMIN users can execute arbitrary code (Python, R, Scala) within computing units. This is by design - Texera
is a data analytics platform where custom code execution is a core feature. The system currently does not sandbox user
code beyond the isolation provided by the deployment environment (local processes or Kubernetes pods). Deployment
managers should use resource limits, monitor usage, and restrict user roles appropriately.

### Resource Consumption

Users can create workflows that consume significant CPU, memory, or storage. Texera is designed for data-intensive
workloads. Deployment managers control this through computing unit resource limits, quotas, and monitoring.

### Information Disclosure within Authorized Access

Users with READ or WRITE access to a resource can view all its contents. Access control is at the resource level - once
access is granted, full visibility is expected. Resource owners should grant access only to trusted users.

### Public Resources

Resources marked as public are visible to all users. Public sharing is a deliberate collaboration feature. Users should
review resources before making them public and avoid including sensitive data or credentials.

### Issues Requiring Deployment Manager Access

Issues requiring physical access to servers, administrative access to infrastructure, database access, or access to
configuration files are out of scope. These access levels are considered trusted.

### Third-Party Dependencies

Theoretical vulnerabilities in dependencies that have not been exploited in Texera's usage are not in scope.
You are they are welcome to raise an issue or a PR.

## Reporting Security Vulnerabilities

The [Apache Software Foundation](https://apache.org/) takes a rigorous stance on eliminating security issues in its software projects. If you
find a security bug, with that in mind, please **DO NOT** file public issues (e.g., GitHub issues). Before reporting a
security issue, check the security model declared above. To report a new vulnerability you have discovered, please
follow the ASF security [vulnerability reporting process](https://apache.org/security/#reporting-a-vulnerability).
The Texera community follows the ASF
security [vulnerability handling process](https://apache.org/security/#vulnerability-handling), and will fix it as soon
as possible.

## Changes to This Policy

This security policy may be updated from time to time. Significant changes will be announced on the project mailing
lists and website.

---

**Last Updated**: November 2025

**Disclaimer**: This project is currently undergoing incubation at The Apache Software Foundation (ASF). Incubation is
required of all newly accepted projects until a further review indicates that the infrastructure, communications, and
decision-making process have stabilized in a manner consistent with other successful ASF projects. While incubation
status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project
has yet to be fully endorsed by the ASF.

