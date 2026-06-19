---
title: Security
description: Comprehensive guide to Texera's security model, user roles, access control, and vulnerability reporting
weight: 70
---

{{% pageinfo %}}
This page provides comprehensive information about Texera's security model, including authentication mechanisms, authorization policies, user roles, resource access control, and guidelines for reporting security vulnerabilities. Understanding these security features is essential for deployment managers and users to ensure secure operation of Texera installations.
{{% /pageinfo %}}

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
Texera's security model.

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
our [wiki](/docs/getting-started/run-on-kubernetes/).

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

## What is NOT a Security Issue

These are things that we are well aware of, and have been reported to us many times, but we do not class as a security vulnerability. Please do not report them.

Issues not classed as security relevant:

- A lack of DMARC or SPF record on our domains
- "Clickjacking" on our domains
- Directory listings. These are deliberate and do not contain sensitive information
- Systems that disclose the versions of the servers and software we use
- Data that is publically accessible in our Jira bug tracking system

## Reporting Security Vulnerabilities

We strongly encourage you to report potential security vulnerabilities to one of our private security mailing lists first, before disclosing them in a public forum.

A list of security contacts for Apache projects is available. If you can't find a project-specific security e-mail address and you have an undisclosed security vulnerability to report, use the general security address below.

**Only use the security contacts to report undisclosed security vulnerabilities in Apache projects and manage the process of fixing such vulnerabilities. We cannot accept regular bug reports or other security-related queries at these addresses. We will ignore mail sent to these addresses that does not relate to an undisclosed security problem in an Apache project.**

**Also note that the security team handles vulnerabilities in Apache projects, not running ASF services. Send reports of vulnerabilities in ASF services to root@apache.org. (This includes issues with apache.org websites)**

The general security mailing list address is: security@apache.org. This is a private mailing list.

Please send one plain-text, unencrypted, email for each vulnerability you are reporting. We may ask you to resubmit your report if you send it as an image, movie, HTML, or PDF attachment when you could as easily describe it with plain text.

