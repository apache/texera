Subject: [VOTE] Release Apache Texera (incubating) ${VERSION} RC${RC_NUM}

Hi Texera Community,

This is a call for vote to release Apache Texera (incubating) ${VERSION}.

== Release Candidate Artifacts ==

https://dist.apache.org/repos/dist/dev/incubator/texera/${VERSION}-RC${RC_NUM}/

The directory contains:
- Source tarball (.tar.gz) with GPG signature (.asc) and SHA512 checksum (.sha512)
- Docker Compose deployment bundle with GPG signature and SHA512 checksum
- Helm chart package with GPG signature and SHA512 checksum

== Container Images ==

Container images are available at:
  ${IMAGE_REGISTRY}/texera-dashboard-service:${VERSION}
  ${IMAGE_REGISTRY}/texera-workflow-execution-coordinator:${VERSION}
  ${IMAGE_REGISTRY}/texera-workflow-compiling-service:${VERSION}
  ${IMAGE_REGISTRY}/texera-file-service:${VERSION}
  ${IMAGE_REGISTRY}/texera-config-service:${VERSION}
  ${IMAGE_REGISTRY}/texera-access-control-service:${VERSION}
  ${IMAGE_REGISTRY}/texera-workflow-computing-unit-managing-service:${VERSION}

These images are built from the source tarball included in this release.
The Dockerfiles are included in the source for audit and verification.

== Git Tag ==

https://github.com/apache/texera/releases/tag/${TAG_NAME}
Commit: ${COMMIT_HASH}

== Keys ==

The release was signed with GPG key [${GPG_KEY_ID}] (${GPG_EMAIL})
KEYS file: https://downloads.apache.org/incubator/texera/KEYS

== Vote ==

The vote will be open for at least 72 hours.

[ ] +1 Approve the release
[ ]  0 No opinion
[ ] -1 Disapprove the release (please provide the reason)

== Checklist ==

[ ] Checksums and PGP signatures are valid
[ ] LICENSE and NOTICE files are correct
[ ] All files have ASF license headers where appropriate
[ ] No unexpected binary files
[ ] Source tarball matches the Git tag
[ ] Can compile from source successfully
[ ] Docker Compose bundle deploys successfully with the published images
[ ] Helm chart deploys successfully (if applicable)

Thanks,
[Your Name]
Apache Texera (incubating) PPMC
