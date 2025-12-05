Subject: [VOTE] Release Apache Texera (incubating) ${VERSION} RC${RC_NUM}

Hi Texera Community,

This is a call for vote to release Apache Texera (incubating) ${VERSION}.

== Release Candidate Artifacts ==

The release candidate artifacts can be found at:
https://dist.apache.org/repos/dist/dev/incubator/texera/${RC_DIR}/

The artifacts include:
- apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz (source tarball)
- apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz.asc (GPG signature)
- apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz.sha512 (SHA512 checksum)

== Git Tag ==

The Git tag for this release candidate:
https://github.com/apache/incubator-texera/releases/tag/${TAG_NAME}

The commit hash for this tag:
${COMMIT_HASH}

== Release Notes ==

Release notes can be found at:
https://github.com/apache/incubator-texera/releases/tag/${TAG_NAME}

== Keys ==

The artifacts have been signed with Key [${GPG_KEY_ID}], corresponding to [${GPG_EMAIL}].

The KEYS file containing the public keys can be found at:
https://dist.apache.org/repos/dist/dev/incubator/texera/KEYS

== How to Verify ==

1. Download the release artifacts:

   wget https://dist.apache.org/repos/dist/dev/incubator/texera/${RC_DIR}/apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz
   wget https://dist.apache.org/repos/dist/dev/incubator/texera/${RC_DIR}/apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz.asc
   wget https://dist.apache.org/repos/dist/dev/incubator/texera/${RC_DIR}/apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz.sha512

2. Import the KEYS file and verify the GPG signature:

   wget https://dist.apache.org/repos/dist/dev/incubator/texera/KEYS
   gpg --import KEYS
   gpg --verify apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz.asc apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz

3. Verify the SHA512 checksum:

   sha512sum -c apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz.sha512

4. Extract and build from source:

   tar -xzf apache-texera-${VERSION}-rc${RC_NUM}-src.tar.gz
   cd apache-texera-${VERSION}-rc${RC_NUM}-src
   # Follow build instructions in README

== How to Vote ==

The vote will be open for at least 72 hours.

Please vote accordingly:

[ ] +1 Approve the release
[ ]  0 No opinion
[ ] -1 Disapprove the release (please provide the reason)

== Checklist for Reference ==

When reviewing, please check:

[ ] Download links are valid
[ ] Checksums and PGP signatures are valid
[ ] LICENSE and NOTICE files are correct
[ ] All files have ASF license headers where appropriate
[ ] No unexpected binary files
[ ] Source tarball matches the Git tag
[ ] Can compile from source successfully

Thanks,
[Your Name]
Apache Texera (incubating) PPMC
