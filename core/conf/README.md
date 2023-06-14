# Note for conf

`jdbc.conf` is the configure file for the database connection. You need to change it to your own username and password before running Texera. It is important to avoid committing and pushing your own username and password to the remote branch, so make sure you clear the username and password before committing. It is recommended to use the command `git update-index --skip-worktree <directory>/conf/jdbc.conf` to let the git ignore your personal data. You can add it back through the command `git update-index --no-skip-worktree <directory>/conf/jdbc.conf`. Source: https://stackoverflow.com/questions/4348590/how-can-i-make-git-ignore-future-revisions-to-a-file 

`jooq-conf.xml` is the configure file for the JOOQ generation. You can modify the content if you want to change the generation details, including the package name and directory for the JOOQ generated files. To run the auto generation, go to `core/dataflow/src/main/java/edu/uci/ics/texera/dataflow/jooq/RunCodegen.java`

`web-config.yml` is the configure file for the server.