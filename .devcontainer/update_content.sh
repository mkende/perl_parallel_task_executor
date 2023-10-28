# This script is executed during the updateContent phase of the container lifecycle.
# It contains all the commands that need access to the repository content and so,
# cannot be executed as part of the Dockerfile.

sudo cpanm --notest --installdeps --with-develop --with-configure --with-recommends --with-suggests --with-all-features .
perl Makefile.PL
