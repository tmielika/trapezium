Internal repository is hosted on stash. We mirror github repository to stash and maintain integration branch.
All internal code merges to integration branch after feature and data review. Integration branch merges to
github master regularly and we cut releases from github which comes to stash through mirroring.

Steps to mirror:

git clone --mirror https://github.com/Verizon/trapezium

cd trapezium.git/

git remote set-url --push origin https://username@mycompany.com/stash/scm/bda/trapezium.git

git fetch -p origin

git push --mirror
