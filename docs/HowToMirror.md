Internal repository is hosted on stash. BDA team mirror github repository to Verizon Stash and maintain "verizon" 
branch on stash. All internal code merges to "verizon" branch after feature and data review. "verizon" branch from 
stash merges to Github master regularly and we cut releases from github which comes to stash through automated mirroring 
from Jenkins build.

Steps to mirror:

git clone --mirror https://github.com/Verizon/trapezium

cd trapezium.git/

git remote set-url --push origin https://username@mycompany.com/stash/scm/bda/trapezium.git

git push --all stash
git push --tags stash
