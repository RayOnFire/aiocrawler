sudo apt update
sudo apt install -y python-pip
pip install virtualenv
cd ~/aiocrawler
virtualenv env -p /usr/bin/python3
source env/bin/activate
pip install -e .
pip install aiohttp
cd test
echo 'Done'