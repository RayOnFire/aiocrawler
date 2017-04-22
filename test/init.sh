sudo apt update
sudo apt install -y python-pip
sudo apt install -y python3-dev
pip install virtualenv
cd ~/aiocrawler
virtualenv env -p /usr/bin/python3
source env/bin/activate
pip install -e .
pip install aiohttp
pip install psutil
cd test
python pixiv.py $1 $2
exit $?; fi