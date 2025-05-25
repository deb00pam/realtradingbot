sudo apt update
sudo apt upgrade -y
sudo apt install python3 -y
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
sudo cp tradingbot.service /etc/systemd/system/
sudo cp tradingdashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start --now tradingbot.service
bash run.sh
