sudo systemctl restart tradingbot.service
sudo systemctl restart tradingdashboard.service
sudo journalctl -u tradingdashboard.service -f
