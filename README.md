# maintenance
## useful maintenance commands for our ec2 machines

https://medium.com/@m.yunan.helmy/increase-the-size-of-ebs-volume-in-your-ec2-instance-3859e4be6cb7

## overall disk usage
du -sh

## top directories by size
sudo du -hsx * | sort -rh | head -10

## trim /var/log/journal/
sudo journalctl --vacuum-size=200M
sudo journalctl --vacuum-time=10d

## unused docker images
docker system prune -a

## yarn cache
yarn cache clean

## find out why ec2 instance rebooted
journalctl --since "1 hour ago" --no-pager | grep -i "reboot\|shutdown\|panic\|oom"
journalctl -b -1 -n 50 --no-pager
last -x reboot shutdown
cat /var/log/unattended-upgrades/unattended-upgrades.log
