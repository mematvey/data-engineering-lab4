$roles = '1..3 | ForEach-Object { "zk$_ - " + (docker exec zk$_-hw4 zkServer.sh status 2>&1 | Select-Object -Last 1) }'

docker exec zk1-hw4 zkServer.sh status
Invoke-Expression $roles

docker stop zk3-hw4
Start-Sleep 10
Invoke-Expression $roles
uv run python tools\zkcmd.py zk1 "ls /"

docker stop zk2-hw4
Start-Sleep 6
Invoke-Expression $roles
docker exec zk1-hw4 sh -c "echo srvr | nc localhost 2181"
uv run python tools\zkcmd.py zk1 "ls /" 15

docker start zk2-hw4 zk3-hw4
Start-Sleep 15
Invoke-Expression $roles
uv run python tools\zkcmd.py zk1 "ls /"
