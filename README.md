
grep "4c6fc790-1cd3-11e6-a8c2-3dbc0239079b" /var/log/fluent/events.20160518.b53318d21931ada92 | node ./streamer.js > evts.json
curl -sv '172.26.6.188:9411/api/v1/spans' -X POST -H 'Content-Type: application/json' -d@evts.json

