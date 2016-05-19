var assert = require('assert-plus');
var LineStream = require('lstream');

var scratchData = {};

function processLines(_objHandler, _endHandler) {
    var lstreamOpts = {encoding: 'utf8'};

    lstream = new LineStream(lstreamOpts);

    lstream.on('readable', function _onLstreamReadable() {
        var line;

        // read the first line
        line = lstream.read();
        while (line !== null) {
            assert.string(line, 'line');

            // just let it throw if not JSON: that's a bug
            obj = JSON.parse(line.trim());
            _objHandler(obj);

            // read the next line
            line = lstream.read();
        }
    });

    lstream.on('end', function() { // emitted at the end of file
        _endHandler();
    });

    // send our stdin to the lstream
    process.stdin.pipe(lstream);
}

function translateAnnotationValue(kind) {
    switch (kind) {
        case 'client.response':
            return 'cr';
        case 'client.request':
            return 'cs';
        case 'server.request':
            return 'sr';
        case 'server.response':
            return 'ss';
        default:
            return kind;
    }
}

function findDuration(obj) {
    var i;
    var min_time = NaN;
    var max_time = NaN;

    // XXX assert at least 1 annotation w/ timestamp

    for (i = 0; i < obj.annotations.length; i++) {
        if (isNaN(min_time) || obj.annotations[i].timestamp < min_time) {
            min_time = obj.annotations[i].timestamp;
        }
        if (isNaN(max_time) || obj.annotations[i].timestamp > max_time) {
            max_time = obj.annotations[i].timestamp;
        }
    }

    return (max_time - min_time);
}

function arrayifyBinaryAnnotations(obj) {
    var i;
    var newBA = [];

    Object.keys(obj.binaryAnnotations).forEach(function (k) {
        newBA.push({
            key: k,
            value: obj.binaryAnnotations[k]
        });
    });

    obj.binaryAnnotations = newBA;
}

function objHandler(obj) {
    var id;
    var span = {};

    if (!obj.evt || !obj.evt.span_id) {
        // ignore old-style records and non-evt records
        return;
    }

    id = obj.evt.span_id;

    if (!scratchData.hasOwnProperty(id)) {
        scratchData[id] = {
            annotations: [],
            binaryAnnotations: {
                reqId: obj.evt.trace_id
            }, // obj for now, we'll convert later
            id: id.replace(/-/g, '').substr(0,16),
            name: obj.evt.operation,
            parentId: obj.evt.parent_span_id.replace(/-/g, '').substr(0,16),
            timestamp: Date.parse(obj.time) * 1000,
            traceId: obj.evt.trace_id.replace(/-/g, '').substr(0,16)
        };
    }

    if (scratchData[id].name.match(/restifyclient\.[A-Z]/)
        && !obj.evt.operation.match(/restifyclient\.[A-Z]/)) {
        // The restifyclient.{GET,POST,etc} logs are intended to be placeholders
        // until we know the endpoint name (at the server). So when we have the
        // actual name, use that in preference.
        scratchData[id].name = obj.evt.operation;
    }

    // Each "log" entry has a timestamp and will be considered an "annotation"
    // in Zipkin's terminology.
    scratchData[id].annotations.push({
        endpoint: {
            ipv4: 0,
            port: 0,
            serviceName: obj.name
        }, timestamp: Date.parse(obj.time) * 1000,
        value: translateAnnotationValue(obj.evt.kind)
    });

    // If we've got the server.request, that's the more official name for the
    // endpoint so we'll replace the span.name with that.
    if (obj.evt.kind === 'server.request') {
        scratchData[id].name = obj.evt.operation;
    }

    if (!obj.evt.tags.hasOwnProperty('hostname')) {
        obj.evt.tags.hostname = obj.hostname;
    }
    if (obj.evt.kind.match(/client\./)) {
        obj.evt.tags.clientPid = obj.pid;
    } else if (obj.evt.kind.match(/server\./)) {
        obj.evt.tags.serverPid = obj.pid;
    } else {
        obj.evt.tags.pid = obj.pid;
    }

    Object.keys(obj.evt.tags).forEach(function _addTag(k) {
        scratchData[id].binaryAnnotations[k] = obj.evt.tags[k].toString();
    });
}

function endHandler() {
    var results = [];

    Object.keys(scratchData).forEach(function (k) {
        var obj = scratchData[k];

        obj.duration = findDuration(obj);
        arrayifyBinaryAnnotations(obj);

        results.push(obj);
    });

    console.log(JSON.stringify(results, null, 2));
}

processLines(objHandler, endHandler);
