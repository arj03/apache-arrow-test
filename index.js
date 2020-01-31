const os = require('os')
const path = require('path')
const pull = require('pull-stream')
const arrow = require('apache-arrow')

const dir = path.join(os.homedir(), ".ssb-lite")

const EventEmitter = require('events')
SSB = {
    events: new EventEmitter()
}

const s = require('sodium-browserify')
s.events.on('sodium-browserify:wasm loaded', function() {

    console.log("wasm loaded")
    
    var net = require('./node_modules/ssb-browser-core/net').init(dir)
    var db = require('./node_modules/ssb-browser-core/db').init(dir, net.id)    


    console.log("my id: ", net.id)

    var helpers = require('./node_modules/ssb-browser-core/core-helpers')

    var validate = require('ssb-validate')
    var state = validate.initial()

    SSB = Object.assign(SSB, {
	db,
	net,
	dir,

	validate,
	state,

	connected: helpers.connected,

	validMessageTypes: ['post', 'peer-invite/confirm', 'peer-invite/accept', 'peer-invite'],
	privateMessages: true,

	remoteAddress: 'net:between-two-worlds.dk:8008~shs:lbocEWqF2Fg6WMYLgmfYvqJlMfL7hiqVAV6ANjHWNw8=.ed25519',
    })

    var start = new Date()

    function datediff(date1, date2) {
	return date1.getTime() - date2.getTime();
    }
    
    pull(
	db.store.stream(),
	pull.collect((err, messages) => {
	    console.log(`extract ${messages.length} messages, ${datediff(new Date(), start)}`)
	    
	    var keys = messages.map(x => x.value.key)
	    var seqs = messages.map(x => x.seq)
	    var dates = messages.map(x => new Date(x.value.timestamp))
	    var authors = messages.map(x => x.value.value.author)
	    var types = messages.map(x => x.value.value.content.type)
	    
	    var t = arrow.Table.new([
		arrow.DateVector.from(dates),
		arrow.Int32Vector.from(seqs),
		arrow.Utf8Vector.from(keys),
		arrow.Vector.from({ values: authors, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
		arrow.Vector.from({ values: types, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
	    ],
	    ["date", "seq", "key", "author", "type"]
	   )

	    console.log(`imported data into arrow ${keys.length} keys, ${datediff(new Date(), start)}`)

	    var today = Array.from(
		t.filter(
		    arrow.predicate.col('date').gt(new Date(2020, 0, 30)).and(
			arrow.predicate.col('author').eq("@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519")).and(
			    arrow.predicate.col('type').eq("post"))
		)
	    )

	    console.log(`query ${today.length} results, ${datediff(new Date(), start)}`)

	    var w = arrow.RecordBatchStreamWriter.writeAll(t)
	    const fs = require('fs')
	    w.toUint8Array().then((data) => {
		fs.writeFileSync("index.arrow", data)
	    })
	    
	    return
	    
	    //console.log(t)

	    console.log(today)
	})
    )
    
    return
    
    /*
    process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0
    const http = require('https');

    var str = ''
    
    const request = http.get("https://between-two-worlds.dk:8989/blobs/get/&Th+atKGMs9Hb7DhishRD30olvoY+T8qHpvo2NeCq9rs=.sha256", (response) => {
	response.setEncoding('utf8')
	response.on('data', (body) => {
	  str += body
	})

	response.on('end', () => {
	  helpers.initialSync(JSON.parse(str))
	})
    });
   */
})
