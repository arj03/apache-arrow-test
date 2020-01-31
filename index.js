const pull = require('pull-stream')
const arrow = require('apache-arrow')
const OffsetLog = require('flumelog-aligned-offset')
const OffsetLogCompat = require('flumelog-aligned-offset/compat')
const codec = require('flumecodec/json')
const Flume = require('flumedb')

var log = OffsetLogCompat(OffsetLog(
  'log.offset',
  {blockSize:1024*64, codec:codec}
))

var db = Flume(log, true)

var start = new Date()

function datediff(date1, date2) {
  return date1.getTime() - date2.getTime();
}

pull(
  db.stream(),
  pull.collect((err, messages) => {
    console.log(`extract ${messages.length} messages, ${datediff(new Date(), start)}`)

    var flumeseqs = messages.map(x => x.seq)

    var keys = messages.map(x => x.value.key)
    var dates = messages.map(x => new Date(x.value.timestamp))
    var authors = messages.map(x => x.value.value.author)
    var sequences = messages.map(x => x.value.value.sequence)
    var types = messages.map(x => x.value.value.content.type)

    // post specific
    var channels = messages.map(x => x.value.value.content.channel)
    var roots = messages.map(x => x.value.value.content.root ? x.value.value.content.root.toString() : null)

    // indexes:
    // clock: [data.value.author, data.value.sequence] => flume seq
    // keys:  data.key => flume seq
    // ssb-query: general query, men med det vi har skulle det virke, mangler måske root?

    // backlinks: find alt som peger på noget (dest), #hashtags, @mention, %message, &blob. Dvs. det bliver en liste per besked

    // last:  data.value.author => reduced { data.key, data.value.sequence, data.value.timestamp }
    // peer-invites: value.dest => flume seq + reduced invite state
    // contacts2 (ssb-friends): reduced follow state
    
    /*
    var total = dates.length

    var t = arrow.Table.new([
      arrow.DateVector.from(dates.slice(0, total / 2)),
      arrow.Int32Vector.from(seqs.slice(0, total / 2)),
      arrow.Utf8Vector.from(keys.slice(0, total / 2)),
      arrow.Vector.from({ values: authors.slice(0, total / 2), type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
      arrow.Vector.from({ values: types.slice(0, total / 2), type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
    ], ["date", "seq", "key", "author", "type"])

    var t2 = arrow.Table.new([
      arrow.DateVector.from(dates.slice(total / 2, total)),
      arrow.Int32Vector.from(seqs.slice(total / 2, total)),
      arrow.Utf8Vector.from(keys.slice(total / 2, total)),
      arrow.Vector.from({ values: authors.slice(total / 2, total), type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
      arrow.Vector.from({ values: types.slice(total / 2, total), type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
    ], ["date", "seq", "key", "author", "type"])

    console.log(t.length)
    console.log(t2.length)

    var combined = new arrow.Table([t.chunks, t2.chunks])

    console.log(combined.length)
    console.log(`imported data into arrow ${keys.length} keys, ${datediff(new Date(), start)}`)
    */

    /*
    var queryStart = new Date()

    var today = Array.from(
      combined.filter(
	arrow.predicate.col('date').gt(new Date(2020, 0, 30)).and(
	  arrow.predicate.col('author').eq("@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519")).and(
	    arrow.predicate.col('type').eq("post"))
      )
    )

    console.log(`query ${today.length} results, ${datediff(new Date(), queryStart)}`)
    */

    var all = arrow.Table.new([
      arrow.DateVector.from(dates),
      arrow.Int32Vector.from(flumeseqs),
      arrow.Utf8Vector.from(keys),
      arrow.Vector.from({ values: authors, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
      arrow.Int32Vector.from(sequences),
      arrow.Vector.from({ values: types, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
      arrow.Vector.from({ values: channels, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
      arrow.Vector.from({ values: roots, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
    ], ["date", "flumeseq", "key", "author", "sequence", "type", "channel", "root"])
    
    console.log(all.length)
    console.log(`imported data into arrow ${keys.length} keys, ${datediff(new Date(), start)}`)

    queryStart = new Date()

    var today2 = Array.from(
      all.filter(
	arrow.predicate.col('date').gt(new Date(2020, 0, 30)).and(
	  arrow.predicate.col('author').eq("@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519")).and(
	    arrow.predicate.col('type').eq("post"))
      )
    )

    console.log(`query ${today2.length} results, ${datediff(new Date(), queryStart)}`)
    //console.log(today2)
    
    var w = arrow.RecordBatchStreamWriter.writeAll(all)
    const fs = require('fs')
    w.toUint8Array().then((data) => {
      fs.writeFileSync("index.arrow", data)
    })
    
    return
    
    //console.log(t)

    console.log(today)
  })
)

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
