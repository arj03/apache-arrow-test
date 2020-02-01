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

const ref = require('ssb-ref')
const matchChannel = /^#[^\s#]+$/

/*
[ [ '@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519' ],
  [ '#testing' ],
  [ '%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256',
    '#testing' ],
  [ '&xGsuLb0REV+6iDAy4QGvGxQvaIcRGZFiqBgXD8dw7aM=.sha256' ],
  [ '&xwYBP2fBpyobN19U5RO+f9BwxwX/BWOAxXJ2Q8S9W+0=.sha256' ],
  [ '@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519',
    '&Il2SFDKScJcqt3CTl+ZaeIJLXGwmPbQHUTi9lVaUH5c=.sha256' ],
  [ '%ZbyoNNv9dF0sjXngPjJAifwnMTWmlsXWyq0KLB7BoTQ=.sha256',
    '%orkjiS9Rk37b4yQuyMnF08QwK8hL4X/wglUCtOflaWs=.sha256',
    '#patchwork-dev' ],
  [ '#patchwork-design' ],
  [ '%1P6WqqYofmV7+494nQGBBg9S78/eFU9689py6zwOfs0=.sha256',
    '%JSOpWWQ2b8QNZBSncz6G+ppSUHZIZJVI6jj4nV8Ce38=.sha256',
    '#new-people' ],
  [ '%2fUo4+ARCO8Ue8PKI324ZuxtO19BGppAzJ8VUxOZtNY=.sha256',
    '%wNkP/PymJa3nrJH+NhsNFbFHQBD6o1t6qKssF1pK3+E=.sha256',
    '#patchwork-design' ] ]
*/

var extractRegex = /([@%&][A-Za-z0-9\/+]{43}=\.[\w\d]+)/
var channelExtractRegex = /(#[A-Za-z0-9\/+]{30})/

function type(id) {
  if(typeof(id) !== 'string') return false
  var c = id.charAt(0)
  if (c == '@' && ref.isFeedId(id))
    return 'feed'
  else if (c == '%' && ref.isMsgId(id))
    return 'msg'
  else if (c == '&' && ref.isBlobId(id))
    return 'blob'
  else
    return false
}

function extractLinks(msg) {
  //  console.log("====")

  /*
  var links = new Set(extractRegex.exec(msg.value.content.text))

  var channels = channelExtractRegex.exec(msg.value.content.text)
  for (var i = 0; channels != null && i < channels.length; ++i)
    links.add(`#${ref.normalizeChannel(channels[i].slice(1))}`)

  for (var k in msg.value.content) {
    var t = type(msg.value.content[k])
    console.log(`type: ${t}, ${k}: ${msg.value.content[k]}`)
    if (t == 'channel')
      links.add(`#${ref.normalizeChannel(msg.value.content["channel"])}`)
    else if (t)
      links.add(msg.value.content[k])
  }

  return Array.from(links)
  */

  var links = new Set()
  walk(msg.value.content, function (path, value) {
    if (Array.isArray(path) && path[0] === 'channel') { // HACK: handle legacy channel mentions
      var channel = ref.normalizeChannel(value)
      if (channel)
        value = `#${channel}`
    }

    // TODO: should add channel matching to ref.type
    if (type(value))
      links.add(value)
    else if (isChannel(value))
      links.add(`#${ref.normalizeChannel(value.slice(1))}`)
  })

  return Array.from(links)
}

function isChannel (value) {
  return typeof value === 'string' && value.length < 30 && matchChannel.test(value)
}

function walk (obj, fn, prefix) {
  //console.log(`walking ${JSON.stringify(obj)}, prefix: ${prefix}`)
  if (obj && typeof obj === 'object') {
    for (var k in obj) {
      walk(obj[k], fn, (prefix || []).concat(k))
    }
  } else {
    fn(prefix, obj)
  }
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

    // channel and root "keys" are contained in links

    var links = messages.map(x => extractLinks(x.value)) 

    console.log(`extracted data ${datediff(new Date(), start)}`)
   
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

    var listChild = new arrow.Field('list[Utf8]', new arrow.Utf8())
    
    var all = arrow.Table.new([
      arrow.DateVector.from(dates),
      arrow.Int32Vector.from(flumeseqs),
      arrow.Utf8Vector.from(keys),
      arrow.Vector.from({ values: authors, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
      arrow.Int32Vector.from(sequences),
      arrow.Vector.from({ values: types, type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
      arrow.Vector.from({ values: links, type: new arrow.List(listChild) }),
    ], ["date", "flumeseq", "key", "author", "sequence", "type", "links"])
    
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
    
    var q2 = Array.from(
      all.filter(
        arrow.predicate.col('links').eq('%GcvjVk+NLsjOB5Vd+vceGXEOeYmHoRA6lgXVJzPuMxw=.sha256')))

    console.log(q2)
    
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
