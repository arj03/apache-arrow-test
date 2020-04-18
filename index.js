const pull = require('pull-stream')
const arrow = require('apache-arrow')
const OffsetLog = require('flumelog-offset')
const codec = require('flumecodec/json')
const Flume = require('flumedb')

var log = OffsetLog(
  '/home/chrx/.ssb/flume/log.offset',
  {blockSize:1024*64, codec:codec}
)

var db = Flume(log, true)

var start = new Date()

function datediff(date1, date2) {
  return date1.getTime() - date2.getTime();
}

const ref = require('ssb-ref')
const matchChannel = /^#[^\s#]+$/

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
  var links = new Set()
  walk(msg.value.content, function (path, value) {
    if (Array.isArray(path) && path[0] === 'channel') { // HACK: handle legacy channel mentions
      var channel = ref.normalizeChannel(value)
      if (channel)
        value = `#${channel}`
    }

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
  pull.take(10000),
  pull.collect((err, messages) => {
    console.log(`extract ${messages.length} messages, ${datediff(new Date(), start)}ms`)

    var flumeseqs = messages.map(x => x.seq)

    var keys = messages.map(x => x.value.key)
    var dates = messages.map(x => new Date(x.value.timestamp))
    var authors = messages.map(x => x.value.value.author)
    var sequences = messages.map(x => x.value.value.sequence)
    var types = messages.map(x => x.value.value.content.type)

    // channel and root "keys" are contained in links

    var links = messages.map(x => extractLinks(x.value))
    console.log(links.slice(9997, 9999))

    console.log(`extracted data ${datediff(new Date(), start)}ms`)
   
    // indexes:
    // clock: [data.value.author, data.value.sequence] => flume seq
    // keys:  data.key => flume seq
    // ssb-query: general query, men med det vi har skulle det virke, mangler måske root?

    // backlinks: find alt som peger på noget (dest), #hashtags, @mention, %message, &blob. Dvs. det bliver en liste per besked

    // last:  data.value.author => reduced { data.key, data.value.sequence, data.value.timestamp }
    // peer-invites: value.dest => flume seq + reduced invite state
    // contacts2 (ssb-friends): reduced follow state

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

    console.log(`imported data into arrow ${keys.length} keys, ${datediff(new Date(), start)}ms`)

    var queryStart = new Date()

    var today2 = Array.from(
      all.filter(
	//arrow.predicate.col('date').gt(new Date(2020, 0, 30)).and(
	  arrow.predicate.col('author').eq("@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519").and(
	    arrow.predicate.col('type').eq("post"))
      )
    )

    console.log(`query returned ${today2.length} results, took ${datediff(new Date(), queryStart)}ms`)
    //console.log(today2)

    var queryStart2 = new Date()

    const link = '%TbiUknx1XfDdxccudfDds54hfzVNCjh2N99KXtLuyVc=.sha256'
    const uLink = new TextEncoder("utf-8").encode(link);
    
    var q2 = Array.from(
      all.filter(
        arrow.predicate.custom(
          (idx) => {
            var l = get_links(idx)
            if (l.length > 0) {
              var s = l.values.slice(l.valueOffsets[0], l.valueOffsets[l.valueOffsets.length-1])
              var findIndex = 0
              for (var i = 0; i < s.length; ++i) {
                if (s[i] == uLink[findIndex])
                {
                  if (++findIndex == uLink.length)
                    return true
                } else
                  findIndex = 0
              }
              return false
            } else
              return false
          },
          (batch) => {
            get_links = arrow.predicate.col('links').bind(batch)
          })
      )
    )

    console.log(`query returned ${q2.length} results, took ${datediff(new Date(), queryStart2)}ms`)
    console.log(q2)
    
    return

    var w = arrow.RecordBatchStreamWriter.writeAll(all)
    const fs = require('fs')
    w.toUint8Array().then((data) => {
      fs.writeFileSync("index.arrow", data)
    })
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
