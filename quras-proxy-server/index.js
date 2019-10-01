//
// Create the proxy server listening on port 9009
//
var httpProxy = require('http-proxy')
var fs = require('fs')

httpProxy.createServer({
  target: {
      host: '13.230.62.42',
      port: 3001
  },
  ssl: {
    key: fs.readFileSync('./ssl/quras.key', 'utf8'),
    cert: fs.readFileSync('./ssl/quras.crt', 'utf8')
  }
}).listen(9009);