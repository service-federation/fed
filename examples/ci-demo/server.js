const http = require('http');
const port = process.env.PORT;
http
  .createServer((req, res) => res.end('ok'))
  .listen(port, () => console.log(`listening on ${port}`));
