// Connects to the postgres the isolated script run provisioned. DB_PORT is
// mapped in via the script's environment — nothing is exported implicitly.
const net = require('net');
const port = process.env.DB_PORT;
if (!port) {
  console.error('DB_PORT not set — script environment mapping is broken');
  process.exit(1);
}
const s = net.connect({ host: '127.0.0.1', port }, () => {
  console.log(`connected to postgres on port ${port}`);
  s.end();
  process.exit(0);
});
s.on('error', (e) => {
  console.error('failed:', e.message);
  process.exit(1);
});
setTimeout(() => {
  console.error('timeout connecting to postgres');
  process.exit(1);
}, 10000);
