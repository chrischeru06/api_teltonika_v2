/**
 * Teltonika GPS Tracker Server - Version corrigée
 * Auteur: Cerubala Christian Wann'y
 */
const net = require('net');
const Parser = require('teltonika-parser-ex');
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const winston = require('winston');

const TCP_PORT = 2354;
const TCP_TIMEOUT = 300000;
const IMEI_FOLDER_BASE = '/var/www/html/IMEI';
const MAX_GEOJSON_SIZE = 100 * 1024 * 1024; // 100MB

if (!fs.existsSync(IMEI_FOLDER_BASE)) {
  fs.mkdirSync(IMEI_FOLDER_BASE, { recursive: true });
}

const deviceState = new Map();

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'server.log' })
  ]
});

const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'Chris@1996..',
  database: 'car_trucking_v3',
  waitForConnections: true,
  connectionLimit: 50,
  queueLimit: 0,
};

let db;
async function initDbPool() {
  try {
    db = await mysql.createPool(dbConfig);
    logger.info('MySQL pool created');
  } catch (err) {
    logger.error('MySQL pool creation failed:', err.message);
    setTimeout(initDbPool, 5000);
  }
}
initDbPool();

function toMysqlDatetime(isoDate) {
  return isoDate.replace('T', ' ').replace('Z', '').split('.')[0];
}

function isValidGps(gps) {
  return gps && gps.latitude !== 0 && gps.longitude !== 0 && Math.abs(gps.latitude) <= 90 && Math.abs(gps.longitude) <= 180;
}

async function insertTrackingData(values) {
  const query = `INSERT INTO tracking_data (
    latitude, longitude, vitesse, altitude, date,
    angle, satellites, mouvement, gnss_statut,
    device_uid, ignition
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  try {
    await db.execute(query, values);
  } catch (err) {
    logger.error('Insert Error:', err.message);
  }
}

async function logLatestDeviceData() {
  try {
    const [rows] = await db.query(`
      SELECT t1.*
      FROM tracking_data t1
      JOIN (
        SELECT device_uid, MAX(date) AS max_date
        FROM tracking_data
        GROUP BY device_uid
      ) t2 ON t1.device_uid = t2.device_uid AND t1.date = t2.max_date
    `);

    const result = rows.map(row => ({
      imei: row.device_uid,
      latitude: parseFloat(row.latitude),
      longitude: parseFloat(row.longitude),
      speed: parseFloat(row.vitesse),
      altitude: parseFloat(row.altitude),
      angle: parseFloat(row.angle),
      satellites: parseInt(row.satellites),
      timestamp: row.date,
      ignition: row.ignition,
      movement: row.mouvement,
      gnss_status: row.gnss_statut
    }));

    console.log(JSON.stringify(result, null, 2));
  } catch (err) {
    logger.error('Error fetching latest device data:', err.message);
  }
}

const tcpServer = net.createServer(socket => {
  logger.info('TCP client connected');
  let imei = null;
  socket.setTimeout(TCP_TIMEOUT);

  socket.on('timeout', () => socket.end());
  socket.on('end', () => imei && deviceState.delete(imei));
  socket.on('error', err => {
    logger.error('Socket error:', err);
    imei && deviceState.delete(imei);
  });

  socket.on('data', async data => {
    try {
      const parser = new Parser(data);

      if (parser.isImei) {
        imei = parser.imei;
        socket.write(Buffer.from([0x01]));

        if (!deviceState.has(imei)) {
          deviceState.set(imei, { lastIgnition: null });
          const folder = path.join(IMEI_FOLDER_BASE, imei);
          if (!fs.existsSync(folder)) fs.mkdirSync(folder, { recursive: true });
        }
        return;
      }

      const avl = parser.getAvl();
      if (!avl?.records?.length) return;

      const state = deviceState.get(imei);

      console.log(avl.records);
      

      for (const record of avl.records) {
        const { gps, timestamp, ioElements } = record;
        if (!isValidGps(gps)) continue;

        const io = {
          ignition: ioElements.find(e => e.label === 'Ignition')?.value || 0,
          mouvement: ioElements.find(e => e.label === 'Movement')?.value || 0,
          gnss_statut: ioElements.find(e => e.label === 'GNSS Status')?.value || 1,
        };

        const timestampIso = toMysqlDatetime(new Date(timestamp).toISOString());

        if (io.ignition === 1) {
          const values = [
            gps.latitude, gps.longitude, gps.speed || 0, gps.altitude, timestampIso,
            gps.angle, gps.satellites, io.mouvement, io.gnss_statut, imei, io.ignition
          ];
          await insertTrackingData(values);

          if (!state.trip) {
            state.trip = { startTime: timestampIso, points: [] };
          }

          state.trip.points.push({
            geometry: { type: "Point", coordinates: [gps.longitude, gps.latitude] },
            properties: {
              timestamp: timestampIso,
              speed: gps.speed,
              altitude: gps.altitude,
              angle: gps.angle,
              satellites: gps.satellites
            }
          });
        }

        const ignitionChanged = state.lastIgnition !== null && state.lastIgnition === 1 && io.ignition === 0;
        state.lastIgnition = io.ignition;

        if (ignitionChanged && state.trip) {
          const folder = path.join(IMEI_FOLDER_BASE, imei);
          const dateName = new Date().toISOString().replace(/[:.]/g, '-');
          const filename = `trip_${dateName}_linestring.geojson`;
          const filepath = path.join(folder, filename);

          const geojson = {
            type: "FeatureCollection",
            features: [
              {
                type: "Feature",
                geometry: {
                  type: "LineString",
                  coordinates: state.trip.points.map(p => p.geometry.coordinates)
                },
                properties: {
                  imei,
                  startTime: state.trip.startTime,
                  endTime: timestampIso,
                  totalPoints: state.trip.points.length
                }
              }
            ]
          };

          const geojsonStr = JSON.stringify(geojson, null, 2);

          if (Buffer.byteLength(geojsonStr) > MAX_GEOJSON_SIZE) {
            logger.warn(`GeoJSON file too large for IMEI ${imei}`);
            // Handle split logic if desired
          }

          fs.writeFileSync(filepath, geojsonStr);
          logger.info(`Trip saved: ${filepath}`);

          await db.execute(
            `INSERT INTO path_histo_trajet_geojson (DEVICE_UID, TRIP_START, TRIP_END, PATH_FILE)
             VALUES (?, ?, ?, ?)`,
            [imei, state.trip.startTime, timestampIso, filepath]
          );

          await db.execute('DELETE FROM tracking_data WHERE device_uid = ?', [imei]);
          delete state.trip;
        }
      }

      await logLatestDeviceData();

    } catch (err) {
      logger.error(`Processing error for IMEI ${imei}: ${err.message}`);
    }
  });
});

tcpServer.listen(TCP_PORT, () => {
  logger.info(`TCP Server running on port ${TCP_PORT}`);
});
