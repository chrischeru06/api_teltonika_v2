const net = require('net');
const Parser = require('teltonika-parser-ex');
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const winston = require('winston');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const TCP_PORT = 2354;
const HTTP_PORT = 8000;
const TCP_TIMEOUT = 300000;
const IMEI_FOLDER_BASE = '/var/www/html/IMEI';
const MAX_GEOJSON_SIZE = 100 * 1024 * 1024;

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

// === Base de données ===
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
  return gps && gps.latitude !== 0 && gps.longitude !== 0 &&
    Math.abs(gps.latitude) <= 90 && Math.abs(gps.longitude) <= 180;
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

// === SOCKET.IO ===
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});


// HTTP

app.get('/api/last-trajets', async (req, res) => {
  try {
    const [rows] = await db.execute(`
      SELECT DEVICE_UID, TRIP_START, TRIP_END, PATH_FILE, LATITUDE, LONGITUDE
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY DEVICE_UID ORDER BY TRIP_END DESC) AS rn
        FROM path_histo_trajet_geojson
      ) AS t
      WHERE t.rn = 1
    `);
    res.status(200).json(rows);
  } catch (err) {
    logger.error('Erreur récupération dernier trajet par appareil:', err.message);
    res.status(500).json({ message: 'Erreur serveur' });
  }
});


io.on('connection', socket => {
  logger.info('Socket.IO client connected');

  // Gestion des abonnements par IMEI
  socket.on('subscribe', imei => {
    socket.join(imei);
    logger.info(`Client subscribed to IMEI: ${imei}`);
  });

  socket.on('unsubscribe', imei => {
    socket.leave(imei);
    logger.info(`Client unsubscribed from IMEI: ${imei}`);
  });
});

// === TCP Server ===
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

      for (const record of avl.records) {
        const { gps, timestamp, ioElements } = record;
        if (!isValidGps(gps)) continue;

        const ioData = {
          ignition: ioElements.find(e => e.label === 'Ignition')?.value || 0,
          mouvement: ioElements.find(e => e.label === 'Movement')?.value || 0,
          gnss_statut: ioElements.find(e => e.label === 'GNSS Status')?.value || 1,
        };

        const timestampIso = toMysqlDatetime(new Date(timestamp).toISOString());

        const values = [
          gps.latitude, gps.longitude, gps.speed || 0, gps.altitude, timestampIso,
          gps.angle, gps.satellites, ioData.mouvement, ioData.gnss_statut, imei, ioData.ignition
        ];

        // 🔴 INSERT EN BDD
        await insertTrackingData(values);

        // 🟢 EMETTRE AUX CLIENTS ABONNÉS À CET IMEI
        io.to(imei).emit('tracking_data', {
          imei,
          latitude: gps.latitude,
          longitude: gps.longitude,
          speed: gps.speed,
          altitude: gps.altitude,
          timestamp: timestampIso,
          angle: gps.angle,
          satellites: gps.satellites,
          ignition: ioData.ignition,
          movement: ioData.mouvement,
          gnss_status: ioData.gnss_statut
        });

        // Trajet en cours
        if (ioData.ignition === 1) {
          if (!state.trip) {
            state.trip = { startTime: timestampIso, points: [] };
          }

          // Ajout du point avec les coordonnées 3D
          state.trip.points.push({
            geometry: { 
              type: "Point", 
              coordinates: [gps.longitude, gps.latitude, gps.altitude] 
            },
            properties: {
              timestamp: timestampIso,
              speed: gps.speed,
              angle: gps.angle,
              satellites: gps.satellites
            }
          });
        }

        // Fin de trajet
        const ignitionChanged = state.lastIgnition !== null && state.lastIgnition === 1 && ioData.ignition === 0;
        state.lastIgnition = ioData.ignition;

        if (ignitionChanged && state.trip) {
          const folder = path.join(IMEI_FOLDER_BASE, imei);
          const dateName = new Date().toISOString().replace(/[:.]/g, '-');
          const filename = `trip_${dateName}_linestring.geojson`;
          const filepath = path.join(folder, filename);


          // Création du GeoJSON avec les coordonnées 3D
          const geojson = {
                        type: "FeatureCollection",
                        features: state.trip.points.map(p => ({
                            type: "Feature",
                            geometry: {
                            type: "Point",
                            coordinates: p.geometry.coordinates
                            },
                            properties: {
                            imei,
                            timestamp: p.timestamp,
                            speed: p.speed || null
                            }
                        }))};

          const geojsonStr = JSON.stringify(geojson, null, 2);

          if (Buffer.byteLength(geojsonStr) <= MAX_GEOJSON_SIZE) {
            fs.writeFileSync(filepath, geojsonStr);
            logger.info(`Trip saved: ${filepath}`);
            await db.execute(
              `INSERT INTO path_histo_trajet_geojson (DEVICE_UID, TRIP_START, TRIP_END, PATH_FILE,LATITUDE,LONGITUDE)
               VALUES (?, ?, ?, ?)`,
              [imei, state.trip.startTime, timestampIso, filepath, gps.latitude, gps.longitude]
            );
            await db.execute('DELETE FROM tracking_data WHERE device_uid = ?', [imei]);
          } else {
            logger.warn(`GeoJSON file too large for IMEI ${imei}`);
          }

          delete state.trip;
        }
      }
    } catch (err) {
      logger.error(`Processing error for IMEI ${imei}: ${err.message}`);
    }
  });
});

// === Lancement des serveurs ===
tcpServer.listen(TCP_PORT, () => {
  logger.info(`TCP Server running on port ${TCP_PORT}`);
});

server.listen(HTTP_PORT, () => {
  logger.info(`HTTP/WebSocket Server running on port ${HTTP_PORT}`);
});

