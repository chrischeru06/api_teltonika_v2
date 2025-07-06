const net = require('net');
const Parser = require('teltonika-parser-ex');
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const winston = require('winston');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const TCP_PORT = 2354;
const HTTP_PORT = 8000;
const TCP_TIMEOUT = 300000;
const IMEI_FOLDER_BASE = '/var/www/html/api_teltonika/IMEI';
const MAX_GEOJSON_SIZE = 100 * 1024 * 1024;
const IMEI_REGEX = /^\d{15}$/;

// Constantes pour la nouvelle logique de trajets
const MIN_WAIT_BEFORE_TRIP_END = 60000; // 1 minute en millisecondes

if (!fs.existsSync(IMEI_FOLDER_BASE)) {
  fs.mkdirSync(IMEI_FOLDER_BASE, { recursive: true, mode: 0o755 });
}

const deviceState = new Map();

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
  transports: [new winston.transports.Console(), new winston.transports.File({ filename: 'server.log' })]
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

// === Helpers ===
function toMysqlDatetime(isoDate) {
  return isoDate.replace('T', ' ').replace('Z', '').split('.')[0];
}
function isValidGps(gps) {
  return gps && gps.latitude !== 0 && gps.longitude !== 0 && Math.abs(gps.latitude) <= 90 && Math.abs(gps.longitude) <= 180;
}
function isValidImei(imei) {
  return IMEI_REGEX.test(imei);
}
function getImeiFolder(imei) {
  if (!isValidImei(imei)) {
    throw new Error(`Invalid IMEI format: ${imei}`);
  }
  const folder = path.join(IMEI_FOLDER_BASE, imei);
  if (!path.resolve(folder).startsWith(path.resolve(IMEI_FOLDER_BASE))) {
    throw new Error(`Invalid folder path for IMEI: ${imei}`);
  }
  return folder;
}
async function createImeiFolder(imei) {
  const folder = getImeiFolder(imei);
  if (!fs.existsSync(folder)) {
    fs.mkdirSync(folder, { recursive: true, mode: 0o755 });
    logger.info(`Created folder for IMEI: ${imei}`);
  }
  return folder;
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

// Finalize Trip - VERSION AMÉLIORÉE
async function finalizeTrip(imei, state, timestampIso, gps) {
  try {
    const folder = await createImeiFolder(imei);
    const dateName = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `trip_${dateName}_linestring.geojson`;
    const filepath = path.join(folder, filename);
    
    if (!path.resolve(filepath).startsWith(path.resolve(folder))) {
      throw new Error(`Invalid trip path for ${imei}`);
    }

    // Trier les points du trajet par timestamp
    const sortedPoints = state.trip.points.sort((a, b) => {
      return new Date(a.properties.timestamp) - new Date(b.properties.timestamp);
    });

    // Extraire les coordonnées pour la LineString
    const coordinates = sortedPoints.map(p => p.geometry.coordinates);

    // Créer la feature LineString
    const lineFeature = {
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: coordinates
      },
      properties: { 
        layer: "path",
        trip_start: state.trip.startTime,
        trip_end: timestampIso,
        total_points: sortedPoints.length,
        device_uid: imei
      }
    };

    // Ajouter la propriété layer aux points
    const pointFeatures = sortedPoints.map(p => ({
      type: "Feature",
      geometry: p.geometry,
      properties: { 
        ...p.properties, 
        layer: "point",
        device_uid: imei
      }
    }));

    // Combiner dans une FeatureCollection
    const geojson = {
      type: "FeatureCollection",
      features: [...pointFeatures, lineFeature]
    };

    const geojsonStr = JSON.stringify(geojson, null, 2);
    
    if (Buffer.byteLength(geojsonStr) <= MAX_GEOJSON_SIZE) {
      fs.writeFileSync(filepath, geojsonStr, { mode: 0o644 });
      logger.info(`Trip saved: ${filepath} (${state.trip.points.length} points + 1 linestring)`);
      
      await db.execute(
        `INSERT INTO path_histo_trajet_geojson (DEVICE_UID, TRIP_START, TRIP_END, PATH_FILE, LATITUDE, LONGITUDE) VALUES (?, ?, ?, ?, ?, ?)`,
        [imei, state.trip.startTime, timestampIso, filepath, gps.latitude, gps.longitude]
      );
      
      await db.execute(`DELETE FROM tracking_data WHERE device_uid = ?`, [imei]);
    } else {
      logger.warn(`GeoJSON too large for ${imei}`);
    }
  } catch (err) {
    logger.error(`Error finalizing trip for ${imei}: ${err.message}`);
  }
}

// === Web Server Setup ===
const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });
app.use('/media', express.static(IMEI_FOLDER_BASE));

// Routes
app.get('/api/get_historiques_trajets/', async (req, res) => {
  const { device_uid } = req.query;

  if (!device_uid || !isValidImei(device_uid)) {
    return res.status(400).json({ message: 'Valid DEVICE_UID (15 digits) required' });
  }
  try {
    const [rows] = await db.execute(
      `SELECT * FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? ORDER BY TRIP_START DESC`,
      [device_uid]
    );
    if (rows.length === 0) {
      return res.status(404).json({ message: 'No trips found for this DEVICE_UID' });
    }
    res.json(rows);
  } catch (error) {
    logger.error('Error fetching trips:', error);
    res.status(500).json({ message: 'Server error' });
  }
});

// Nouvelle API pour récupérer l'historique des trajets
app.get('/api/trips_history', async (req, res) => {
  try {
    const { device_uid, start_date, end_date, limit } = req.query;
    
    let query = `
      SELECT 
        ID, 
        DEVICE_UID, 
        TRIP_START, 
        TRIP_END, 
        PATH_FILE, 
        LATITUDE, 
        LONGITUDE
      FROM path_histo_trajet_geojson
    `;
    
    const params = [];
    const conditions = [];
    
    if (device_uid) {
      if (!isValidImei(device_uid)) {
        return res.status(400).json({ error: 'Invalid DEVICE_UID format' });
      }
      conditions.push('DEVICE_UID = ?');
      params.push(device_uid);
    }
    
    if (start_date) {
      conditions.push('TRIP_START >= ?');
      params.push(start_date);
    }
    
    if (end_date) {
      conditions.push('TRIP_END <= ?');
      params.push(end_date);
    }
    
    if (conditions.length > 0) {
      query += ' WHERE ' + conditions.join(' AND ');
    }
    
    query += ' ORDER BY TRIP_END DESC';
    
    if (limit) {
      const parsedLimit = parseInt(limit);
      if (!isNaN(parsedLimit) && parsedLimit > 0) {
        query += ' LIMIT ?';
        params.push(parsedLimit);
      }
    }
    
    const [rows] = await db.execute(query, params);
    
    const results = rows.map(row => {
      const filename = path.basename(row.PATH_FILE);
      return {
        id: row.ID,
        device_uid: row.DEVICE_UID,
        trip_start: row.TRIP_START,
        trip_end: row.TRIP_END,
        path_file: row.PATH_FILE,
        latitude: parseFloat(row.LATITUDE),
        longitude: parseFloat(row.LONGITUDE),
        geojson_url: `/api/geojson/${row.DEVICE_UID}/${filename}`,
        geojson_full_url: `http://31.97.54.87:${HTTP_PORT}/api/geojson/${row.DEVICE_UID}/${filename}`
      };
    });
    
    res.json({
      success: true,
      count: results.length,
      data: results
    });
    
  } catch (error) {
    logger.error('Error in /api/trips_history:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      details: error.message
    });
  }
});
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
    res.json(rows);
  } catch (err) {
    logger.error('Error fetching last trips:', err.message);
    res.status(500).json({ message: 'Server error' });
  }
});

// Nouvelle API pour les temps de course et repos
app.get('/api/get_trip_data/', async (req, res) => {
  try {
    const { device_uid } = req.query;

    if (!device_uid || !isValidImei(device_uid)) {
      return res.status(400).json({ message: 'Valid DEVICE_UID (15 digits) required' });
    }

    const [trips] = await db.execute(
      `SELECT TRIP_START, TRIP_END, LATITUDE, LONGITUDE 
       FROM path_histo_trajet_geojson 
       WHERE DEVICE_UID = ? 
       ORDER BY TRIP_END DESC`,
      [device_uid]
    );

    if (trips.length === 0) {
      return res.status(404).json({ message: 'No trips found for this device' });
    }

    const tripData = trips.map((trip, index) => {
      const startTime = new Date(trip.TRIP_START);
      const endTime = new Date(trip.TRIP_END);
      
      const tripDuration = (endTime - startTime) / (1000 * 60);
      
      let restDuration = 0;
      if (index < trips.length - 1) {
        const nextTripStart = new Date(trips[index + 1].TRIP_START);
        restDuration = (nextTripStart - endTime) / (1000 * 60);
      }

      // Conversion explicite en nombres
      const endLat = parseFloat(trip.LATITUDE);
      const endLng = parseFloat(trip.LONGITUDE);

      return {
        trip_start: trip.TRIP_START,
        trip_end: trip.TRIP_END,
        trip_duration_minutes: tripDuration.toFixed(2),
        rest_duration_minutes: restDuration.toFixed(2),
        end_latitude: isNaN(endLat) ? null : endLat,
        end_longitude: isNaN(endLng) ? null : endLng
      };
    });

    res.json(tripData);
  } catch (error) {
    logger.error('Error fetching trip data:', error);
    res.status(500).json({ message: 'Server error' });
  }
});

// Fonction courseCard
async function courseCard(req, res) {
  try {
    const { device_uid } = req.query;

    if (!device_uid || !isValidImei(device_uid)) {
      return res.status(400).json({ message: 'Valid DEVICE_UID (15 digits) required' });
    }

    // Récupérer les données de l'API interne
    const response = await fetch(`http://31.97.54.87/:${HTTP_PORT}/api/get_trip_data/?device_uid=${device_uid}`);
    if (!response.ok) {
      throw new Error(`API request failed with status ${response.status}`);
    }

    const tripData = await response.json();

    // Formater les données pour la réponse
    const formattedData = tripData.map(trip => ({
      start_time: trip.trip_start,
      end_time: trip.trip_end,
      duration: `${trip.trip_duration_minutes} minutes`,
      rest_time: trip.rest_duration_minutes > 0 ? 
                `${trip.rest_duration_minutes} minutes` : 
                'N/A (last trip)',
      end_position: {
        latitude: trip.end_latitude,
        longitude: trip.end_longitude
      }
    }));

    res.json({
      status: 'success',
      device_uid,
      trip_count: formattedData.length,
      trips: formattedData
    });
  } catch (error) {
    logger.error('Error in courseCard:', error);
    res.status(500).json({ 
      status: 'error',
      message: error.message 
    });
  }
}

// Ajouter la route pour courseCard
app.get('/api/course_card', courseCard);

// Socket.IO
io.on('connection', socket => {
  logger.info('Socket.IO client connected');

  socket.on('subscribe', imei => {
    if (!isValidImei(imei)) {
      logger.warn(`Invalid IMEI subscription attempt: ${imei}`);
      return;
    }
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
  socket.on('timeout', () => {
    logger.info(`Socket timeout for IMEI: ${imei}`);
    socket.end();
  });
  socket.on('end', () => {
    if (imei) {
      deviceState.delete(imei);
      logger.info(`Connection ended for IMEI: ${imei}`);
    }
  });
  socket.on('error', err => {
    logger.error(`Socket error for IMEI ${imei}: ${err.message}`);
    if (imei) deviceState.delete(imei);
  });
  socket.on('data', async data => {
    try {
      const parser = new Parser(data);

      if (parser.isImei) {
        imei = parser.imei;

        if (!isValidImei(imei)) {
          logger.error(`Invalid IMEI received: ${imei}`);
          socket.end();
          return;
        }
        socket.write(Buffer.from([0x01]));

        if (!deviceState.has(imei)) {
          deviceState.set(imei, { lastIgnition: 0 });
          await createImeiFolder(imei);
        }
        return;
      }

      if (!imei) {
        logger.warn('Received data before IMEI');
        return;
      }

      const avl = parser.getAvl();
      if (!avl?.records?.length) return;

      const state = deviceState.get(imei);
      if (!state) {
        logger.warn(`No device state found for IMEI ${imei}, skipping processing`);
        return;
      }

      for (const record of avl.records) {
        const { gps, timestamp, ioElements } = record;
        if (!isValidGps(gps)) continue;

        const ioData = {
          ignition: ioElements.find(e => e.label === 'Ignition')?.value || 0,
          mouvement: ioElements.find(e => e.label === 'Movement')?.value || 0,
          gnss_statut: ioElements.find(e => e.label === 'GNSS Status')?.value || 1,
        };
        const timestampIso = toMysqlDatetime(new Date(timestamp).toISOString());
        const currentTime = new Date(timestamp);

        // Émettre les données en temps réel
        io.emit('tracking_data', {
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

        // === NOUVELLE LOGIQUE DE TRAJETS ===

        // 1. Début du trajet (ignition de 0 à 1)
        const ignitionChangedToOn = state.lastIgnition === 0 && ioData.ignition === 1;

        if (ignitionChangedToOn) {
          logger.info(`Ignition ON detected for ${imei} - Starting new trip`);

          // Créer un nouveau trajet
          state.trip = {
            startTime: timestampIso,
            points: [],
            ignitionOffTime: null,
            hasMovement: false,
            firstPointInserted: false,
            tripStartTimestamp: currentTime
          };

          // Enregistrer le premier point immédiatement
          const firstPoint = {
            geometry: { type: "Point", coordinates: [gps.longitude, gps.latitude] },
            properties: {
              timestamp: timestampIso,
              speed: gps.speed ?? 0,
              angle: gps.angle ?? 0,
              satellites: gps.satellites ?? 0
            }
          };

          state.trip.points.push(firstPoint);
          state.trip.firstPointInserted = true;

          // Insérer le premier point en base
          await insertTrackingData([
            gps.latitude, gps.longitude, gps.speed || 0, gps.altitude, timestampIso,
            gps.angle, gps.satellites, ioData.mouvement, ioData.gnss_statut,
            imei, ioData.ignition
          ]);

          logger.info(`First point inserted for trip ${imei}: speed=${gps.speed || 0}`);
        }

        // 2. Trajet en cours avec ignition ON
        if (ioData.ignition === 1 && state.trip && state.trip.firstPointInserted) {
          // Ajouter des points seulement si la vitesse > 0
          if (gps.speed > 0) {
            if (!state.trip.hasMovement) {
              state.trip.hasMovement = true;
              logger.info(`Movement detected for ${imei} - Trip is now valid`);
            }

            // Ajouter le point au trajet
            state.trip.points.push({
              geometry: { type: "Point", coordinates: [gps.longitude, gps.latitude] },
              properties: {
                timestamp: timestampIso,
                speed: gps.speed ?? 0,
                angle: gps.angle ?? 0,
                satellites: gps.satellites ?? 0
              }
            });

            // Insérer en base
            await insertTrackingData([
              gps.latitude, gps.longitude, gps.speed || 0, gps.altitude, timestampIso,
              gps.angle, gps.satellites, ioData.mouvement, ioData.gnss_statut,
              imei, ioData.ignition
            ]);
          }
        }

        // 3. Détection de l'arrêt de l'ignition (de 1 à 0)
        const ignitionChangedToOff = state.lastIgnition === 1 && ioData.ignition === 0;

        if (ignitionChangedToOff && state.trip) {
          state.trip.ignitionOffTime = currentTime;
          logger.info(`Ignition OFF detected for ${imei} - Starting countdown`);
        }

        // 4. Vérification pour finaliser le trajet
        if (state.trip && state.trip.ignitionOffTime && ioData.ignition === 0) {
          const timeSinceIgnitionOff = currentTime - state.trip.ignitionOffTime;
          const timeSinceTripStart = currentTime - state.trip.tripStartTimestamp;

          // Attendre au moins 1 minute avant de finaliser
          if (timeSinceIgnitionOff >= MIN_WAIT_BEFORE_TRIP_END) {
            // Vérifier si le trajet est valide (a eu du mouvement)
            if (state.trip.hasMovement && timeSinceTripStart >= MIN_WAIT_BEFORE_TRIP_END) {
              logger.info(`Finalizing valid trip for ${imei}: ${state.trip.points.length} points, duration: ${Math.round(timeSinceTripStart/1000)}s`);
              await finalizeTrip(imei, state, timestampIso, gps);
            } else {
              if (!state.trip.hasMovement) {
                logger.warn(`Trip ignored for ${imei}: no movement detected (speed never > 0)`);
              } else {
                logger.warn(`Trip ignored for ${imei}: trip duration too short (${Math.round(timeSinceTripStart/1000)}s < 60s)`);
              }
            }

            // Supprimer le trajet dans tous les cas
            delete state.trip;
          }
        }

        // Mettre à jour l'état de l'ignition
        state.lastIgnition = ioData.ignition;
      }
    } catch (err) {
      logger.error(`Processing error for IMEI ${imei}: ${err.message}`);
    }
  });
});

// === Startup ===
tcpServer.listen(TCP_PORT, () => {
  logger.info(`TCP Server running on port ${TCP_PORT}`);
});
server.listen(HTTP_PORT, () => {
  logger.info(`HTTP/WebSocket Server running on port ${HTTP_PORT}`);
});
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
});