// batch-correct.js

const mysql = require('mysql2/promise');
const winston = require('winston');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

// Configuration DB identique à ton backend
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'Chris@1996..',
  database: 'car_trucking_v3',
  waitForConnections: true,
  connectionLimit: 20,
  queueLimit: 0,
};

const IMEI_FOLDER_BASE = '/var/www/html/api_teltonika/IMEI';
const MAPBOX_TOKEN = 'pk.eyJ1IjoibWFydGlubW4iLCJhIjoiY2xkZzNna2N2MG5sODN0bXN6N25icW9qdyJ9.LmRRBi8cDi2qYzn2PbEoZg';

// Logger simple console
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(winston.format.timestamp(), winston.format.simple()),
  transports: [new winston.transports.Console()]
});

// Fonctions utiles à copier depuis ton backend (extrait)
function isValidImei(imei) {
  return /^\d{15}$/.test(imei);
}

function getImeiFolder(imei) {
  if (!isValidImei(imei)) throw new Error(`Invalid IMEI: ${imei}`);
  const folder = path.join(IMEI_FOLDER_BASE, imei);
  if (!path.resolve(folder).startsWith(path.resolve(IMEI_FOLDER_BASE))) {
    throw new Error(`Invalid folder path for IMEI: ${imei}`);
  }
  return folder;
}

async function mapboxMatchCoordinates(coords) {
  if (!coords.length || coords.length < 2) return null;
  const maxPoints = 100;
  const step = Math.max(1, Math.floor(coords.length / maxPoints));
  const sampled = coords.filter((_, i) => i % step === 0);
  if (sampled[0] !== coords[0]) sampled.unshift(coords[0]);
  if (sampled[sampled.length - 1] !== coords[coords.length - 1]) sampled.push(coords[coords.length - 1]);
  const coordStr = sampled.map(c => `${c[0]},${c[1]}`).join(';');

  const url = `https://api.mapbox.com/matching/v5/mapbox/driving/${coordStr}?access_token=${MAPBOX_TOKEN}&geometries=geojson&overview=full`;
  try {
    const resp = await axios.get(url, { timeout: 30000 });
    if (resp.data && resp.data.matchings && resp.data.matchings.length) {
      return resp.data.matchings[0].geometry;
    }
  } catch (err) {
    logger.error(`Mapbox match error: ${err.message}`);
  }
  return null;
}

async function correctTrajectory(db, device_uid, trip_id, path_file) {
  try {
    if (!fs.existsSync(path_file)) {
      throw new Error(`File not found: ${path_file}`);
    }
    const rawData = JSON.parse(fs.readFileSync(path_file, 'utf8'));
    if (!rawData.features || rawData.features.length === 0) {
      throw new Error('Raw GeoJSON has no features');
    }

    let coords = [];
    if (rawData.type === 'FeatureCollection') {
      for (const f of rawData.features) {
        if (f.geometry?.type === 'Point') {
          coords.push(f.geometry.coordinates);
        } else if (f.geometry?.type === 'LineString') {
          coords = coords.concat(f.geometry.coordinates);
        }
      }
    } else if (rawData.type === 'Feature' && rawData.geometry?.type === 'LineString') {
      coords = rawData.geometry.coordinates;
    }

    if (coords.length < 2) {
      throw new Error('Not enough coordinates for matching');
    }

    const correctedGeometry = await mapboxMatchCoordinates(coords);
    if (!correctedGeometry) {
      throw new Error('Mapbox matching failed');
    }

    const correctedGeoJSON = {
      type: 'Feature',
      properties: {
        device_uid,
        original_trip_id: trip_id,
        corrected_at: new Date().toISOString(),
        source: 'mapbox'
      },
      geometry: correctedGeometry
    };

    const folder = getImeiFolder(device_uid);
    const correctedFilename = `corrected_${Date.now()}.geojson`;
    const correctedFilePath = path.join(folder, correctedFilename);
    fs.writeFileSync(correctedFilePath, JSON.stringify(correctedGeoJSON, null, 2), { mode: 0o644 });

    await db.execute(
      'UPDATE path_histo_trajet_geojson SET PATH_FILE = ?, IS_CORRECTED = 1, UPDATED_AT = NOW() WHERE ID = ?',
      [correctedFilePath, trip_id]
    );

    logger.info(`Trip ${trip_id} corrected for device ${device_uid}`);
    return true;
  } catch (err) {
    logger.error(`Error correcting trip ${trip_id}: ${err.message}`);
    return false;
  }
}

(async () => {
  try {
    const db = await mysql.createPool(dbConfig);
    logger.info('DB pool created');

    // Récupérer tous trajets non corrigés
    const [rows] = await db.execute('SELECT ID, DEVICE_UID, PATH_FILE FROM path_histo_trajet_geojson WHERE IS_CORRECTED = 0');
    logger.info(`${rows.length} trips to correct`);

    for (const trip of rows) {
      if (!isValidImei(trip.DEVICE_UID)) {
        logger.warn(`Skipping invalid IMEI ${trip.DEVICE_UID}`);
        continue;
      }
      const success = await correctTrajectory(db, trip.DEVICE_UID, trip.ID, trip.PATH_FILE);
      if (!success) {
        logger.warn(`Failed to correct trip ID ${trip.ID}`);
      }
    }

    logger.info('Batch correction finished');
    process.exit(0);
  } catch (err) {
    logger.error('Fatal error:', err);
    process.exit(1);
  }
})();
