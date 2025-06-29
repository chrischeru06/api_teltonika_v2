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
const axios = require('axios');

const TCP_PORT = 2354;
const HTTP_PORT = 8000;
const TCP_TIMEOUT = 300000;
const IMEI_FOLDER_BASE = '/var/www/html/api_teltonika/IMEI';
const MAX_GEOJSON_SIZE = 100 * 1024 * 1024;
const IMEI_REGEX = /^\d{15}$/;
const MAPBOX_TOKEN = 'pk.eyJ1IjoibWFydGlubWJ4IiwiYSI6ImNrMDc0dnBzNzA3c3gzZmx2bnpqb2NwNXgifQ.D6Fm6UO9bWViernvxZFW_A';

// Création dossier IMEI si besoin
if (!fs.existsSync(IMEI_FOLDER_BASE)) {
  fs.mkdirSync(IMEI_FOLDER_BASE, { recursive: true, mode: 0o755 });
}

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
  return gps &&
    gps.latitude !== 0 &&
    gps.longitude !== 0 &&
    Math.abs(gps.latitude) <= 90 &&
    Math.abs(gps.longitude) <= 180;
}

function isValidImei(imei) {
  return IMEI_REGEX.test(imei);
}

function getImeiFolder(imei) {
  if (!isValidImei(imei)) throw new Error(`Invalid IMEI format: ${imei}`);
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

// Calcul simple distance en mètres entre deux points GPS (Haversine)
function haversineDistance([lng1, lat1], [lng2, lat2]) {
  const toRad = deg => (deg * Math.PI) / 180;
  const R = 6371000; // m
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Enhanced static points filtering
function filterStaticPoints(features) {
  if (!features.length) return features;
  
  const filtered = [features[0]];
  const minMovementThreshold = 5; // meters
  
  for (let i = 1; i < features.length; i++) {
    const prev = filtered[filtered.length - 1];
    const curr = features[i];
    
    if (!prev.geometry?.coordinates || !curr.geometry?.coordinates) {
      continue;
    }
    
    const [lngPrev, latPrev] = prev.geometry.coordinates;
    const [lngCurr, latCurr] = curr.geometry.coordinates;

    // Check if coordinates are exactly the same
    if (lngCurr === lngPrev && latCurr === latPrev) {
      continue; // Skip identical points
    }
    
    // Check if movement is significant enough
    const distance = haversineDistance([lngPrev, latPrev], [lngCurr, latCurr]);
    if (distance >= minMovementThreshold) {
      filtered.push(curr);
    }
  }
  
  // Always include the last point if it's different from the last filtered point
  const lastOriginal = features[features.length - 1];
  const lastFiltered = filtered[filtered.length - 1];
  
  if (features.length > 1 && lastOriginal !== lastFiltered) {
    const [lngOrig, latOrig] = lastOriginal.geometry?.coordinates || [0, 0];
    const [lngFilt, latFilt] = lastFiltered.geometry?.coordinates || [0, 0];
    
    if (lngOrig !== lngFilt || latOrig !== latFilt) {
      filtered.push(lastOriginal);
    }
  }
  
  return filtered;
}

// Enhanced filtering function for outliers
function filterOutliers(features, maxDistance = 500) {
  if (!features.length) return features;
  
  const filtered = [features[0]];
  let consecutiveOutliers = 0;
  const maxConsecutiveOutliers = 3;
  
  for (let i = 1; i < features.length; i++) {
    const prev = filtered[filtered.length - 1];
    const curr = features[i];
    
    if (!prev.geometry?.coordinates || !curr.geometry?.coordinates) {
      continue;
    }
    
    const dist = haversineDistance(prev.geometry.coordinates, curr.geometry.coordinates);
    
    if (dist <= maxDistance) {
      filtered.push(curr);
      consecutiveOutliers = 0;
    } else {
      consecutiveOutliers++;
      
      // If we have too many consecutive outliers, we might be missing real movement
      // so include this point to avoid losing track
      if (consecutiveOutliers >= maxConsecutiveOutliers) {
        filtered.push(curr);
        consecutiveOutliers = 0;
        logger.warn(`Including potential outlier after ${maxConsecutiveOutliers} consecutive outliers (distance=${dist.toFixed(1)}m)`);
      } else {
        logger.warn(`Filtered out GPS outlier (distance=${dist.toFixed(1)}m) at index ${i}`);
      }
    }
  }
  
  return filtered;
}

// Enhanced coordinate validation
function validateCoordinates(coordinates) {
  const errors = [];
  
  for (let i = 0; i < coordinates.length; i++) {
    const [lng, lat] = coordinates[i];
    
    if (typeof lng !== 'number' || typeof lat !== 'number') {
      errors.push(`Invalid coordinate type at index ${i}: [${lng}, ${lat}]`);
      continue;
    }
    
    if (isNaN(lng) || isNaN(lat)) {
      errors.push(`NaN coordinate at index ${i}: [${lng}, ${lat}]`);
      continue;
    }
    
    if (Math.abs(lat) > 90) {
      errors.push(`Invalid latitude at index ${i}: ${lat}`);
    }
    
    if (Math.abs(lng) > 180) {
      errors.push(`Invalid longitude at index ${i}: ${lng}`);
    }
    
    if (lng === 0 && lat === 0) {
      errors.push(`Zero coordinates at index ${i}`);
    }
  }
  
  return errors;
}

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use('/media', express.static(IMEI_FOLDER_BASE));

// API pour récupérer les trajets historiques d'un device_uid
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

    return res.status(200).json(rows);
  } catch (error) {
    logger.error('Error fetching trips:', error);
    return res.status(500).json({ message: 'Server error' });
  }
});

// Dernier trajet par device_uid
app.get('/api/last-trajets', async (req, res) => {
  try {
    const [rows] = await db.execute(`
      SELECT DEVICE_UID, TRIP_START, TRIP_END, PATH_FILE, LATITUDE, LONGITUDE
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY DEVICE_UID ORDER BY TRIP_END DESC) AS rn
        FROM path_histo_trajet_geojson
      ) AS t
      WHERE t.rn = 1
    `);
    res.status(200).json(rows);
  } catch (err) {
    logger.error('Error fetching last trips:', err.message);
    res.status(500).json({ message: 'Server error' });
  }
});

// Récupération d'une trajectoire spécifique
app.get('/api/trajectory/:device_uid/:trip_id', async (req, res) => {
  const { device_uid, trip_id } = req.params;
  if (!isValidImei(device_uid)) return res.status(400).json({ message: 'Invalid IMEI' });

  try {
    const [rows] = await db.execute(
      `SELECT * FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? AND ID = ?`,
      [device_uid, trip_id]
    );

    if (!rows.length) {
      return res.status(404).json({ message: 'Trajectory not found' });
    }

    const traj = rows[0];
    if (!fs.existsSync(traj.PATH_FILE)) {
      return res.status(404).json({ message: 'Trajectory file missing' });
    }

    const content = fs.readFileSync(traj.PATH_FILE, 'utf8');
    const geojson = JSON.parse(content);

    return res.status(200).json({
      ...traj,
      geojson,
    });
  } catch (err) {
    logger.error('Error fetching trajectory:', err.message);
    return res.status(500).json({ message: 'Server error' });
  }
});

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

// Enhanced trajectory correction function with better validation and error handling
async function correctTrajectoryWithMapbox(device_uid, pathFile) {
  try {
    if (!fs.existsSync(pathFile)) {
      logger.error(`Raw trajectory file missing: ${pathFile}`);
      return null;
    }

    const rawData = JSON.parse(fs.readFileSync(pathFile, 'utf8'));

    if (!rawData.features || !Array.isArray(rawData.features)) {
      logger.error(`Invalid GeoJSON structure for device ${device_uid}`);
      return null;
    }

    // Enhanced filtering with better validation
    rawData.features = filterStaticPoints(rawData.features);
    rawData.features = filterOutliers(rawData.features, 500); // Increased tolerance to 500m

    const coordinates = rawData.features
      .map(f => {
        if (!f.geometry || !f.geometry.coordinates) return null;
        const [lng, lat] = f.geometry.coordinates;
        // More strict coordinate validation
        if (typeof lng !== 'number' || typeof lat !== 'number') return null;
        if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
        if (lng === 0 && lat === 0) return null;
        // Round to 6 decimal places to avoid precision issues
        return [Math.round(lng * 1000000) / 1000000, Math.round(lat * 1000000) / 1000000];
      })
      .filter(coord => coord !== null);

    if (coordinates.length < 2) {
      logger.warn(`Not enough valid coordinates for device ${device_uid} after filtering (${coordinates.length} points)`);
      return null;
    }

    // Validate coordinates before processing
    const validationErrors = validateCoordinates(coordinates);
    if (validationErrors.length > 0) {
      logger.error(`Coordinate validation errors for device ${device_uid}:`, validationErrors.slice(0, 5));
      return null;
    }

    // Enhanced sampling with minimum distance between points
    const minDistanceBetweenPoints = 10; // meters
    let sampled = [coordinates[0]];
    
    for (let i = 1; i < coordinates.length; i++) {
      const lastSampled = sampled[sampled.length - 1];
      const current = coordinates[i];
      const distance = haversineDistance(lastSampled, current);
      
      if (distance >= minDistanceBetweenPoints) {
        sampled.push(current);
      }
    }

    // Ensure we have the last point if it's significantly different
    const lastOriginal = coordinates[coordinates.length - 1];
    const lastSampled = sampled[sampled.length - 1];
    if (haversineDistance(lastSampled, lastOriginal) > minDistanceBetweenPoints) {
      sampled.push(lastOriginal);
    }

    // Limit to 100 points max for Mapbox API
    if (sampled.length > 100) {
      const step = Math.floor(sampled.length / 100);
      const reduced = [];
      for (let i = 0; i < sampled.length; i += step) {
        reduced.push(sampled[i]);
      }
      // Always include first and last
      if (reduced[0] !== sampled[0]) reduced.unshift(sampled[0]);
      if (reduced[reduced.length - 1] !== sampled[sampled.length - 1]) {
        reduced.push(sampled[sampled.length - 1]);
      }
      sampled = reduced.slice(0, 100);
    }

    if (sampled.length < 2) {
      logger.warn(`Not enough points after sampling for device ${device_uid}`);
      return null;
    }

    const coordString = sampled.map(c => `${c[0]},${c[1]}`).join(';');
    logger.info(`Mapbox Matching coordString for device ${device_uid}: ${coordString.substring(0, 200)}...`);

    // Try different profiles with better error handling
    const profiles = ['driving', 'walking'];
    let response = null;
    let lastError = null;

    for (const profile of profiles) {
      try {
        const url = `https://api.mapbox.com/matching/v5/mapbox/${profile}/${coordString}`;
        logger.info(`Trying Mapbox matching with profile '${profile}' for device ${device_uid}`);
        
        const config = {
          timeout: 30000,
          params: {
            access_token: MAPBOX_TOKEN,
            geometries: 'geojson',
            overview: 'full',
            steps: 'false',
           // annotations: 'false'
          }
        };

        response = await axios.get(url, config);

        if (response.data.code === 'NoSegment') {
          logger.warn(`Mapbox NoSegment error with profile '${profile}' for device ${device_uid}`);
          lastError = new Error(`NoSegment error with profile ${profile}`);
          response = null;
          continue;
        }
        
        if (response.data.code === 'NoMatch') {
          logger.warn(`Mapbox NoMatch error with profile '${profile}' for device ${device_uid}`);
          lastError = new Error(`NoMatch error with profile ${profile}`);
          response = null;
          continue;
        }

        if (response.data.code !== 'Ok') {
          logger.warn(`Mapbox returned code '${response.data.code}' with profile '${profile}' for device ${device_uid}`);
          lastError = new Error(`Mapbox error code: ${response.data.code}`);
          response = null;
          continue;
        }

        // Validate response structure
        if (!response.data.matchings || !response.data.matchings[0] || !response.data.matchings[0].geometry) {
          logger.warn(`Invalid response structure from Mapbox for device ${device_uid}`);
          lastError = new Error('Invalid response structure from Mapbox');
          response = null;
          continue;
        }

        // Success - break out of loop
        logger.info(`Successfully matched trajectory with profile '${profile}' for device ${device_uid}`);
        break;

      } catch (axiosErr) {
        logger.error(`Axios error during Mapbox matching with profile '${profile}' for device ${device_uid}: ${axiosErr.message}`);
        
        // Log more details for 422 errors
        if (axiosErr.response && axiosErr.response.status === 422) {
          logger.error(`422 Error details: ${JSON.stringify(axiosErr.response.data)}`);
          logger.error(`Coordinates that caused 422: ${coordString.substring(0, 500)}...`);
        }
        
        lastError = axiosErr;
        response = null;
      }
    }

    if (!response || !response.data?.matchings || !response.data.matchings[0]) {
      logger.error(`Mapbox matching failed for device ${device_uid}, no valid matchings`);
      if (lastError) {
        logger.error(`Last error detail: ${lastError.message}`);
      }
      return null;
    }

    const matching = response.data.matchings[0];
    const correctedGeoJSON = {
      type: 'Feature',
      geometry: matching.geometry,
      properties: {
        device_uid,
        source: 'mapbox-matching',
        original_points: coordinates.length,
        sampled_points: sampled.length,
        matched_points: matching.geometry.coordinates.length,
        confidence: matching.confidence || 0,
        distance: matching.distance || 0,
        duration: matching.duration || 0,
        corrected_at: new Date().toISOString()
      }
    };

    const folder = getImeiFolder(device_uid);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const correctedFilePath = path.join(folder, `corrected_${timestamp}.geojson`);
    
    fs.writeFileSync(correctedFilePath, JSON.stringify(correctedGeoJSON, null, 2), { mode: 0o644 });

    logger.info(`Corrected trajectory for ${device_uid} saved to ${correctedFilePath}`);
    logger.info(`Correction stats: ${coordinates.length} -> ${sampled.length} -> ${matching.geometry.coordinates.length} points`);
    
    return correctedFilePath;

  } catch (err) {
    logger.error(`Error in correction with Mapbox: ${err.message}`);
    logger.error(`Stack trace: ${err.stack}`);
    return null;
  }
}

async function correctAndReplaceTrajectory(device_uid, trip_id) {
  logger.info(`Starting correction for device_uid=${device_uid}, trip_id=${trip_id}`);
  try {
    const [rows] = await db.execute(
      `SELECT ID, PATH_FILE, IS_CORRECTED FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? AND ID = ?`,
      [device_uid, trip_id]
    );

    if (!rows.length) {
      const msg = `Trajectory ID ${trip_id} for device ${device_uid} not found in DB`;
      logger.error(msg);
      throw new Error(msg);
    }

    const row = rows[0];
    logger.info(`Trajectory found. PATH_FILE: ${row.PATH_FILE}, IS_CORRECTED: ${row.IS_CORRECTED}`);

    if (!fs.existsSync(row.PATH_FILE)) {
      const msg = `Trajectory file ${row.PATH_FILE} does not exist on disk.`;
      logger.error(msg);
      throw new Error(msg);
    }

    if (row.IS_CORRECTED === 1) {
      logger.info(`Trajectory ${trip_id} for ${device_uid} already corrected`);
      return row.PATH_FILE;
    }

    const correctedPath = await correctTrajectoryWithMapbox(device_uid, row.PATH_FILE);

    if (!correctedPath) {
      const msg = 'Correction failed, correctedPath is null';
      logger.error(msg);
      throw new Error(msg);
    }

    await db.execute(
      `UPDATE path_histo_trajet_geojson SET PATH_FILE = ?, IS_CORRECTED = 1, UPDATED_AT = NOW() WHERE ID = ?`,
      [correctedPath, row.ID]
    );
    logger.info(`Database updated for trajectory ${trip_id}, device ${device_uid}`);

    if (fs.existsSync(row.PATH_FILE) && row.PATH_FILE !== correctedPath) {
      fs.unlinkSync(row.PATH_FILE);
      logger.info(`Deleted old file: ${row.PATH_FILE}`);
    }

    return correctedPath;
  } catch (err) {
    logger.error(`Correction error in correctAndReplaceTrajectory: ${err.message}`);
    throw err;
  }
}

app.post('/api/correct-trajectory/:device_uid/:trip_id', async (req, res) => {
  const { device_uid, trip_id } = req.params;
  logger.info(`POST /api/correct-trajectory called with device_uid=${device_uid}, trip_id=${trip_id}`);
  if (!isValidImei(device_uid)) {
    logger.warn(`Invalid IMEI received: ${device_uid}`);
    return res.status(400).json({ message: 'Invalid IMEI' });
  }

  try {
    const corrected = await correctAndReplaceTrajectory(device_uid, trip_id);
    if (!corrected) {
      logger.error('Correction failed: corrected path is null');
      return res.status(500).json({ message: 'Correction failed' });
    }
    logger.info(`Correction successful for device_uid=${device_uid}, trip_id=${trip_id}`);
    return res.status(200).json({ message: 'Trajectory corrected', corrected_file: corrected });
  } catch (err) {
    logger.error(`API /correct-trajectory error: ${err.stack || err.message}`);
    return res.status(500).json({ message: 'Server error', error: err.message });
  }
});

app.post('/api/correct-all', async (req, res) => {
  logger.info(`POST /api/correct-all called`);
  try {
    const [rows] = await db.execute(
      `SELECT ID, DEVICE_UID FROM path_histo_trajet_geojson WHERE IS_CORRECTED = 0`
    );
    if (rows.length === 0) {
      logger.info('No trajectories to correct');
      return res.status(200).json({ message: 'No trajectories to correct' });
    }

    let success = 0, fail = 0;

    for (const row of rows) {
      try {
        await correctAndReplaceTrajectory(row.DEVICE_UID, row.ID);
        success++;
      } catch (err) {
        logger.error(`Failed to correct trip ID ${row.ID}: ${err.message}`);
        fail++;
      }
    }

    res.status(200).json({
      message: 'Correction batch finished',
      corrected: success,
      failed: fail
    });
  } catch (err) {
    logger.error(`Error in /api/correct-all: ${err.message}`);
    res.status(500).json({ message: 'Server error', error: err.message });
  }
});
// ===========================
// ENDPOINT: GET CORRECTED TRAJECTORY
// ===========================
app.get("/api/corrected-trajectory/:device_uid/:trip_id", async (req, res) => {
  const { device_uid, trip_id } = req.params;

  if (!isValidImei(device_uid)) {
    return res.status(400).json({ message: "Invalid IMEI" });
  }

  try {
    const [rows] = await db.execute(
      `SELECT * FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? AND ID = ? AND IS_CORRECTED = 1`,
      [device_uid, trip_id]
    );
    if (!rows.length) {
      return res.status(404).json({ message: "Corrected trajectory not found" });
    }

    const trajectory = rows[0];
    if (!fs.existsSync(trajectory.PATH_FILE)) {
      return res.status(404).json({ message: "Corrected trajectory file missing from disk" });
    }

    const geoJsonData = JSON.parse(fs.readFileSync(trajectory.PATH_FILE, "utf8"));
    return res.status(200).json({
      id: trajectory.ID,
      device_uid: trajectory.DEVICE_UID,
      trip_start: trajectory.TRIP_START,
      trip_end: trajectory.TRIP_END,
      latitude: trajectory.LATITUDE,
      longitude: trajectory.LONGITUDE,
      is_corrected: trajectory.IS_CORRECTED,
      created_at: trajectory.CREATED_AT,
      updated_at: trajectory.UPDATED_AT,
      corrected_geojson: geoJsonData,
      correction_stats: {
        original_points: geoJsonData.properties?.original_points ?? null,
        sampled_points: geoJsonData.properties?.sampled_points ?? null,
        matched_points: geoJsonData.properties?.matched_points ?? null,
        confidence: geoJsonData.properties?.confidence ?? null,
        distance: geoJsonData.properties?.distance ?? null,
        duration: geoJsonData.properties?.duration ?? null,
        corrected_at: geoJsonData.properties?.corrected_at ?? null
      }
    });
  } catch (error) {
    logger.error(`Error fetching corrected trajectory for ${device_uid}/${trip_id}: ${error.message}`);
    res.status(500).json({ message: "Server error" });
  }
});

// ===========================
// ENDPOINT: POST CORRECT TRAJECTORY
// ===========================
app.post("/api/correct-trajectory/:device_uid/:trip_id", async (req, res) => {
  const { device_uid, trip_id } = req.params;
  logger.info(`POST /api/correct-trajectory called with device_uid=${device_uid}, trip_id=${trip_id}`);

  if (!isValidImei(device_uid)) {
    logger.warn(`Invalid IMEI received: ${device_uid}`);
    return res.status(400).json({ message: "Invalid IMEI" });
  }

  try {
    const correctedPath = await correctAndReplaceTrajectory(device_uid, trip_id);
    if (!correctedPath) {
      logger.error("Correction failed: corrected path is null");
      return res.status(500).json({ message: "Correction failed" });
    }

    const [rows] = await db.execute(
      `SELECT * FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? AND ID = ?`,
      [device_uid, trip_id]
    );
    if (rows.length && fs.existsSync(correctedPath)) {
      const geoJsonData = JSON.parse(fs.readFileSync(correctedPath, "utf8"));
      return res.status(200).json({
        message: "Trajectory corrected successfully",
        corrected_file: correctedPath,
        trajectory: {
          id: rows[0].ID,
          device_uid: rows[0].DEVICE_UID,
          trip_start: rows[0].TRIP_START,
          trip_end: rows[0].TRIP_END,
          is_corrected: rows[0].IS_CORRECTED,
          corrected_geojson: geoJsonData,
          correction_stats: {
            original_points: geoJsonData.properties?.original_points ?? null,
            sampled_points: geoJsonData.properties?.sampled_points ?? null,
            matched_points: geoJsonData.properties?.matched_points ?? null,
            confidence: geoJsonData.properties?.confidence ?? null,
            distance: geoJsonData.properties?.distance ?? null,
            duration: geoJsonData.properties?.duration ?? null
          }
        }
      });
    }

    return res.status(200).json({
      message: "Trajectory corrected successfully",
      corrected_file: correctedPath,
      note: "Use GET /api/corrected-trajectory/:device_uid/:trip_id to retrieve the corrected data"
    });
  } catch (error) {
    logger.error(`API /correct-trajectory error: ${error.stack || error.message}`);
    return res.status(500).json({ message: "Server error", error: error.message });
  }
});

// ===========================
// ENDPOINT: GET ALL CORRECTED TRAJECTORIES
// ===========================
app.get("/api/corrected-trajectories/:device_uid", async (req, res) => {
  const { device_uid } = req.params;

  if (!isValidImei(device_uid)) {
    return res.status(400).json({ message: "Invalid IMEI" });
  }

  try {
    const [rows] = await db.execute(
      `SELECT ID, DEVICE_UID, TRIP_START, TRIP_END, LATITUDE, LONGITUDE, IS_CORRECTED, CREATED_AT, UPDATED_AT 
       FROM path_histo_trajet_geojson 
       WHERE DEVICE_UID = ? AND IS_CORRECTED = 1 
       ORDER BY TRIP_START DESC`,
      [device_uid]
    );
    if (!rows.length) {
      return res.status(404).json({ message: "No corrected trajectories found for this device" });
    }

    return res.status(200).json({ device_uid, total_corrected: rows.length, trajectories: rows });
  } catch (error) {
    logger.error(`Error fetching corrected trajectories for ${device_uid}: ${error.message}`);
    return res.status(500).json({ message: "Server error" });
  }
});
// Function to undo trajectory correction
async function undoTrajectoryCorrection(device_uid, trip_id) {
  logger.info(`Starting undo correction for device_uid=${device_uid}, trip_id=${trip_id}`);
  
  try {
    // Get the current trajectory record
    const [rows] = await db.execute(
      `SELECT ID, PATH_FILE, IS_CORRECTED, DEVICE_UID FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? AND ID = ?`,
      [device_uid, trip_id]
    );

    if (!rows.length) {
      const msg = `Trajectory ID ${trip_id} for device ${device_uid} not found in DB`;
      logger.error(msg);
      throw new Error(msg);
    }

    const row = rows[0];
    logger.info(`Trajectory found. PATH_FILE: ${row.PATH_FILE}, IS_CORRECTED: ${row.IS_CORRECTED}`);

    if (row.IS_CORRECTED === 0) {
      logger.info(`Trajectory ${trip_id} for ${device_uid} is not corrected, nothing to undo`);
      return { success: true, message: 'Trajectory was not corrected', action: 'none' };
    }

    // Look for the original trajectory file
    const folder = getImeiFolder(device_uid);
    const currentCorrectedFile = row.PATH_FILE;
    
    // Find potential original files in the folder
    // Original files typically don't have "corrected_" prefix
    const files = fs.readdirSync(folder);
    const originalFiles = files.filter(file => 
      file.endsWith('.geojson') && 
      !file.startsWith('corrected_') &&
      file !== path.basename(currentCorrectedFile)
    ).sort((a, b) => {
      // Sort by creation time (newer first)
      const statA = fs.statSync(path.join(folder, a));
      const statB = fs.statSync(path.join(folder, b));
      return statB.mtime - statA.mtime;
    });

    let originalFile = null;
    let restoredFromBackup = false;

    // Strategy 1: Look for a backup file with similar timestamp
    if (currentCorrectedFile.includes('corrected_')) {
      // Extract timestamp from corrected file name
      const correctedFileName = path.basename(currentCorrectedFile);
      const timestampMatch = correctedFileName.match(/corrected_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})/);
      
      if (timestampMatch) {
        const timestamp = timestampMatch[1];
        // Look for a backup file with this timestamp
        const backupFileName = `backup_${timestamp}.geojson`;
        const backupPath = path.join(folder, backupFileName);
        
        if (fs.existsSync(backupPath)) {
          originalFile = backupPath;
          restoredFromBackup = true;
          logger.info(`Found backup file: ${backupFileName}`);
        }
      }
    }

    // Strategy 2: If no backup found, look for the most recent original file
    if (!originalFile && originalFiles.length > 0) {
      // Check if any of these files contain reasonable trajectory data
      for (const file of originalFiles) {
        const filePath = path.join(folder, file);
        try {
          const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
          // Basic validation - check if it's a valid GeoJSON with features
          if (content.type === 'FeatureCollection' && content.features && content.features.length > 0) {
            originalFile = filePath;
            logger.info(`Using original file: ${file}`);
            break;
          }
        } catch (err) {
          logger.warn(`Could not parse potential original file ${file}: ${err.message}`);
          continue;
        }
      }
    }

    // Strategy 3: Create a basic trajectory from database records if no original file found
    if (!originalFile) {
      logger.info(`No original file found, attempting to recreate from database records`);
      
      const [trackingRows] = await db.execute(
        `SELECT latitude, longitude, date, vitesse, altitude, angle, satellites 
         FROM tracking_data 
         WHERE device_uid = ? 
         AND date BETWEEN (
           SELECT DATE_SUB(TRIP_START, INTERVAL 1 HOUR) 
           FROM path_histo_trajet_geojson 
           WHERE ID = ?
         ) AND (
           SELECT DATE_ADD(TRIP_END, INTERVAL 1 HOUR) 
           FROM path_histo_trajet_geojson 
           WHERE ID = ?
         )
         ORDER BY date ASC`,
        [device_uid, trip_id, trip_id]
      );

      if (trackingRows.length > 0) {
        // Create GeoJSON from tracking data
        const features = trackingRows.map(record => ({
          type: "Feature",
          geometry: {
            type: "Point",
            coordinates: [record.longitude, record.latitude]
          },
          properties: {
            timestamp: record.date,
            speed: record.vitesse,
            altitude: record.altitude,
            angle: record.angle,
            satellites: record.satellites
          }
        }));

        const originalGeoJSON = {
          type: "FeatureCollection",
          features: features
        };

        // Save recreated original file
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const recreatedPath = path.join(folder, `recreated_original_${timestamp}.geojson`);
        fs.writeFileSync(recreatedPath, JSON.stringify(originalGeoJSON, null, 2), { mode: 0o644 });
        originalFile = recreatedPath;
        logger.info(`Recreated original trajectory from database: ${recreatedPath}`);
      }
    }

    if (!originalFile) {
      const msg = `Cannot find or recreate original trajectory for device ${device_uid}, trip ${trip_id}`;
      logger.error(msg);
      throw new Error(msg);
    }

    // Update database to point to original file and mark as not corrected
    await db.execute(
      `UPDATE path_histo_trajet_geojson 
       SET PATH_FILE = ?, IS_CORRECTED = 0, UPDATED_AT = NOW() 
       WHERE ID = ?`,
      [originalFile, row.ID]
    );

    logger.info(`Database updated for trajectory ${trip_id}, device ${device_uid} - correction undone`);

    // Clean up the corrected file if it exists and is different from original
    if (fs.existsSync(currentCorrectedFile) && currentCorrectedFile !== originalFile) {
      // Move corrected file to backup instead of deleting it
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const backupPath = path.join(folder, `backup_corrected_${timestamp}.geojson`);
      fs.renameSync(currentCorrectedFile, backupPath);
      logger.info(`Moved corrected file to backup: ${backupPath}`);
    }

    return {
      success: true,
      message: 'Trajectory correction undone successfully',
      action: 'restored',
      original_file: originalFile,
      restored_from_backup: restoredFromBackup
    };

  } catch (err) {
    logger.error(`Error in undoTrajectoryCorrection: ${err.message}`);
    throw err;
  }
}

// Enhanced correction function that creates backups
async function correctTrajectoryWithMapboxBackup(device_uid, pathFile) {
  try {
    // Create backup of original file before correction
    const folder = getImeiFolder(device_uid);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupPath = path.join(folder, `backup_${timestamp}.geojson`);
    
    if (fs.existsSync(pathFile)) {
      fs.copyFileSync(pathFile, backupPath);
      logger.info(`Created backup of original trajectory: ${backupPath}`);
    }

    // Call the original correction function
    return await correctTrajectoryWithMapbox(device_uid, pathFile);
  } catch (err) {
    logger.error(`Error in correctTrajectoryWithMapboxBackup: ${err.message}`);
    throw err;
  }
}

// API endpoint to undo trajectory correction
app.post('/api/undo-correction/:device_uid/:trip_id', async (req, res) => {
  const { device_uid, trip_id } = req.params;
  logger.info(`POST /api/undo-correction called with device_uid=${device_uid}, trip_id=${trip_id}`);
  
  if (!isValidImei(device_uid)) {
    logger.warn(`Invalid IMEI received: ${device_uid}`);
    return res.status(400).json({ message: 'Invalid IMEI' });
  }

  try {
    const result = await undoTrajectoryCorrection(device_uid, trip_id);
    
    if (!result.success) {
      return res.status(500).json({ message: 'Undo correction failed', details: result });
    }

    logger.info(`Undo correction successful for device_uid=${device_uid}, trip_id=${trip_id}`);
    
    // Return the original trajectory data
    try {
      const [rows] = await db.execute(
        `SELECT * FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? AND ID = ?`,
        [device_uid, trip_id]
      );

      if (rows.length && fs.existsSync(result.original_file)) {
        const geoJsonContent = fs.readFileSync(result.original_file, 'utf8');
        const geoJsonData = JSON.parse(geoJsonContent);
        
        return res.status(200).json({
          message: result.message,
          action: result.action,
          restored_from_backup: result.restored_from_backup,
          trajectory: {
            id: rows[0].ID,
            device_uid: rows[0].DEVICE_UID,
            trip_start: rows[0].TRIP_START,
            trip_end: rows[0].TRIP_END,
            is_corrected: rows[0].IS_CORRECTED,
            original_geojson: geoJsonData
          }
        });
      }
    } catch (readError) {
      logger.warn(`Could not read original file immediately: ${readError.message}`);
    }

    return res.status(200).json(result);

  } catch (err) {
    logger.error(`API /undo-correction error: ${err.stack || err.message}`);
    return res.status(500).json({ message: 'Server error', error: err.message });
  }
});

// API endpoint to undo all corrections for a device
app.post('/api/undo-all-corrections/:device_uid', async (req, res) => {
  const { device_uid } = req.params;
  logger.info(`POST /api/undo-all-corrections called with device_uid=${device_uid}`);
  
  if (!isValidImei(device_uid)) {
    return res.status(400).json({ message: 'Invalid IMEI' });
  }

  try {
    const [rows] = await db.execute(
      `SELECT ID FROM path_histo_trajet_geojson WHERE DEVICE_UID = ? AND IS_CORRECTED = 1`,
      [device_uid]
    );

    if (rows.length === 0) {
      return res.status(200).json({ 
        message: 'No corrected trajectories found for this device',
        undone: 0
      });
    }

    let success = 0, failed = 0;
    const results = [];

    for (const row of rows) {
      try {
        const result = await undoTrajectoryCorrection(device_uid, row.ID);
        if (result.success) {
          success++;
          results.push({ trip_id: row.ID, status: 'success', message: result.message });
        } else {
          failed++;
          results.push({ trip_id: row.ID, status: 'failed', message: result.message });
        }
      } catch (err) {
        failed++;
        results.push({ trip_id: row.ID, status: 'failed', message: err.message });
        logger.error(`Failed to undo correction for trip ID ${row.ID}: ${err.message}`);
      }
    }

    res.status(200).json({
      message: 'Undo corrections batch finished',
      device_uid,
      total_processed: rows.length,
      undone: success,
      failed: failed,
      results: results
    });

  } catch (err) {
    logger.error(`Error in /api/undo-all-corrections: ${err.message}`);
    res.status(500).json({ message: 'Server error', error: err.message });
  }
});

// API endpoint to get trajectory status (corrected or original)
app.get('/api/trajectory-status/:device_uid/:trip_id', async (req, res) => {
  const { device_uid, trip_id } = req.params;
  
  if (!isValidImei(device_uid)) {
    return res.status(400).json({ message: 'Invalid IMEI' });
  }

  try {
    const [rows] = await db.execute(
      `SELECT ID, DEVICE_UID, IS_CORRECTED, PATH_FILE, TRIP_START, TRIP_END, UPDATED_AT 
       FROM path_histo_trajet_geojson 
       WHERE DEVICE_UID = ? AND ID = ?`,
      [device_uid, trip_id]
    );

    if (!rows.length) {
      return res.status(404).json({ message: 'Trajectory not found' });
    }

    const trajectory = rows[0];
    const fileExists = fs.existsSync(trajectory.PATH_FILE);
    
    // Check for backup files
    const folder = getImeiFolder(device_uid);
    const files = fs.readdirSync(folder);
    const backupFiles = files.filter(file => file.startsWith('backup_')).length;

    return res.status(200).json({
      id: trajectory.ID,
      device_uid: trajectory.DEVICE_UID,
      is_corrected: trajectory.IS_CORRECTED === 1,
      path_file: trajectory.PATH_FILE,
      file_exists: fileExists,
      trip_start: trajectory.TRIP_START,
      trip_end: trajectory.TRIP_END,
      last_updated: trajectory.UPDATED_AT,
      backup_files_available: backupFiles,
      can_undo: trajectory.IS_CORRECTED === 1,
      status: trajectory.IS_CORRECTED === 1 ? 'corrected' : 'original'
    });

  } catch (error) {
    logger.error(`Error checking trajectory status: ${error.message}`);
    return res.status(500).json({ message: 'Server error' });
  }
});

async function tryRecreateOriginalFromDatabase(db, device_uid, trip_id) {
  logger.info(`Attempting to recreate original trajectory for ${device_uid}/${trip_id}`);
  try {
    const [trackingRows] = await db.execute(
      `SELECT latitude, longitude, date, vitesse, altitude, angle, satellites
       FROM tracking_data
       WHERE device_uid = ?
       AND date BETWEEN (
           SELECT DATE_SUB(TRIP_START, INTERVAL 1 HOUR) 
           FROM path_histo_trajet_geojson 
           WHERE ID = ?
       )
       AND (
           SELECT DATE_ADD(TRIP_END, INTERVAL 1 HOUR) 
           FROM path_histo_trajet_geojson 
           WHERE ID = ?
       )
       ORDER BY date ASC`,
      [device_uid, trip_id, trip_id]
    );

    if (!trackingRows.length) {
      logger.warn(`No tracking data found for ${device_uid}/${trip_id}. Aborting recreation...`);
      return null;
    }

    const features = trackingRows.map(record => ({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [record.longitude, record.latitude]
      },
      properties: {
        timestamp: record.date,
        speed: record.vitesse,
        altitude: record.altitude,
        angle: record.angle,
        satellites: record.satellites
      }
    }));

    const originalGeoJSON = {
      type: "FeatureCollection",
      features
    };
    const folder = getImeiFolder(device_uid);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const recreatedPath = path.join(folder, `recreated_original_${timestamp}.geojson`);
    fs.writeFileSync(recreatedPath, JSON.stringify(originalGeoJSON, null, 2), { mode: 0o644 });
    logger.info(`Original trajectory recreated from database at ${recreatedPath}`);
    return recreatedPath;

  } catch (error) {
    logger.error(`Error in tryRecreateOriginalFromDatabase for ${device_uid}/${trip_id}: ${error.message}`);
    return null;
  }
}


const tcpServer = net.createServer(socket => {
  let imei = null;
  let parser = null;
  socket.setTimeout(TCP_TIMEOUT);
  socket.on('timeout', () => socket.destroy());
  socket.on('data', async data => {
    try {
      if (!parser) {
        parser = new Parser(data);
        if (parser.isImeiPacket) {
          imei = parser.imei;
          if (!isValidImei(imei)) return socket.destroy();
          await createImeiFolder(imei);
          return socket.write(Buffer.from([0x01]));
        }
      }
      if (!imei) return socket.destroy();
      parser = new Parser(data);
      if (parser.isDataPacket && parser.avl && parser.avl.records) {
        const geoJsonFeatures = [];
        for (const rec of parser.avl.records) {
          if (!isValidGps(rec.gps)) continue;
          await insertTrackingData([
            rec.gps.latitude,
            rec.gps.longitude,
            rec.gps.speed || 0,
            rec.gps.altitude || 0,
            toMysqlDatetime(new Date(rec.timestamp).toISOString()),
            rec.gps.angle || 0,
            rec.gps.satellites || 0,
            rec.gps.speed > 0 ? 1 : 0,
            1,
            imei,
            rec.ioElements?.['239'] || 0
          ]);
          geoJsonFeatures.push({
            type: "Feature",
            geometry: { type: "Point", coordinates: [rec.gps.longitude, rec.gps.latitude] },
            properties: { timestamp: new Date(rec.timestamp).toISOString() }
          });
        }
        if (geoJsonFeatures.length) {
          const folder = getImeiFolder(imei);
          const file = path.join(folder, `${Date.now()}.geojson`);
          const geoStr = JSON.stringify({ type: "FeatureCollection", features: geoJsonFeatures }, null, 2);
          if (geoStr.length <= MAX_GEOJSON_SIZE) fs.writeFileSync(file, geoStr, { mode: 0o644 });
        }
        const ack = Buffer.allocUnsafe(4);
        ack.writeUInt32BE(geoJsonFeatures.length, 0);
        socket.write(ack);
      }
    } catch (err) {
      logger.error(`TCP error: ${err.message}`);
      socket.destroy();
    }
  });
  socket.on('error', err => logger.error(`Socket error: ${err.message}`));
  socket.on('close', () => logger.info(`Connection closed: ${imei || 'unknown'}`));
});

tcpServer.listen(TCP_PORT, () => logger.info(`TCP Server listening on ${TCP_PORT}`));
server.listen(HTTP_PORT, () => logger.info(`HTTP/WebSocket server on ${HTTP_PORT}`));