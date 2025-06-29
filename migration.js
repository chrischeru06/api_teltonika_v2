const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const winston = require('winston');

// Configuration
const IMEI_FOLDER_BASE = '/var/www/html/api_teltonika/IMEI';
const MAX_GEOJSON_SIZE = 100 * 1024 * 1024;
const BACKUP_SUFFIX = '.backup';

// Configuration de la base de données
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'Chris@1996..',
  database: 'car_trucking_v3',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
};

// Logger pour suivre la progression
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'migration.log' })
  ]
});

let db;

async function initDb() {
  try {
    db = await mysql.createPool(dbConfig);
    logger.info('✅ Connexion à la base de données établie');
  } catch (err) {
    logger.error('❌ Erreur de connexion à la base de données:', err.message);
    process.exit(1);
  }
}

// Fonction pour lire et parser un fichier GeoJSON existant
function readExistingGeoJSON(filepath) {
  try {
    if (!fs.existsSync(filepath)) {
      logger.warn(`⚠️  Fichier introuvable: ${filepath}`);
      return null;
    }

    const content = fs.readFileSync(filepath, 'utf8');
    const geojson = JSON.parse(content);
    
    // Vérifier si c'est déjà au nouveau format
    if (geojson.features && geojson.features.some(f => f.properties?.layer)) {
      logger.info(`ℹ️  Fichier déjà au nouveau format: ${filepath}`);
      return null; // Déjà migré
    }

    return geojson;
  } catch (err) {
    logger.error(`❌ Erreur lors de la lecture du fichier ${filepath}:`, err.message);
    return null;
  }
}

// Fonction pour convertir l'ancien format vers le nouveau
function convertToNewFormat(oldGeojson, tripData) {
  try {
    let points = [];

    // Extraire les points selon l'ancien format
    if (oldGeojson.type === 'FeatureCollection' && oldGeojson.features) {
      // Format FeatureCollection de points
      points = oldGeojson.features
        .filter(f => f.geometry && f.geometry.type === 'Point')
        .map(f => ({
          geometry: f.geometry,
          properties: f.properties || {}
        }));
    } else if (oldGeojson.type === 'Feature' && oldGeojson.geometry?.type === 'LineString') {
      // Format LineString - convertir les coordonnées en points
      points = oldGeojson.geometry.coordinates.map((coord, index) => ({
        geometry: {
          type: 'Point',
          coordinates: coord
        },
        properties: {
          timestamp: tripData.TRIP_START, // Utiliser le début du trajet par défaut
          speed: 0,
          angle: 0,
          satellites: 0,
          point_index: index
        }
      }));
    }

    if (points.length === 0) {
      logger.warn('⚠️  Aucun point trouvé dans le fichier GeoJSON');
      return null;
    }

    // Trier les points par timestamp si disponible
    points.sort((a, b) => {
      const timeA = a.properties.timestamp ? new Date(a.properties.timestamp) : new Date(tripData.TRIP_START);
      const timeB = b.properties.timestamp ? new Date(b.properties.timestamp) : new Date(tripData.TRIP_START);
      return timeA - timeB;
    });

    // Extraire les coordonnées pour la LineString
    const coordinates = points.map(p => p.geometry.coordinates);

    // Créer la feature LineString
    const lineFeature = {
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: coordinates
      },
      properties: {
        layer: "path",
        trip_start: tripData.TRIP_START,
        trip_end: tripData.TRIP_END,
        total_points: points.length,
        device_uid: tripData.DEVICE_UID,
        migrated_at: new Date().toISOString()
      }
    };

    // Ajouter la propriété layer aux points
    const pointFeatures = points.map((p, index) => ({
      type: "Feature",
      geometry: p.geometry,
      properties: {
        ...p.properties,
        layer: "point",
        device_uid: tripData.DEVICE_UID,
        point_sequence: index + 1
      }
    }));

    // Créer le nouveau GeoJSON
    const newGeojson = {
      type: "FeatureCollection",
      features: [...pointFeatures, lineFeature],
      migration_info: {
        original_format: oldGeojson.type,
        migrated_at: new Date().toISOString(),
        original_features_count: oldGeojson.features ? oldGeojson.features.length : 1,
        new_features_count: pointFeatures.length + 1
      }
    };

    return newGeojson;
  } catch (err) {
    logger.error('❌ Erreur lors de la conversion:', err.message);
    return null;
  }
}

// Fonction pour sauvegarder le nouveau fichier
function saveNewGeoJSON(filepath, geojson) {
  try {
    // Créer une sauvegarde de l'ancien fichier
    const backupPath = filepath + BACKUP_SUFFIX;
    if (fs.existsSync(filepath)) {
      fs.copyFileSync(filepath, backupPath);
      logger.info(`📁 Sauvegarde créée: ${backupPath}`);
    }

    // Écrire le nouveau fichier
    const geojsonStr = JSON.stringify(geojson, null, 2);
    
    if (Buffer.byteLength(geojsonStr) <= MAX_GEOJSON_SIZE) {
      fs.writeFileSync(filepath, geojsonStr, { mode: 0o644 });
      logger.info(`✅ Fichier migré: ${filepath}`);
      return true;
    } else {
      logger.warn(`⚠️  Fichier trop volumineux, migration ignorée: ${filepath}`);
      return false;
    }
  } catch (err) {
    logger.error(`❌ Erreur lors de la sauvegarde de ${filepath}:`, err.message);
    return false;
  }
}

// Fonction principale de migration
async function migrateAllTrips() {
  try {
    logger.info('🚀 Début de la migration des trajets...');

    // Récupérer tous les trajets de la base de données
    const [trips] = await db.execute(`
      SELECT DEVICE_UID, TRIP_START, TRIP_END, PATH_FILE, LATITUDE, LONGITUDE
      FROM path_histo_trajet_geojson
      ORDER BY TRIP_START DESC
    `);

    logger.info(`📊 ${trips.length} trajets trouvés dans la base de données`);

    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;

    for (let i = 0; i < trips.length; i++) {
      const trip = trips[i];
      const progress = `[${i + 1}/${trips.length}]`;
      
      logger.info(`${progress} Traitement du trajet ${trip.DEVICE_UID} - ${trip.TRIP_START}`);

      try {
        // Vérifier si le fichier existe
        if (!fs.existsSync(trip.PATH_FILE)) {
          logger.warn(`${progress} ⚠️  Fichier introuvable: ${trip.PATH_FILE}`);
          errorCount++;
          continue;
        }

        // Lire l'ancien fichier GeoJSON
        const oldGeojson = readExistingGeoJSON(trip.PATH_FILE);
        
        if (!oldGeojson) {
          // Fichier déjà migré ou erreur de lecture
          skippedCount++;
          continue;
        }

        // Convertir au nouveau format
        const newGeojson = convertToNewFormat(oldGeojson, trip);
        
        if (!newGeojson) {
          logger.error(`${progress} ❌ Échec de la conversion`);
          errorCount++;
          continue;
        }

        // Sauvegarder le nouveau fichier
        if (saveNewGeoJSON(trip.PATH_FILE, newGeojson)) {
          successCount++;
          logger.info(`${progress} ✅ Migration réussie`);
        } else {
          errorCount++;
        }

      } catch (err) {
        logger.error(`${progress} ❌ Erreur lors du traitement:`, err.message);
        errorCount++;
      }

      // Petite pause pour éviter de surcharger le système
      if (i % 10 === 0 && i > 0) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }

    // Résumé de la migration
    logger.info('🎉 Migration terminée!');
    logger.info(`📈 Résultats:`);
    logger.info(`   ✅ Succès: ${successCount}`);
    logger.info(`   ⚠️  Ignorés: ${skippedCount}`);
    logger.info(`   ❌ Erreurs: ${errorCount}`);
    logger.info(`   📊 Total: ${trips.length}`);

    if (errorCount > 0) {
      logger.warn(`⚠️  ${errorCount} fichiers n'ont pas pu être migrés. Consultez les logs pour plus de détails.`);
    }

  } catch (err) {
    logger.error('❌ Erreur fatale lors de la migration:', err.message);
    process.exit(1);
  }
}

// Fonction pour nettoyer les sauvegardes (optionnel)
async function cleanupBackups() {
  try {
    logger.info('🧹 Nettoyage des fichiers de sauvegarde...');
    
    const [trips] = await db.execute(`SELECT PATH_FILE FROM path_histo_trajet_geojson`);
    let cleanedCount = 0;

    for (const trip of trips) {
      const backupPath = trip.PATH_FILE + BACKUP_SUFFIX;
      if (fs.existsSync(backupPath)) {
        fs.unlinkSync(backupPath);
        cleanedCount++;
      }
    }

    logger.info(`🧹 ${cleanedCount} fichiers de sauvegarde supprimés`);
  } catch (err) {
    logger.error('❌ Erreur lors du nettoyage:', err.message);
  }
}

// Point d'entrée principal
async function main() {
  const args = process.argv.slice(2);
  const shouldCleanup = args.includes('--cleanup');
  const confirmMigration = args.includes('--confirm');

  if (!confirmMigration) {
    console.log('🔧 Script de migration des trajets GeoJSON');
    console.log('');
    console.log('Ce script va :');
    console.log('1. ✅ Lire tous les trajets existants dans la base de données');
    console.log('2. 📁 Créer une sauvegarde de chaque fichier GeoJSON');
    console.log('3. 🔄 Convertir au nouveau format (points + LineString)');
    console.log('4. 💾 Remplacer les anciens fichiers');
    console.log('');
    console.log('Usage: node migration.js --confirm [--cleanup]');
    console.log('  --confirm  : Confirme l\'exécution de la migration');
    console.log('  --cleanup  : Supprime les fichiers de sauvegarde après migration');
    console.log('');
    console.log('⚠️  ATTENTION: Assurez-vous d\'avoir une sauvegarde complète avant de continuer!');
    return;
  }

  await initDb();
  await migrateAllTrips();

  if (shouldCleanup) {
    await cleanupBackups();
  }

  await db.end();
  logger.info('✅ Migration terminée avec succès!');
}

// Gestion des erreurs non capturées
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', error);
  process.exit(1);
});

// Exécution
main().catch(err => {
  logger.error('❌ Erreur fatale:', err);
  process.exit(1);
});