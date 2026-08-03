require("dotenv").config({ path: require("path").join(__dirname, "../../.env") });
require("dotenv").config();

const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { pipeline } = require("stream");
const { promisify } = require("util");
const mongoose = require("mongoose");
const xlsx = require("xlsx");

const pipelineAsync = promisify(pipeline);


// Dear Programmer:-
// When i wrote this code, only god and
// i knew how it worked.
// Now only god knows it.
//
// Therefore, if you are trying to optimize
// this routine and it fails (most surely),
// please increase this counter as a 
// warning for the next developer: 
//
// total_hours_wasted_here: 25
// total_wasted_coffee_cups: 05
// Good luck :)
// - A Humble Programmer (Zaheer Khan)

// ================= CONFIG =================

const REGION = (process.env.AWS_REGION || "us-east-1").trim();
const BUCKET = (process.env.AWS_BUCKET_NAME || "rasta-vehicle-counter").trim();

const ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const MONGO_URI = process.env.MONGO_URI || "";

const FFMPEG_CUSTOM_PATH =
  "C:\\ffmpeg\\ffmpeg-8.1.2-essentials_build\\ffmpeg-8.1.2-essentials_build\\bin\\ffmpeg.exe";

const FFMPEG = fs.existsSync(FFMPEG_CUSTOM_PATH)
  ? FFMPEG_CUSTOM_PATH
  : "ffmpeg";

const EXCEL_PATH = (() => {
  const cliFile = process.argv.find((arg) => /\.(xlsx|xls|csv)$/i.test(arg));
  if (cliFile) return path.resolve(cliFile);
  if (process.env.EXCEL_PATH) return path.resolve(process.env.EXCEL_PATH);

  const candidates = [
    path.join(__dirname, "videofeed.csv"),
    path.join(__dirname, "videofeed.xlsx"),
    path.join(__dirname, "videofeed.xls"),
    path.join(__dirname, "videos.csv"),
    path.join(__dirname, "videos.xlsx"),
  ];

  const found = candidates.find((p) => fs.existsSync(p));
  return found || path.join(__dirname, "videofeed.csv");
})();

const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const parsedLimit = limitArg ? parseInt(limitArg.split("=")[1], 10) : (process.env.LIMIT_ROWS ? parseInt(process.env.LIMIT_ROWS, 10) : null);

const skipArg = process.argv.find((arg) => arg.startsWith("--skip=") || arg.startsWith("--start="));
const parsedSkip = skipArg
  ? (skipArg.startsWith("--start=") ? parseInt(skipArg.split("=")[1], 10) - 1 : parseInt(skipArg.split("=")[1], 10))
  : (process.env.SKIP_ROWS ? parseInt(process.env.SKIP_ROWS, 10) : 0);

const batchArg = process.argv.find((arg) => arg.startsWith("--batch="));
const parsedBatch = batchArg
  ? parseInt(batchArg.split("=")[1], 10)
  : (process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE, 10) : 4);

const SKIP_ROWS = Math.max(0, parsedSkip || 0);
const BATCH_SIZE = Math.max(1, parsedBatch || 4);

const IS_TEST_MODE =
  process.argv.includes("--test") ||
  process.argv.includes("-t") ||
  process.env.TEST_MODE === "true";

const ROW_LIMIT = parsedLimit || (IS_TEST_MODE ? 1 : null);




// ==========================================

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
  },
});

const TEMP_DIR = path.join(__dirname, "temp");

if (!fs.existsSync(TEMP_DIR)) {
  fs.mkdirSync(TEMP_DIR, { recursive: true });
}

// Mongo Schema for VehicleCounterStationAfter
const vehicleCounterStationAfterSchema = new mongoose.Schema(
  {
    stationId: { type: String, index: true },
    cam_id: { type: String },
    date: { type: Date, index: true },
    projectId: { type: String, index: true },
    dayVideoUrl: { type: String, default: "" },
    nightVideoUrl: { type: String, default: "" },
  },
  { strict: false, timestamps: false }
);

const VehicleCounterStationAfter =
  mongoose.models.vehicleCounterStationAfter ||
  mongoose.model(
    "vehicleCounterStationAfter",
    vehicleCounterStationAfterSchema,
    "vehiclecounterstationafters"
  );

/**
 * Clean S3 object key (strips S3 bucket base URLs if full URL is passed)
 */
function cleanS3Key(keyOrUrl) {
  if (!keyOrUrl || typeof keyOrUrl !== "string") return "";
  let cleanKey = keyOrUrl.trim();
  if (cleanKey.startsWith("http://") || cleanKey.startsWith("https://")) {
    try {
      const parsed = new URL(cleanKey);
      cleanKey = parsed.pathname.replace(/^\//, "");
    } catch (e) {
      // ignore url parse error
    }
  }
  return cleanKey.replace(/^\//, "");
}

/**
 * Generate converted object key ending with _h264
 * e.g. path/to/f000067.mp4 -> path/to/f000067_h264.mp4
 */
function getH264Key(originalKey) {
  const clean = cleanS3Key(originalKey);
  if (!clean) return "";
  const ext = path.extname(clean);
  if (ext) {
    const baseWithoutExt = clean.slice(0, -ext.length);
    return `${baseWithoutExt}_h264${ext}`;
  }
  return `${clean}_h264.mp4`;
}

/**
 * Safely delete local file
 */
function cleanupFile(filePath) {
  if (filePath && fs.existsSync(filePath)) {
    try {
      fs.unlinkSync(filePath);
      console.log(`Deleted local temp file: ${filePath}`);
    } catch (err) {
      console.warn(`Failed to delete local temp file ${filePath}:`, err.message);
    }
  }
}

async function downloadVideo(key, output) {
  console.log("\n========================================");
  console.log("Downloading from S3...");
  console.log("Key:", key);

  const response = await s3.send(
    new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    })
  );

  await pipelineAsync(response.Body, fs.createWriteStream(output));

  console.log("Downloaded:", output);
  console.log(
    "Original Size:",
    (fs.statSync(output).size / 1024 / 1024).toFixed(2),
    "MB"
  );
}

function convertVideo(input, output) {
  return new Promise((resolve, reject) => {
    console.log("\n========================================");
    console.log("Starting FFmpeg...");

    const args = [
      "-y",
      "-f",
      "mpegts",
      "-probesize",
      "100M",
      "-analyzeduration",
      "100M",
      "-i",
      input,
      "-map",
      "0:v:0",
      "-map",
      "0:a:0",
      "-c:v",
      "libx264",
      "-threads",
      "4",
      "-preset",
      "veryfast",
      "-crf",
      "28",
      "-pix_fmt",
      "yuv420p",
      "-movflags",
      "+faststart",
      "-c:a",
      "aac",
      "-b:a",
      "64k",
      output,
    ];

    console.log(`FFmpeg Command:\n${FFMPEG} ${args.join(" ")}\n`);

    const ffmpeg = spawn(FFMPEG, args);

    ffmpeg.stdout.on("data", (data) => {
      process.stdout.write(data.toString());
    });

    ffmpeg.stderr.on("data", (data) => {
      process.stdout.write(data.toString());
    });

    ffmpeg.on("error", reject);

    ffmpeg.on("close", (code) => {
      if (code === 0) {
        console.log("\n========================================");
        console.log("Conversion Complete");
        console.log(
          "Converted Size:",
          (fs.statSync(output).size / 1024 / 1024).toFixed(2),
          "MB"
        );
        resolve();
      } else {
        reject(new Error(`FFmpeg exited with code ${code}`));
      }
    });
  });
}

async function uploadVideo(key, filePath) {
  console.log("\n========================================");
  console.log("Uploading to S3...");
  console.log("Destination Key:", key);

  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: fs.createReadStream(filePath),
      ContentType: "video/mp4",
    })
  );

  console.log("Upload Complete:", key);
}

/**
 * Download, convert to H264, upload back to S3, and clean local temp files
 */
async function processSingleVideo(originalKey) {
  const cleanKey = cleanS3Key(originalKey);
  if (!cleanKey) return null;

  const uploadKey = getH264Key(cleanKey);
  const inputName = path.basename(cleanKey);
  const outputName = path.basename(uploadKey);

  const uniqueId = `${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
  const localInput = path.join(TEMP_DIR, `${uniqueId}_${inputName}`);
  const localOutput = path.join(TEMP_DIR, `${uniqueId}_${outputName}`);

  try {
    // Step 1: Download from S3
    await downloadVideo(cleanKey, localInput);

    // Step 2: Convert via FFmpeg
    await convertVideo(localInput, localOutput);

    // Step 3: Upload converted video to S3
    await uploadVideo(uploadKey, localOutput);

    console.log(`Successfully processed S3 video: ${uploadKey}`);
    return uploadKey;
  } finally {
    // Step 4: Delete local downloaded and converted videos
    cleanupFile(localInput);
    cleanupFile(localOutput);
  }
}

/**
 * Generate variations of cam_id (e.g. "30" -> ["30", "030", "0030"])
 */
function getCamIdVariations(camId) {
  if (camId === null || camId === undefined) return [];
  const str = String(camId).trim();
  if (!str) return [];
  const stripped = str.replace(/^0+/, "") || "0";
  const variations = new Set([
    str,
    stripped,
    str.padStart(2, "0"),
    str.padStart(3, "0"),
    str.padStart(4, "0"),
  ]);
  return Array.from(variations);
}

/**
 * Find MongoDB document by date, cam_id, and original video keys
 */
async function findMongoDocument(camId, rawDate, dayKey, nightKey, row = {}) {
  const camVariations = getCamIdVariations(camId);
  const cleanDay = cleanS3Key(dayKey);
  const cleanNight = cleanS3Key(nightKey);

  console.log(`Searching MongoDB with cam_id variations:`, camVariations);

  let doc = null;

  // Option 1: Direct ID or Station ID or Project ID match if in row
  const rowId = row._id || row.id;
  const stationId = row.stationId || row.station_id;

  if (rowId && mongoose.Types.ObjectId.isValid(rowId)) {
    doc = await VehicleCounterStationAfter.findById(rowId);
    if (doc) {
      console.log(`Found MongoDB document by _id: ${rowId}`);
      return doc;
    }
  }

  // Option 2: Match by cam_id and date range
  let dateObj = null;
  if (rawDate) {
    if (rawDate instanceof Date) {
      dateObj = rawDate;
    } else if (typeof rawDate === "string" || typeof rawDate === "number") {
      dateObj = new Date(rawDate);
    }
  }

  if (camVariations.length > 0 && dateObj && !isNaN(dateObj.getTime())) {
    // Start/End of day range covering UTC and Local timezones
    const year = dateObj.getUTCFullYear();
    const month = dateObj.getUTCMonth();
    const day = dateObj.getUTCDate();

    const startOfDayUTC = new Date(Date.UTC(year, month, day, 0, 0, 0, 0));
    const endOfDayUTC = new Date(Date.UTC(year, month, day, 23, 59, 59, 999));

    // Also wider +/- 1 day buffer to handle timezone shifts
    const startBuffer = new Date(startOfDayUTC.getTime() - 24 * 60 * 60 * 1000);
    const endBuffer = new Date(endOfDayUTC.getTime() + 24 * 60 * 60 * 1000);

    const query = {
      cam_id: { $in: camVariations },
      date: { $gte: startBuffer, $lte: endBuffer },
    };
    if (stationId) query.stationId = stationId;

    const candidates = await VehicleCounterStationAfter.find(query);
    console.log(`Query ${JSON.stringify(query)} found ${candidates.length} candidates.`);

    if (candidates.length === 1) {
      doc = candidates[0];
    } else if (candidates.length > 1) {
      // Find candidate closest in date to target YYYY-MM-DD
      doc = candidates.find((c) => {
        if (!c.date) return false;
        const cDate = new Date(c.date);
        return (
          cDate.getUTCFullYear() === year &&
          cDate.getUTCMonth() === month &&
          cDate.getUTCDate() === day
        );
      }) || candidates[0];
    }
  }

  // Option 3: Match by cam_id alone
  if (!doc && camVariations.length > 0) {
    const query = { cam_id: { $in: camVariations } };
    if (stationId) query.stationId = stationId;
    doc = await VehicleCounterStationAfter.findOne(query);
    if (doc) {
      console.log(`Found MongoDB document by cam_id match:`, camVariations);
    }
  }

  // Option 4: Fallback match by original day/night video URL
  if (!doc && (cleanDay || cleanNight)) {
    const orConditions = [
      ...(cleanDay ? [{ dayVideoUrl: cleanDay }] : []),
      ...(cleanNight ? [{ nightVideoUrl: cleanNight }] : []),
    ];
    doc = await VehicleCounterStationAfter.findOne({ $or: orConditions });
    if (doc) {
      console.log(`Found MongoDB document by matching original video URL keys.`);
    }
  }

  return doc;
}

/**
 * Process a single Excel row
 */
async function processRow(row, rowIndex) {
  console.log(`\n--------------------------------------------------`);
  console.log(`Processing Row #${rowIndex + 1}:`, row);

  const camId =
    row.cam_id ||
    row.camId ||
    row["cam_id"] ||
    row["Cam ID"] ||
    row["Camera ID"] ||
    "";

  const rawDate = row.date || row.Date || row.DATE || null;

  const dayObjectKey = cleanS3Key(
    row.dayObjectKey ||
      row.day_object_key ||
      row.dayKey ||
      row.dayVideoUrl ||
      row["Day Object Key"] ||
      row["day_object_key"] ||
      ""
  );

  const nightObjectKey = cleanS3Key(
    row.nightObjectKey ||
      row.night_object_key ||
      row.nightKey ||
      row.nightVideoUrl ||
      row["Night Object Key"] ||
      row["night_object_key"] ||
      ""
  );

  if (!dayObjectKey && !nightObjectKey) {
    console.warn(`Row #${rowIndex + 1} has no dayObjectKey or nightObjectKey. Skipping.`);
    return;
  }

  console.log(`Row details -> cam_id: "${camId}", date: "${rawDate}"`);
  console.log(`dayObjectKey: "${dayObjectKey}"`);
  console.log(`nightObjectKey: "${nightObjectKey}"`);

  // Step A: Convert day video if key exists
  let newDayKey = null;
  if (dayObjectKey) {
    console.log(`\n--- Processing Day Video ---`);
    newDayKey = await processSingleVideo(dayObjectKey);
  }

  // Step B: Convert night video if key exists
  let newNightKey = null;
  if (nightObjectKey) {
    console.log(`\n--- Processing Night Video ---`);
    newNightKey = await processSingleVideo(nightObjectKey);
  }

  // Step C: Find and update document in MongoDB
  if (MONGO_URI) {
    console.log(`\nUpdating MongoDB for cam_id: "${camId}", date: "${rawDate}"...`);
    const doc = await findMongoDocument(camId, rawDate, dayObjectKey, nightObjectKey, row);

    if (doc) {
      if (newDayKey) {
        doc.dayVideoUrl = newDayKey;
      }
      if (newNightKey) {
        doc.nightVideoUrl = newNightKey;
      }
      await doc.save();
      console.log(`Successfully updated MongoDB Document (ID: ${doc._id})`);
      console.log(`  dayVideoUrl: "${doc.dayVideoUrl}"`);
      console.log(`  nightVideoUrl: "${doc.nightVideoUrl}"`);
    } else {
      console.error(
        `MongoDB Document NOT FOUND for cam_id: "${camId}", date: "${rawDate}". Could not update video URLs.`
      );
    }
  } else {
    console.warn("MONGO_URI not provided. Skipping MongoDB update.");
  }
}

async function main() {
  console.log("===========================================");
  console.log("H265 -> H264 Migration Script");
  console.log("Bucket    :", BUCKET);
  console.log("Region    :", REGION);
  console.log("Excel File:", EXCEL_PATH);
  console.log("===========================================");

  if (!fs.existsSync(EXCEL_PATH)) {
    console.error(`\nExcel file not found at: ${EXCEL_PATH}`);
    console.error("Please provide valid excel file path via CLI argument or EXCEL_PATH env var.");
    console.error("Usage: node src/script/convertVideosToH264.js /path/to/file.xlsx");
    process.exit(1);
  }

  if (MONGO_URI) {
    console.log("\nConnecting to MongoDB...");
    await mongoose.connect(MONGO_URI);
    console.log("Connected to MongoDB.");
  } else {
    console.warn("\nWarning: MONGO_URI environment variable is missing.");
  }

  const workbook = xlsx.readFile(EXCEL_PATH, { cellDates: true });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  let rows = xlsx.utils.sheet_to_json(sheet);

  console.log(`Found ${rows.length} rows in Excel sheet "${sheetName}".`);

  const totalRows = rows.length;

  if (SKIP_ROWS > 0) {
    console.log(`\n[SKIP ENABLED] Skipping first ${SKIP_ROWS} row(s). Starting from row #${SKIP_ROWS + 1}.`);
    rows = rows.slice(SKIP_ROWS);
  }

  if (ROW_LIMIT && ROW_LIMIT > 0) {
    console.log(`\n[LIMIT ENABLED] Limiting execution to ${ROW_LIMIT} entry/entries.`);
    rows = rows.slice(0, ROW_LIMIT);
  }

  console.log(`\n[BATCHING ENABLED] Processing ${BATCH_SIZE} row(s) at a time.`);

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    console.log(
      `\n========================================` +
      `\nProcessing Batch (Rows ${SKIP_ROWS + i + 1} to ${SKIP_ROWS + i + batch.length} of total sheet)` +
      `\n========================================`
    );

    await Promise.all(
      batch.map((row, batchIdx) => {
        const originalRowIndex = SKIP_ROWS + i + batchIdx;
        return processRow(row, originalRowIndex).catch((err) => {
          console.error(`Failed processing row #${originalRowIndex + 1}:`, err);
        });
      })
    );
  }

  if (MONGO_URI) {
    await mongoose.disconnect();
    console.log("\nDisconnected from MongoDB.");
  }

  console.log("\n===========================================");
  console.log("All Processing Finished");
  console.log("===========================================");
}

main().catch((err) => {
  console.error("Fatal Error:", err);
  process.exit(1);
});