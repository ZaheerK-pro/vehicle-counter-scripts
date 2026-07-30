import dotenv from "dotenv";
import path from "path";
import fs from "fs";
import { spawn, execFile } from "child_process";
import { pipeline } from "stream";
import { promisify } from "util";
import mongoose from "mongoose";
import * as XLSX from "xlsx";
import {
  S3Client,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import VehicleCounterStationResultModel from "../src/models/VehicleCounterStationResult";

dotenv.config({ path: path.join(__dirname, "../.env") });

const pipelineAsync = promisify(pipeline);

// 

// ================= CONFIG & CREDENTIALS =================
const MONGO_URI =
  process.env.MONGO_URI ||
  "mongodb+srv://rasta_server:Ey7evteADrhYUFzs@rastaclustor.k92c0.mongodb.net/construction-360?retryWrites=true&w=majority";

const AWS_REGION = (process.env.AWS_REGION || "us-east-1").trim();
const AWS_BUCKET_NAME = (
  process.env.AWS_BUCKET_NAME || "rasta-vehicle-counter"
).trim();
const AWS_ACCESS_KEY_ID = (process.env.AWS_ACCESS_KEY_ID || "").trim();
const AWS_SECRET_ACCESS_KEY = (
  process.env.AWS_SECRET_ACCESS_KEY || ""
).trim();

const TEMP_DIR = path.join(__dirname, "temp");
if (!fs.existsSync(TEMP_DIR)) {
  fs.mkdirSync(TEMP_DIR, { recursive: true });
}

// Set TEST_SINGLE_STATION to false to process all stations
const TEST_SINGLE_STATION = false;

// Optional filter for a specific station (e.g. "vehicle-counter-unprocessed-video/27 june/059"), or null for ALL stations
const TARGET_STATION_FILTER: string | null = null;

// Filter and process ONLY stations that appear 2+ times in the Excel sheet (set to false to process all stations)
const ONLY_DUPLICATES_FILTER: boolean = process.env.ONLY_DUPLICATES !== "false";

// Optional: Resume from a specific station ID (e.g. "vehicle-counter-unprocessed-video/3 july/029")
const START_STATION: string | null = process.env.START_STATION || null;

// Optional: Resume from 0-based index
const START_INDEX: number = process.env.START_INDEX
  ? parseInt(process.env.START_INDEX, 10)
  : 0;

// ================= S3 CLIENT =================
const s3Client = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
});

// ================= FFPROBE HELPER =================
function getFFprobePath(): string {
  const customPath =
    "C:\\ffmpeg\\ffmpeg-8.1.2-essentials_build\\ffmpeg-8.1.2-essentials_build\\bin\\ffprobe.exe";
  if (fs.existsSync(customPath)) {
    return customPath;
  }
  return "ffprobe";
}

/**
 * Get video duration in seconds using ffprobe.
 */
function probeDuration(inputUrlOrFile: string): Promise<number> {
  const ffprobeCmd = getFFprobePath();
  return new Promise((resolve, reject) => {
    const args = [
      "-v",
      "error",
      "-rw_timeout",
      "5000000", // 5s network timeout in microseconds
      "-probesize",
      "32768", // Read max 32KB for header duration to avoid deep stream parsing
      "-analyzeduration",
      "0", // Stop analyzing stream data immediately after reading container header
      "-show_entries",
      "format=duration",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      inputUrlOrFile,
    ];

    execFile(ffprobeCmd, args, { timeout: 60000 }, (error, stdout, stderr) => {
      if (error) {
        return reject(
          new Error(`ffprobe failed: ${error.message} | ${stderr}`)
        );
      }
      const duration = parseFloat(stdout.trim());
      if (isNaN(duration)) {
        return reject(new Error(`Invalid duration output from ffprobe: '${stdout}'`));
      }
      resolve(duration);
    });
  });
}

/**
 * Helper function to run async tasks with a limit on concurrency.
 */
async function asyncPool<T, R>(
  concurrency: number,
  items: T[],
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  if (items.length === 0) return [];
  const results: R[] = new Array(items.length);
  let index = 0;

  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => {
      while (index < items.length) {
        const i = index++;
        results[i] = await fn(items[i], i);
      }
    }
  );

  await Promise.all(workers);
  return results;
}

/**
 * Downloads an S3 object to local temp and probes its duration, then cleans up.
 */
async function probeS3ObjectViaDownload(s3Key: string): Promise<number> {
  const filename = `temp_${Date.now()}_${path.basename(s3Key)}`;
  const localFilePath = path.join(TEMP_DIR, filename);

  try {
    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: AWS_BUCKET_NAME,
        Key: s3Key,
      })
    );
    await pipelineAsync(
      response.Body as any,
      fs.createWriteStream(localFilePath)
    );
    const duration = await probeDuration(localFilePath);
    return duration;
  } finally {
    if (fs.existsSync(localFilePath)) {
      try {
        fs.unlinkSync(localFilePath);
      } catch (_) {}
    }
  }
}

/**
 * Probes video duration first via S3 Presigned URL, falling back to temp file download if needed.
 */
async function getVideoDurationSeconds(
  s3Key: string,
  verbose: boolean = false
): Promise<number> {
  const startTime = Date.now();
  try {
    if (verbose) {
      console.log(`\n   [LOG] Generating S3 presigned URL for: ${s3Key}`);
    }
    const command = new GetObjectCommand({
      Bucket: AWS_BUCKET_NAME,
      Key: s3Key,
    });
    const presignedUrl = await getSignedUrl(s3Client, command, {
      expiresIn: 3600,
    });

    if (verbose) {
      console.log(`   [LOG] Executing ffprobe duration check via presigned URL...`);
    }
    const duration = await probeDuration(presignedUrl);
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(2);

    if (verbose) {
      console.log(
        `   [LOG] SUCCESS! Duration: ${duration.toFixed(2)} seconds (${(duration / 60).toFixed(2)} mins) | Time taken: ${elapsedSec}s`
      );
    }
    return duration;
  } catch (err: any) {
    console.warn(
      `   [Warning] Direct presigned URL probe failed for ${s3Key} (${err.message}). Falling back to temp download...`
    );
    return await probeS3ObjectViaDownload(s3Key);
  }
}

// ================= NORMALIZATION HELPERS =================

/**
 * Normalizes a station string for canonical matching:
 * - Trims whitespace
 * - Strips leading "cam-" or "cam_" (case-insensitive)
 * - Converts slashes '/' to underscores '_'
 * - Normalizes multiple spaces/underscores
 *
 * Example:
 *  "cam-vehicle-counter-unprocessed-video_27 june 085" -> "vehicle-counter-unprocessed-video_27 june 085"
 *  "vehicle-counter-unprocessed-video/27 june/085"     -> "vehicle-counter-unprocessed-video_27 june 085"
 */
export function normalizeStationId(id: string): string {
  if (!id) return "";
  return id
    .trim()
    .replace(/^cam[-_]/i, "")
    .replace(/[\/\s]+/g, "_")
    .toLowerCase();
}

/**
 * Converts a station ID string into an S3 prefix format.
 * E.g. "vehicle-counter-unprocessed-video/27 june/085" -> "vehicle-counter-unprocessed-video/27 june/085/"
 * E.g. "cam-vehicle-counter-unprocessed-video_27 june 085" -> "vehicle-counter-unprocessed-video/27 june/085/"
 */
function convertToS3Prefix(stationIdStr: string): string {
  let cleaned = stationIdStr.trim().replace(/^cam[-_]/i, "");

  // If already contains slashes, use slashes
  if (cleaned.includes("/")) {
    return cleaned.endsWith("/") ? cleaned : `${cleaned}/`;
  }

  // Handle format like: vehicle-counter-unprocessed-video_27 june 085 or vehicle-counter-unprocessed-video_27 june_085
  const match = cleaned.match(
    /^(vehicle-counter-unprocessed-video)[_\/](.+?)[_\/\s]+([a-zA-Z0-9]+)$/
  );
  if (match) {
    const [, folder, datePart, stationPart] = match;
    return `${folder}/${datePart.trim()}/${stationPart.trim()}/`;
  }

  // Fallback: replace underscores with slashes if parts exist
  const prefixed = cleaned.replace(/_/g, "/");
  return prefixed.endsWith("/") ? prefixed : `${prefixed}/`;
}

// ================= EXCEL PARSER =================
interface ExcelStationRow {
  camId: string;
  processedCount: number | null;
  rawSummary?: string;
}

function parseProcessedCount(summaryVal: any): number | null {
  if (summaryVal == null) return null;
  if (typeof summaryVal === "number") return summaryVal;
  const str = String(summaryVal).trim();
  // Match patterns like "39/186 videos", "35/57", "39 / 186"
  const match = str.match(/(\d+)\s*\/\s*\d+/);
  if (match) {
    return parseInt(match[1], 10);
  }
  const num = parseInt(str, 10);
  return isNaN(num) ? null : num;
}

function readExcelFile(filePath: string): ExcelStationRow[] {
  console.log(`\n Reading Excel file: ${filePath}`);
  const workbook = XLSX.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  const rows: any[] = XLSX.utils.sheet_to_json(worksheet, { defval: "" });

  const results: ExcelStationRow[] = [];

  for (const row of rows) {
    // Find key for cam ID / station ID
    let camId = "";
    let summaryVal: any = null;

    for (const key of Object.keys(row)) {
      const lowerKey = key.toLowerCase().trim();
      if (
        lowerKey.includes("cam") ||
        lowerKey.includes("station") ||
        lowerKey.includes("location")
      ) {
        camId = String(row[key]).trim();
      }
      if (
        lowerKey.includes("summary") ||
        lowerKey.includes("processed") ||
        lowerKey.includes("count")
      ) {
        summaryVal = row[key];
      }
    }

    // Fallback: check first column if camId not found by name
    if (!camId && Object.keys(row).length > 0) {
      camId = String(row[Object.keys(row)[0]]).trim();
    }
    // Fallback: check second column for summary if not found
    if (summaryVal === null && Object.keys(row).length > 1) {
      summaryVal = row[Object.keys(row)[1]];
    }

    if (camId) {
      const processedCount = parseProcessedCount(summaryVal);
      results.push({
        camId,
        processedCount,
        rawSummary: String(summaryVal || ""),
      });
    }
  }

  console.log(` Found ${results.length} station entries in Excel sheet.`);
  return results;
}

async function fetchS3ObjectsForPrefix(prefix: string): Promise<string[]> {
  const videoKeys: string[] = [];
  let continuationToken: string | undefined = undefined;

  const validExtensions = [
    ".mp4",
    ".mkv",
    ".mov",
    ".avi",
    ".ts",
    ".webm",
    ".flv",
    ".m4v",
  ];

  do {
    const response: ListObjectsV2CommandOutput = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: AWS_BUCKET_NAME,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
    );

    if (response.Contents) {
      for (const obj of response.Contents) {
        if (!obj.Key) continue;
        const ext = path.extname(obj.Key).toLowerCase();
        if (validExtensions.includes(ext)) {
          videoKeys.push(obj.Key);
        }
      }
    }
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return videoKeys;
}

// ================= S3 LISTING =================
async function listVideosInS3(s3Prefix: string): Promise<string[]> {
  let videoKeys = await fetchS3ObjectsForPrefix(s3Prefix);

  // If no videos found and prefix ends with '/', try without trailing '/'
  if (videoKeys.length === 0 && s3Prefix.endsWith("/")) {
    videoKeys = await fetchS3ObjectsForPrefix(s3Prefix.slice(0, -1));
  }

  // If still empty, search in root/parent folder (e.g., "vehicle-counter-unprocessed-video/")
  if (videoKeys.length === 0) {
    const rootFolder = s3Prefix.split("/")[0];
    if (rootFolder) {
      const allRootObjs = await fetchS3ObjectsForPrefix(`${rootFolder}/`);
      const normTarget = normalizeStationId(s3Prefix);
      videoKeys = allRootObjs.filter((key) => {
        const normKey = normalizeStationId(key);
        return normKey.includes(normTarget);
      });
    }
  }

  // Natural sort video keys (e.g. f00201.mp4, f00202.mp4...)
  videoKeys.sort((a, b) =>
    a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" })
  );

  return videoKeys;
}

// ================= MAIN SCRIPT =================
async function main() {
  console.log("=================================================");
  console.log(" Vehicle Counter Station Total Hours Calculator");
  console.log("=================================================");

  // 1. Connect to MongoDB
  console.log("\nConnecting to MongoDB...");
  await mongoose.connect(MONGO_URI);
  console.log(" Connected to MongoDB successfully.");

  // 2. Fetch all MongoDB station result documents
  const mongoDocs = await VehicleCounterStationResultModel.find({});
  console.log(` Found ${mongoDocs.length} documents in collection 'vehiclecounterstationresults'.`);

  // Build a lookup map of normalized mongo station ID -> mongo document
  const mongoDocMap = new Map<string, any>();
  for (const doc of mongoDocs) {
    if (doc.stationId) {
      const norm = normalizeStationId(doc.stationId);
      mongoDocMap.set(norm, doc);
    }
  }

  // 3. Locate Excel sheet if provided or present
  const cliArgs = process.argv.slice(2);
  let excelFilePath: string | null = null;

  if (cliArgs.length > 0 && fs.existsSync(cliArgs[0])) {
    excelFilePath = cliArgs[0];
  } else {
    // Check if any excel file (station.xlsx, station.csv, stations.xlsx, stations.csv) is present
    const candidates = [
      path.join(process.cwd(), "station.xlsx"),
      path.join(process.cwd(), "station.csv"),
      path.join(process.cwd(), "stations.xlsx"),
      path.join(process.cwd(), "stations.csv"),
      path.join(__dirname, "station.xlsx"),
      path.join(__dirname, "station.csv"),
      path.join(__dirname, "stations.xlsx"),
      path.join(__dirname, "stations.csv"),
      path.join(__dirname, "../station.xlsx"),
      path.join(__dirname, "../stations.xlsx"),
    ];
    for (const c of candidates) {
      if (fs.existsSync(c)) {
        excelFilePath = c;
        break;
      }
    }
  }

  let stationTasks: {
    stationIdMongo: string;
    camIdExcelOrS3: string;
    mongoDoc: any;
    processedCountReq: number | null;
    s3Prefix: string;
  }[] = [];

  if (excelFilePath) {
    const excelRows = readExcelFile(excelFilePath);

    for (const row of excelRows) {
      const normExcel = normalizeStationId(row.camId);
      const matchedMongoDoc = mongoDocMap.get(normExcel);

      const s3Prefix = convertToS3Prefix(row.camId);

      stationTasks.push({
        stationIdMongo: matchedMongoDoc ? matchedMongoDoc.stationId : `cam-${normExcel}`,
        camIdExcelOrS3: row.camId,
        mongoDoc: matchedMongoDoc || null,
        processedCountReq: row.processedCount,
        s3Prefix: s3Prefix,
      });
    }
  } else {
    console.log(
      "\n No Excel file provided via CLI args or default path. Processing stations directly from MongoDB documents..."
    );
    for (const doc of mongoDocs) {
      const s3Prefix = convertToS3Prefix(doc.stationId);
      stationTasks.push({
        stationIdMongo: doc.stationId,
        camIdExcelOrS3: s3Prefix.replace(/\/$/, ""),
        mongoDoc: doc,
        processedCountReq: null, // process all videos if count not specified in Excel
        s3Prefix: s3Prefix,
      });
    }
  }

  // Count total occurrences of each station in Excel sheet
  const stationTotalCountsInSheet = new Map<string, number>();
  for (const t of stationTasks) {
    const norm = normalizeStationId(t.stationIdMongo);
    stationTotalCountsInSheet.set(
      norm,
      (stationTotalCountsInSheet.get(norm) || 0) + 1
    );
  }

  // Filter to duplicate stations if ONLY_DUPLICATES_FILTER is true
  if (ONLY_DUPLICATES_FILTER && !TARGET_STATION_FILTER) {
    const duplicateTasks = stationTasks.filter((t) => {
      const norm = normalizeStationId(t.stationIdMongo);
      return (stationTotalCountsInSheet.get(norm) || 0) > 1;
    });

    if (duplicateTasks.length > 0) {
      console.log(
        `\n [DUPLICATE FILTER] Found ${duplicateTasks.length} row(s) for stations that appear multiple times in Excel sheet. Processing matched stations only...`
      );
      stationTasks = duplicateTasks;
    } else {
      console.log(
        `\n [DUPLICATE FILTER] No duplicate stations found in Excel sheet. Processing all stations...`
      );
    }
  }

  const stationVideoOffsetMap = new Map<string, number>();
  const stationOccurrenceTracker = new Map<string, number>();

  let resumeIndex = 0;
  if (TARGET_STATION_FILTER) {
    const normTarget = normalizeStationId(TARGET_STATION_FILTER);
    const filteredTasks = stationTasks.filter((t) => {
      return (
        normalizeStationId(t.stationIdMongo).includes(normTarget) ||
        normalizeStationId(t.camIdExcelOrS3).includes(normTarget) ||
        normalizeStationId(t.s3Prefix).includes(normTarget)
      );
    });
    if (filteredTasks.length > 0) {
      stationTasks = filteredTasks;
      console.log(
        `\n [FILTER APPLIED] Target station filter '${TARGET_STATION_FILTER}' matched ${stationTasks.length} task(s).`
      );
    } else {
      console.warn(
        `\n [WARNING] No tasks matched filter '${TARGET_STATION_FILTER}'. Searching S3 prefix directly...`
      );
      const customPrefix = convertToS3Prefix(TARGET_STATION_FILTER);
      stationTasks = [
        {
          stationIdMongo: `cam-${normTarget}`,
          camIdExcelOrS3: TARGET_STATION_FILTER,
          mongoDoc: mongoDocMap.get(normTarget) || null,
          processedCountReq: null,
          s3Prefix: customPrefix,
        },
      ];
    }
  } else if (START_STATION) {
    const normStart = normalizeStationId(START_STATION);
    const startIdx = stationTasks.findIndex((t) => {
      return (
        normalizeStationId(t.stationIdMongo).includes(normStart) ||
        normalizeStationId(t.camIdExcelOrS3).includes(normStart) ||
        normalizeStationId(t.s3Prefix).includes(normStart)
      );
    });

    if (startIdx !== -1) {
      resumeIndex = startIdx;
      console.log(
        `\n [RESUME] Found starting station '${START_STATION}' at position ${startIdx + 1}/${stationTasks.length}. Slicing remaining ${stationTasks.length - startIdx} task(s)...`
      );
    } else {
      console.warn(
        `\n [WARNING] Starting station '${START_STATION}' not found in task list.`
      );
      if (START_INDEX > 0 && START_INDEX < stationTasks.length) {
        resumeIndex = START_INDEX;
        console.log(
          ` [RESUME] Falling back to start from index ${START_INDEX}...`
        );
      }
    }
  } else if (START_INDEX > 0 && START_INDEX < stationTasks.length) {
    resumeIndex = START_INDEX;
    console.log(
      `\n [RESUME] Starting from index ${START_INDEX} (${stationTasks.length - START_INDEX} remaining task(s)).`
    );
  }

  // Pre-calculate video offsets for tasks before resumeIndex
  for (let k = 0; k < resumeIndex; k++) {
    const t = stationTasks[k];
    const normKey = normalizeStationId(t.stationIdMongo);
    const prev = stationVideoOffsetMap.get(normKey) || 0;
    const req = t.processedCountReq || 0;
    stationVideoOffsetMap.set(normKey, prev + req);
  }

  if (resumeIndex > 0) {
    stationTasks = stationTasks.slice(resumeIndex);
  }

  const isTestMode =
    TEST_SINGLE_STATION || process.argv.includes("--test-one");

  console.log(`\n=================================================`);
  if (isTestMode) {
    console.log(` [TEST MODE ENABLED] Processing ONLY 1 Station with Detailed Logs`);
  } else {
    console.log(` Starting Video Processing for ${stationTasks.length} Station(s)...`);
  }
  console.log(`=================================================`);

  const summaryReport: any[] = [];
  let processedStationCount = 0;

  for (let i = 0; i < stationTasks.length; i++) {
    const task = stationTasks[i];
    const normKey = normalizeStationId(task.stationIdMongo);
    const videoOffset = stationVideoOffsetMap.get(normKey) || 0;
    const occurrence = (stationOccurrenceTracker.get(normKey) || 0) + 1;
    stationOccurrenceTracker.set(normKey, occurrence);

    console.log(`\n-------------------------------------------------`);
    console.log(`[Station ${i + 1}/${stationTasks.length}] Task Details (Occurrence #${occurrence}):`);
    console.log(` - Excel/S3 Cam ID  : ${task.camIdExcelOrS3}`);
    console.log(` - MongoDB stationId : ${task.stationIdMongo}`);
    console.log(` - S3 Prefix        : ${task.s3Prefix}`);
    if (videoOffset > 0) {
      console.log(` - Video Start Offset: ${videoOffset} (from previous occurrences)`);
    }

    // Fetch videos from S3
    console.log(` [LOG] Querying S3 bucket '${AWS_BUCKET_NAME}' for prefix '${task.s3Prefix}'...`);
    const allVideos = await listVideosInS3(task.s3Prefix);
    console.log(` [LOG] Total matching video files found in S3: ${allVideos.length}`);

    if (allVideos.length === 0) {
      console.warn(` [Warning] No videos found under S3 prefix '${task.s3Prefix}'.`);
      if (isTestMode) {
        console.log(` [TEST MODE] Skipping empty prefix and checking next station...`);
        continue;
      }
      summaryReport.push({
        stationId: task.stationIdMongo,
        camId: task.camIdExcelOrS3,
        totalS3Videos: 0,
        processedVideosCount: 0,
        totalHours: 0,
        status: "No Videos Found",
      });
      continue;
    }

    if (videoOffset >= allVideos.length) {
      console.warn(
        ` [Warning] All ${allVideos.length} videos for station '${task.stationIdMongo}' were already processed in previous rows (Offset: ${videoOffset}). Skipping...`
      );
      summaryReport.push({
        stationId: task.stationIdMongo,
        camId: task.camIdExcelOrS3,
        totalS3Videos: allVideos.length,
        processedVideosCount: 0,
        totalHours: 0,
        status: "Already Processed in Previous Row",
      });
      continue;
    }

    // Determine how many videos to process starting from videoOffset
    const remainingVideosCount = allVideos.length - videoOffset;
    let countToProcess = remainingVideosCount;
    if (
      task.processedCountReq !== null &&
      task.processedCountReq > 0 &&
      task.processedCountReq <= remainingVideosCount
    ) {
      countToProcess = task.processedCountReq;
    }

    const videosToProcess = allVideos.slice(videoOffset, videoOffset + countToProcess);

    // Update offset map for future rows of the same station ID
    stationVideoOffsetMap.set(normKey, videoOffset + videosToProcess.length);

    console.log(
      ` [LOG] Preparing to process ${videosToProcess.length} video(s) (video index ${videoOffset + 1} to ${videoOffset + videosToProcess.length} out of ${allVideos.length} total S3 videos)...`
    );

    let totalDurationSeconds = 0;
    let successCount = 0;

    const CONCURRENCY = parseInt(process.env.CONCURRENCY || "30", 10);
    const poolSize = isTestMode ? 1 : CONCURRENCY;
    console.log(
      ` [LOG] Probing video durations with concurrency level = ${poolSize}...`
    );

    const probeResults = await asyncPool(
      poolSize,
      videosToProcess,
      async (vKey, vIdx) => {
        if (isTestMode) {
          console.log(`\n   -------------------------------------------------`);
          console.log(
            `   [LOG] Video [${vIdx + 1}/${countToProcess}] File: ${path.basename(
              vKey
            )}`
          );
          console.log(`   [LOG] Full S3 Key: ${vKey}`);
        }
        try {
          const durationSec = await getVideoDurationSeconds(vKey, isTestMode);
          if (!isTestMode) {
            console.log(
              `   Video [${vIdx + 1}/${countToProcess}] ${path.basename(
                vKey
              )} ... ${durationSec.toFixed(2)}s`
            );
          }
          return { durationSec, success: true };
        } catch (err: any) {
          if (!isTestMode) {
            console.log(
              `   Video [${vIdx + 1}/${countToProcess}] ${path.basename(
                vKey
              )} ... FAILED (${err.message})`
            );
          } else {
            console.error(`   [ERROR] Failed to probe video: ${err.message}`);
          }
          return { durationSec: 0, success: false };
        }
      }
    );

    for (const res of probeResults) {
      if (res && res.success) {
        totalDurationSeconds += res.durationSec;
        successCount++;
      }
    }

    const newlyCalculatedHours = parseFloat((totalDurationSeconds / 3600).toFixed(4));
    console.log(`\n=================================================`);
    console.log(` STATION SUMMARY RESULT FOR ${task.stationIdMongo}:`);
    console.log(` - Total Videos Probed Successfully : ${successCount}/${countToProcess}`);
    console.log(` - Total Duration in Seconds       : ${totalDurationSeconds.toFixed(2)} seconds`);
    console.log(` - Newly Calculated Hours           : ${newlyCalculatedHours} Hours`);
    console.log(`=================================================`);

    // Update MongoDB (Occurrence 1 sets initial totalHours; Occurrence 2+ accumulates)
    let matchedDoc = task.mongoDoc;
    if (matchedDoc && matchedDoc._id) {
      matchedDoc = await VehicleCounterStationResultModel.findById(matchedDoc._id);
    }
    if (!matchedDoc) {
      matchedDoc = await VehicleCounterStationResultModel.findOne({
        $or: [
          { stationId: task.stationIdMongo },
          { stationId: `cam-${task.camIdExcelOrS3.replace(/\//g, "_")}` },
        ],
      });
    }

    let finalTotalHours = newlyCalculatedHours;
    if (matchedDoc) {
      if (occurrence === 1) {
        finalTotalHours = newlyCalculatedHours;
        console.log(
          ` [LOG] [1st Occurrence of '${matchedDoc.stationId}'] Setting initial totalHours = ${finalTotalHours}h in MongoDB.`
        );
      } else {
        const existingHours =
          typeof matchedDoc.totalHours === "number" ? matchedDoc.totalHours : 0;
        finalTotalHours = parseFloat((existingHours + newlyCalculatedHours).toFixed(4));
        console.log(
          ` [LOG] [Occurrence #${occurrence} of '${matchedDoc.stationId}'] Accumulating MongoDB totalHours:`
        );
        console.log(
          `       Existing (${existingHours}h) + Newly Calculated (${newlyCalculatedHours}h) = Total Updated (${finalTotalHours}h)`
        );
      }

      const updateResult = await VehicleCounterStationResultModel.updateOne(
        { _id: matchedDoc._id },
        { $set: { totalHours: finalTotalHours } }
      );
      console.log(
        ` [LOG] MongoDB Update Result: matchedCount=${updateResult.matchedCount}, modifiedCount=${updateResult.modifiedCount}`
      );
    } else {
      console.warn(
        ` [Warning] No document found in MongoDB matching '${task.stationIdMongo}'.`
      );
    }

    summaryReport.push({
      stationId: task.stationIdMongo,
      camId: task.camIdExcelOrS3,
      totalS3Videos: allVideos.length,
      processedVideosCount: countToProcess,
      totalSeconds: parseFloat(totalDurationSeconds.toFixed(2)),
      newlyCalculatedHours: newlyCalculatedHours,
      totalHours: finalTotalHours,
      status: "Success",
    });

    processedStationCount++;

    if (isTestMode && processedStationCount >= 1) {
      console.log(
        `\n [TEST MODE COMPLETE] Tested single station '${task.stationIdMongo}' successfully.`
      );
      break;
    }
  }

  // Print final summary report
  console.log("\n=================================================");
  console.log(" SUMMARY EXECUTION REPORT");
  console.log("=================================================");
  console.table(summaryReport);

  await mongoose.disconnect();
  console.log("\n MongoDB disconnected. Done!");
}

if (require.main === module) {
  main().catch((err) => {
    console.error("FATAL ERROR in calculateTotalHours script:", err);
    process.exit(1);
  });
}
