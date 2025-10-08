// worker.js
import { Worker, Queue } from "bullmq";
import IORedis from "ioredis";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs-extra";
import tmp from "tmp-promise";
import { spawn } from "child_process";
import path from "path";
import pino from "pino";

const log = pino();
const QUEUE_NAME = process.env.QUEUE_NAME || "poppler";

// Ensure ioredis options required by BullMQ
const redis = new IORedis(process.env.REDIS_URL || "redis://redis:6379", {
  // required by BullMQ
  maxRetriesPerRequest: null,
  // optional sane defaults
  enableReadyCheck: true,
  connectTimeout: 10000,
});

const queue = new Queue(QUEUE_NAME, { connection: redis });

const s3 = new S3Client({
  endpoint: process.env.S3_ENDPOINT,
  region: process.env.S3_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_SECRET_KEY,
  },
  forcePathStyle: true,
});

const bucket = process.env.S3_BUCKET || "uploads";

/** Download from S3 to local path */
async function download(key, outPath) {
  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  await fs.ensureDir(path.dirname(outPath));
  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(outPath);
    res.Body.pipe(ws).on("finish", resolve).on("error", reject);
  });
}

/** Upload local file to S3 */
async function upload(localPath, key) {
  const body = fs.createReadStream(localPath);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
}

/** Run a command, capture stderr/stdout for improved errors */
function runCmd(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"], ...opts });
    let stderr = "";
    let stdout = "";
    child.stderr.on("data", (d) => (stderr += d.toString()));
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.on("close", (code) =>
      code === 0 ? resolve({ stdout, stderr }) : reject(new Error(`${cmd} exited ${code}: ${stderr || stdout}`))
    );
    child.on("error", (err) => reject(err));
  });
}

const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const { jobId, s3InputKey } = job.data;
    log.info({ jobId, s3InputKey }, "picked up job");

    const tmpdir = await tmp.dir({ unsafeCleanup: true, prefix: `poppler-${jobId}-` });

    try {
      const inputLocal = path.join(tmpdir.path, "input.pdf");
      await download(s3InputKey, inputLocal);

      // 1) render first page as png (thumbnail)
      const thumbPrefix = path.join(tmpdir.path, "thumb");
      // -singlefile + output prefix will produce thumb.png
      await runCmd("pdftoppm", ["-png", "-f", "1", "-singlefile", "-scale-to", "800", inputLocal, thumbPrefix]);

      const thumb = path.join(tmpdir.path, "thumb.png");
      if (await fs.pathExists(thumb)) {
        const thumbKey = `results/${jobId}/thumbnail.png`;
        await upload(thumb, thumbKey);
        log.info({ jobId, thumbKey }, "uploaded thumbnail");
      } else {
        log.warn({ jobId }, "thumbnail expected but not found");
      }

      // 2) extract plain text
      const txtOut = path.join(tmpdir.path, "out.txt");
      await runCmd("pdftotext", [inputLocal, txtOut]);

      if (await fs.pathExists(txtOut)) {
        const txtKey = `results/${jobId}/extracted.txt`;
        await upload(txtOut, txtKey);
        log.info({ jobId, txtKey }, "uploaded extracted text");
      } else {
        log.warn({ jobId }, "extracted text expected but not found");
      }

      log.info({ jobId }, "poppler work done");
      return { status: "ok" };
    } catch (err) {
      log.error({ err: String(err).slice(0, 2000), jobId }, "poppler job failed");
      // rethrow so BullMQ marks job as failed
      throw err;
    } finally {
      await tmpdir.cleanup().catch((e) => log.warn({ e }, "tmp cleanup failed"));
    }
  },
  {
    connection: redis,
    concurrency: Number(process.env.CONCURRENCY || 2),
  }
);

// Optional: create a Job Scheduler using Queue.upsertJobScheduler
// Enable by setting JOB_SCHEDULER_ID and either JOB_SCHEDULER_EVERY_MS or JOB_SCHEDULER_PATTERN env vars.
// Examples:
//   JOB_SCHEDULER_ID=thumb-every-5s JOB_SCHEDULER_EVERY_MS=5000 node worker.js
//   JOB_SCHEDULER_ID=daily JOB_SCHEDULER_PATTERN="RRULE:FREQ=DAILY;INTERVAL=1" node worker.js
async function maybeCreateJobScheduler() {
  const schedulerId = process.env.JOB_SCHEDULER_ID;
  if (!schedulerId) return;

  const everyMs = process.env.JOB_SCHEDULER_EVERY_MS ? Number(process.env.JOB_SCHEDULER_EVERY_MS) : undefined;
  const pattern = process.env.JOB_SCHEDULER_PATTERN || undefined;

  const repeatOpts = {};
  if (everyMs) repeatOpts.every = everyMs;
  if (pattern) repeatOpts.pattern = pattern;

  if (!repeatOpts.every && !repeatOpts.pattern) {
    log.warn({ schedulerId }, "JOB_SCHEDULER_ID set but no JOB_SCHEDULER_EVERY_MS or JOB_SCHEDULER_PATTERN provided");
    return;
  }

  // jobTemplate controls the job that will be created by the scheduler.
  // Keep it minimal — the worker expects job.data.s3InputKey and job.data.jobId when processing.
  const jobTemplate = {
    name: process.env.JOB_SCHEDULER_NAME || "scheduled-poppler",
    data: JSON.parse(process.env.JOB_SCHEDULER_DATA || "{}"),
    opts: {
      // you may set job-specific opts here (priority, attempts, backoff, etc.)
      removeOnComplete: { age: 3600 },
      removeOnFail: { age: 86400 },
    },
  };

  try {
    const created = await queue.upsertJobScheduler(schedulerId, repeatOpts, jobTemplate);
    log.info({ schedulerId, repeatOpts, created }, "upserted job scheduler");
  } catch (e) {
    log.error({ err: String(e).slice(0, 2000), schedulerId }, "failed creating job scheduler");
  }
}

// bootstrap optional scheduler
maybeCreateJobScheduler().catch((e) => log.error({ err: String(e) }, "scheduler bootstrap error"));

// Events
worker.on("completed", (job) => {
  log.info({ id: job.id }, "job completed");
});

worker.on("failed", (job, err) => {
  log.warn({ id: job?.id, err: err?.message }, "job failed");
});

// Graceful shutdown: close queue, worker, and quit redis
async function shutdown(signal) {
  try {
    log.info({ signal }, "shutdown initiated");
    await worker.close();
    try {
      // close the Queue instance (release resources used by queue internals)
      if (queue && typeof queue.close === "function") {
        await queue.close();
      }
    } catch (e) {
      log.warn({ e }, "error closing queue");
    }
    await redis.quit();
    log.info("shutdown complete");
    process.exit(0);
  } catch (e) {
    log.error({ e }, "shutdown error, forcing exit");
    process.exit(1);
  }
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// catch unhandled rejections to fail fast (you may prefer different behavior)
process.on("unhandledRejection", (reason) => {
  log.error({ reason }, "unhandledRejection — shutting down");
  shutdown("unhandledRejection");
});
